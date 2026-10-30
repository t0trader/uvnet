#include "uvnet/tcp.hpp"

#include <cerrno>
#include <cstring>
#include <deque>
#include <limits>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "detail/buffer_pool.hpp"
#include "detail/platform.hpp"
#include "detail/poller_linux.hpp"

namespace uvnet {
namespace {

constexpr std::size_t kReadBufferSize = 64 * 1024;

struct WriteItem {
  std::vector<std::byte> owned;
  const std::byte* data = nullptr;
  std::size_t size = 0;
  std::size_t offset = 0;
  WriteCallback callback;
  std::shared_ptr<const void> keepalive;
};

VoidResult apply_bind_options(int fd, Family family, const BindOptions& options) {
  const int one = 1;

  if (options.reuse_address &&
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }

  if (options.reuse_port &&
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }

  if (family == Family::IPv6) {
    const int v6only = options.ipv6_only ? 1 : 0;
    if (::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) != 0) {
      return std::unexpected(detail::from_socket_error(errno));
    }
  }

  return {};
}

VoidResult apply_keepalive(int fd, const KeepAliveOptions& options) {
  const int enable = options.enable ? 1 : 0;
  if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enable, sizeof(enable)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }
  if (!options.enable) {
    return {};
  }

  int idle = static_cast<int>(options.delay.count());
  if (idle <= 0) {
    idle = 1;
  }
  if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }
  if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &idle, sizeof(idle)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }
  return {};
}

int create_socket(Family family) {
  return ::socket(detail::AddressCodec::native_family(family),
                  SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                  IPPROTO_TCP);
}

Result<SocketAddress> socket_local_address(int fd) {
  sockaddr_storage storage{};
  socklen_t length = sizeof(storage);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }
  return detail::AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage),
                                           static_cast<int>(length));
}

Result<SocketAddress> socket_peer_address(int fd) {
  sockaddr_storage storage{};
  socklen_t length = sizeof(storage);
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }
  return detail::AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage),
                                           static_cast<int>(length));
}

} // namespace

namespace detail {

class TcpStreamImpl final : public IoWatcher,
                            public std::enable_shared_from_this<TcpStreamImpl> {
 public:
  explicit TcpStreamImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}

  ~TcpStreamImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local, const BindOptions& options) {
    if (fd_ >= 0 && (connecting_ || connected_)) {
      return std::unexpected(make_error(ErrorCode::AlreadyConnected));
    }
    if (!ensure_socket(local.family())) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    auto applied = apply_bind_options(fd_, local.family(), options);
    if (!applied) {
      return applied;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(local, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    bound_ = true;
    return {};
  }

  VoidResult connect(const SocketAddress& remote, ConnectCallback callback) {
    if (fd_ >= 0 && connecting_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (fd_ >= 0 && connected_) {
      return std::unexpected(make_error(ErrorCode::AlreadyConnected));
    }
    if (remote.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!ensure_socket(remote.family())) {
      return std::unexpected(from_socket_error(last_socket_error()));
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(remote, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    connect_callback_ = std::move(callback);
    connecting_ = true;
    const int result =
        ::connect(fd_, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length));

    if (result == 0) {
      connecting_ = false;
      connected_ = true;
      auto callback_copy = std::move(connect_callback_);
      connect_callback_ = nullptr;
      update_interest();
      if (callback_copy) {
        loop_->post([callback_copy] { callback_copy({}); });
      }
      return {};
    }

    if (errno == EINPROGRESS) {
      update_interest();
      return {};
    }

    const Error error = from_socket_error(errno);
    connecting_ = false;
    connect_callback_ = nullptr;
    close_socket(false, true);
    return std::unexpected(error);
  }

  VoidResult set_read_options(const ReadOptions& options) {
    if (read_started_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (options.slots == 0 || options.slots > 64 || options.chunk_size < 1024 ||
        options.chunk_size > 1024 * 1024) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    read_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult start_read(ReadCallback callback) {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!connected_ || fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (eof_) {
      return std::unexpected(make_error(ErrorCode::EndOfFile));
    }
    keepalive_reads_ = true;
    span_callback_ = nullptr;
    read_callback_ = std::move(callback);
    reading_ = true;
    read_started_ = true;
    update_interest();
    return {};
  }

  VoidResult start_read(SpanReadCallback callback) {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!connected_ || fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (eof_) {
      return std::unexpected(make_error(ErrorCode::EndOfFile));
    }
    keepalive_reads_ = false;
    read_callback_ = nullptr;
    span_callback_ = std::move(callback);
    reading_ = true;
    read_started_ = true;
    update_interest();
    return {};
  }

  void stop_read() {
    reading_ = false;
    read_callback_ = nullptr;
    span_callback_ = nullptr;
    update_interest();
  }

  VoidResult write(std::span<const std::byte> data, WriteCallback callback) {
    if (fd_ < 0 || !connected_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (data.empty()) {
      return {};
    }

    WriteItem item = acquire_write_item();
    item.owned.assign(data.begin(), data.end());
    item.data = item.owned.data();
    item.size = item.owned.size();
    item.callback = std::move(callback);
    writes_.push_back(std::move(item));

    flush_writes();
    return {};
  }

  VoidResult write(WriteBuffer buffer, WriteCallback callback) {
    if (fd_ < 0 || !connected_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (buffer.data.empty()) {
      return {};
    }

    WriteItem item = acquire_write_item();
    item.data = buffer.data.data();
    item.size = buffer.data.size();
    item.keepalive = std::move(buffer.keepalive);
    item.callback = std::move(callback);
    writes_.push_back(std::move(item));

    flush_writes();
    return {};
  }

  VoidResult shutdown(WriteCallback callback) {
    if (fd_ < 0 || !connected_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_callback_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }

    shutdown_callback_ = std::move(callback);
    shutdown_pending_ = true;
    if (writes_.empty()) {
      do_shutdown();
    } else {
      update_interest();
    }
    return {};
  }

  void close(bool reset) { close_socket(reset, false); }

  void close_reset() { close_socket(true, false); }

  [[nodiscard]] bool is_open() const noexcept { return fd_ >= 0; }
  [[nodiscard]] bool is_connected() const noexcept { return connected_; }

  Result<SocketAddress> local_address() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return socket_local_address(fd_);
  }

  Result<SocketAddress> peer_address() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return socket_peer_address(fd_);
  }

  VoidResult set_nodelay(bool enable) {
    nodelay_ = enable;
    if (fd_ < 0) {
      return {};
    }
    const int value = enable ? 1 : 0;
    if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult set_keepalive(const KeepAliveOptions& options) {
    keepalive_ = options;
    if (fd_ < 0) {
      return {};
    }
    return apply_keepalive(fd_, options);
  }

  Result<int> recv_buffer_size() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    socklen_t length = sizeof(value);
    if (::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return value;
  }

  VoidResult set_recv_buffer_size(int bytes) {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  Result<int> send_buffer_size() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    socklen_t length = sizeof(value);
    if (::getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return value;
  }

  VoidResult set_send_buffer_size(int bytes) {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  void adopt_fd(int fd, Family family) {
    fd_ = fd;
    family_ = family;
    connected_ = true;
    bound_ = true;
    loop_->begin_work();
    work_ref_ = true;
  }

  void on_io(std::uint32_t events) override {
    auto self = shared_from_this();

    if (connecting_) {
      connecting_ = false;
      int error = 0;
      socklen_t length = sizeof(error);
      const int result = ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &length);
      if (result != 0) {
        error = errno;
      }

      auto callback = std::move(connect_callback_);
      connect_callback_ = nullptr;
      if (error == 0) {
        connected_ = true;
        update_interest();
        if (callback) {
          callback({});
        }
      } else {
        update_interest();
        if (callback) {
          callback(std::unexpected(from_socket_error(error)));
        }
      }

      if (closing_ || fd_ < 0) {
        return;
      }
    }

    if ((events & (EPOLLIN | EPOLLHUP | EPOLLERR)) != 0) {
      do_read();
      if (fd_ < 0 || closing_) {
        return;
      }
    }

    if ((events & (EPOLLOUT | EPOLLERR)) != 0) {
      flush_writes();
    }
  }

 private:
  bool ensure_socket(Family family) {
    if (fd_ >= 0) {
      return family_ == family;
    }

    const int fd = create_socket(family);
    if (fd < 0) {
      return false;
    }

    fd_ = fd;
    family_ = family;
    loop_->begin_work();
    work_ref_ = true;

    if (nodelay_) {
      const int value = 1;
      (void)::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
    }
    if (keepalive_.enable) {
      (void)apply_keepalive(fd_, keepalive_);
    }
    return true;
  }

  void update_interest() {
    if (fd_ < 0) {
      return;
    }

    std::uint32_t wanted = 0;
    if (reading_ && !eof_) {
      wanted |= EPOLLIN;
    }
    if (connecting_ || !writes_.empty() || shutdown_pending_) {
      wanted |= EPOLLOUT;
    }

    EpollPoller& poller = linux_poller(*loop_);
    if (wanted == 0) {
      if (watching_) {
        poller.del(this);
        watching_ = false;
        interest_ = 0;
      }
      return;
    }

    if (!watching_) {
      watching_ = poller.add(this, fd_, wanted);
      interest_ = wanted;
      return;
    }

    if (wanted != interest_) {
      (void)poller.mod(this, wanted);
      interest_ = wanted;
    }
  }

  void deliver_read_error(Error error) {
    if (keepalive_reads_) {
      auto callback = std::move(read_callback_);
      read_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    } else {
      auto callback = std::move(span_callback_);
      span_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    }
  }

  void do_read() {
    if (!reading_ || eof_ || fd_ < 0) {
      return;
    }

    if (keepalive_reads_ && !read_pool_) {
      read_pool_ = BufferPool::create(read_chunk_size_, 32);
    }
    if (!keepalive_reads_ && span_read_buffer_.empty()) {
      span_read_buffer_.resize(read_chunk_size_);
    }

    auto self = shared_from_this();
    while (reading_ && !eof_ && fd_ >= 0) {
      BufferPool::Handle block;
      std::byte* data = nullptr;
      std::size_t capacity = 0;
      if (keepalive_reads_) {
        block = read_pool_->acquire();
        data = block.data.data();
        capacity = block.data.size();
      } else {
        data = span_read_buffer_.data();
        capacity = span_read_buffer_.size();
      }

      const ssize_t count = ::recv(fd_, data, capacity, 0);

      if (count > 0) {
        if (keepalive_reads_) {
          auto callback = read_callback_;
          if (callback) {
            callback(ReadChunk{
                std::span<const std::byte>(data, static_cast<std::size_t>(count)),
                block.keepalive});
          }
        } else {
          auto callback = span_callback_;
          if (callback) {
            callback(std::span<const std::byte>(data, static_cast<std::size_t>(count)));
          }
        }
        continue;
      }

      if (count == 0) {
        eof_ = true;
        reading_ = false;
        update_interest();
        deliver_read_error(make_error(ErrorCode::EndOfFile));
        return;
      }

      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      if (error == EAGAIN || error == EWOULDBLOCK) {
        return;
      }

      reading_ = false;
      update_interest();
      deliver_read_error(from_socket_error(error));
      return;
    }
  }

  WriteItem acquire_write_item() {
    if (!free_writes_.empty()) {
      WriteItem item = std::move(free_writes_.back());
      free_writes_.pop_back();
      return item;
    }
    return WriteItem{};
  }

  void recycle_write_item(WriteItem item) {
    item.data = nullptr;
    item.size = 0;
    item.offset = 0;
    item.callback = nullptr;
    item.keepalive.reset();
    if (free_writes_.size() >= 8) {
      return;
    }
    free_writes_.push_back(std::move(item));
  }

  void flush_writes() {
    auto self = shared_from_this();

    while (!writes_.empty() && fd_ >= 0) {
      WriteItem item = std::move(writes_.front());
      writes_.pop_front();

      bool requeue = false;
      while (item.offset < item.size && fd_ >= 0) {
        const ssize_t count = ::send(fd_, item.data + item.offset, item.size - item.offset, 0);

        if (count > 0) {
          item.offset += static_cast<std::size_t>(count);
          continue;
        }

        if (count == 0) {
          requeue = true;
          break;
        }

        const int error = errno;
        if (error == EINTR) {
          continue;
        }
        if (error == EAGAIN || error == EWOULDBLOCK) {
          requeue = true;
          break;
        }

        // 回调期间保持 buffer/keepalive 有效
        WriteCallback callback = std::move(item.callback);
        const Error mapped = from_socket_error(error);
        if (callback) {
          callback(std::unexpected(mapped));
        }
        fail_writes(mapped);
        recycle_write_item(std::move(item));
        return;
      }

      if (requeue) {
        writes_.push_front(std::move(item));
        break;
      }

      WriteCallback callback = std::move(item.callback);
      if (callback) {
        callback({});
      }
      recycle_write_item(std::move(item));
    }

    if (writes_.empty() && shutdown_pending_) {
      do_shutdown();
      return;
    }

    update_interest();
  }

  void do_shutdown() {
    shutdown_pending_ = false;
    if (fd_ >= 0) {
      (void)::shutdown(fd_, SHUT_WR);
    }
    auto callback = std::move(shutdown_callback_);
    shutdown_callback_ = nullptr;
    update_interest();
    if (callback) {
      callback({});
    }
  }

  void fail_writes(Error error) {
    std::deque<WriteItem> pending;
    pending.swap(writes_);
    for (auto& item : pending) {
      if (item.callback) {
        item.callback(std::unexpected(error));
      }
    }
    update_interest();
  }

  void close_socket(bool reset, bool silent) {
    if (fd_ < 0 && !work_ref_) {
      return;
    }

    auto self = shared_from_this();
    closing_ = true;

    if (fd_ >= 0) {
      if (watching_) {
        linux_poller(*loop_).del(this);
        watching_ = false;
        interest_ = 0;
      }
      if (reset) {
        linger option{};
        option.l_onoff = 1;
        option.l_linger = 0;
        (void)::setsockopt(fd_, SOL_SOCKET, SO_LINGER, &option, sizeof(option));
      }
      (void)::close(fd_);
      fd_ = -1;
    }

    reading_ = false;
    connecting_ = false;
    connected_ = false;
    bound_ = false;
    eof_ = false;
    shutdown_pending_ = false;

    auto connect_cb = std::move(connect_callback_);
    connect_callback_ = nullptr;
    auto shutdown_cb = std::move(shutdown_callback_);
    shutdown_callback_ = nullptr;
    auto read_cb = std::move(read_callback_);
    read_callback_ = nullptr;
    auto span_cb = std::move(span_callback_);
    span_callback_ = nullptr;
    std::deque<WriteItem> pending;
    pending.swap(writes_);

    release_ref();

    if (!silent) {
      if (connect_cb) {
        connect_cb(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      if (shutdown_cb) {
        shutdown_cb(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      (void)read_cb;
      (void)span_cb;
      for (auto& item : pending) {
        if (item.callback) {
          item.callback(std::unexpected(make_error(ErrorCode::Canceled)));
        }
      }
    }
  }

  void release_ref() {
    if (work_ref_) {
      work_ref_ = false;
      loop_->end_work();
    }
  }

  void destroy() noexcept {
    closing_ = true;
    if (fd_ >= 0 && watching_) {
      linux_poller(*loop_).del(this);
      watching_ = false;
    }
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
    read_callback_ = nullptr;
    span_callback_ = nullptr;
    connect_callback_ = nullptr;
    shutdown_callback_ = nullptr;
    writes_.clear();
    free_writes_.clear();
    release_ref();
  }

  std::shared_ptr<LoopImpl> loop_;
  int fd_ = -1;
  Family family_ = Family::Unspecified;
  bool bound_ = false;
  bool connecting_ = false;
  bool connected_ = false;
  bool reading_ = false;
  bool eof_ = false;
  bool closing_ = false;
  bool watching_ = false;
  bool work_ref_ = false;
  std::uint32_t interest_ = 0;

  bool nodelay_ = false;
  KeepAliveOptions keepalive_{false, std::chrono::seconds(60)};

  ReadCallback read_callback_;
  SpanReadCallback span_callback_;
  bool keepalive_reads_ = false;
  std::size_t read_chunk_size_ = kReadBufferSize;
  bool read_started_ = false;
  std::vector<std::byte> span_read_buffer_;
  ConnectCallback connect_callback_;
  WriteCallback shutdown_callback_;
  bool shutdown_pending_ = false;

  std::shared_ptr<BufferPool> read_pool_;
  std::deque<WriteItem> writes_;
  std::vector<WriteItem> free_writes_;
};

class TcpListenerImpl final : public IoWatcher,
                              public std::enable_shared_from_this<TcpListenerImpl> {
 public:
  explicit TcpListenerImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}

  ~TcpListenerImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local, const BindOptions& options) {
    if (fd_ >= 0) {
      return std::unexpected(make_error(ErrorCode::AlreadyExists));
    }
    if (local.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    const int fd = create_socket(local.family());
    if (fd < 0) {
      return std::unexpected(from_socket_error(errno));
    }

    auto applied = apply_bind_options(fd, local.family(), options);
    if (!applied) {
      (void)::close(fd);
      return applied;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(local, storage, length)) {
      (void)::close(fd);
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length)) != 0) {
      const Error error = from_socket_error(errno);
      (void)::close(fd);
      return std::unexpected(error);
    }

    fd_ = fd;
    family_ = local.family();
    loop_->begin_work();
    work_ref_ = true;
    return {};
  }

  VoidResult listen(int backlog, AcceptCallback callback) {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (listening_) {
      return std::unexpected(make_error(ErrorCode::AlreadyExists));
    }

    if (::listen(fd_, backlog) != 0) {
      return std::unexpected(from_socket_error(errno));
    }

    listening_ = true;
    accept_callback_ = std::move(callback);
    watching_ = linux_poller(*loop_).add(this, fd_, EPOLLIN);
    if (!watching_) {
      listening_ = false;
      accept_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  [[nodiscard]] Result<SocketAddress> local_address() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return socket_local_address(fd_);
  }

  [[nodiscard]] bool is_open() const noexcept { return fd_ >= 0; }

  void close() { close_impl(); }

  void on_io(std::uint32_t events) override {
    auto self = shared_from_this();
    if (!listening_ || fd_ < 0) {
      return;
    }
    if ((events & (EPOLLIN | EPOLLERR | EPOLLHUP)) == 0) {
      return;
    }

    while (listening_ && fd_ >= 0) {
      sockaddr_storage storage{};
      socklen_t length = sizeof(storage);
      const int client = ::accept4(fd_,
                                   reinterpret_cast<sockaddr*>(&storage),
                                   &length,
                                   SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (client < 0) {
        const int error = errno;
        if (error == EINTR) {
          continue;
        }
        if (error == EAGAIN || error == EWOULDBLOCK) {
          return;
        }

        auto callback = accept_callback_;
        if (callback) {
          callback(std::unexpected(from_socket_error(error)));
        }
        return;
      }

      const Family family =
          AddressCodec::family_from_native(reinterpret_cast<sockaddr*>(&storage)->sa_family);

      auto stream = std::make_shared<TcpStreamImpl>(loop_);
      stream->adopt_fd(client, family);

      auto callback = accept_callback_;
      if (callback) {
        callback(TcpStream(stream));
      }
    }
  }

 private:
  void close_impl() {
    if (fd_ < 0 && !work_ref_) {
      return;
    }

    auto self = shared_from_this();
    listening_ = false;

    if (fd_ >= 0) {
      if (watching_) {
        linux_poller(*loop_).del(this);
        watching_ = false;
      }
      (void)::close(fd_);
      fd_ = -1;
    }

    accept_callback_ = nullptr;

    release_ref();
  }

  void release_ref() {
    if (work_ref_) {
      work_ref_ = false;
      loop_->end_work();
    }
  }

  void destroy() noexcept {
    listening_ = false;
    if (fd_ >= 0 && watching_) {
      linux_poller(*loop_).del(this);
      watching_ = false;
    }
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
    accept_callback_ = nullptr;
    release_ref();
  }

  std::shared_ptr<LoopImpl> loop_;
  int fd_ = -1;
  Family family_ = Family::Unspecified;
  bool listening_ = false;
  bool watching_ = false;
  bool work_ref_ = false;
  AcceptCallback accept_callback_;
};

} // namespace detail

TcpStream::TcpStream(Loop& loop) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::TcpStreamImpl>(impl->shared_from_this());
}

TcpStream::TcpStream(std::shared_ptr<detail::TcpStreamImpl> impl) : impl_(std::move(impl)) {}

TcpStream::~TcpStream() = default;

VoidResult TcpStream::bind(const SocketAddress& local, const BindOptions& options) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->bind(local, options);
}

VoidResult TcpStream::connect(const SocketAddress& remote, ConnectCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->connect(remote, std::move(cb));
}

VoidResult TcpStream::set_read_options(const ReadOptions& options) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_read_options(options);
}

VoidResult TcpStream::start_read(ReadCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->start_read(std::move(cb));
}

VoidResult TcpStream::start_read(SpanReadCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->start_read(std::move(cb));
}

void TcpStream::stop_read() {
  if (impl_) {
    impl_->stop_read();
  }
}

VoidResult TcpStream::write(std::span<const std::byte> data, WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->write(data, std::move(cb));
}

VoidResult TcpStream::write(WriteBuffer buffer, WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->write(std::move(buffer), std::move(cb));
}

VoidResult TcpStream::shutdown(WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->shutdown(std::move(cb));
}

void TcpStream::close() {
  if (impl_) {
    impl_->close(false);
  }
}

void TcpStream::close_reset() {
  if (impl_) {
    impl_->close_reset();
  }
}

bool TcpStream::is_open() const {
  return impl_ && impl_->is_open();
}

bool TcpStream::is_connected() const {
  return impl_ && impl_->is_connected();
}

Result<SocketAddress> TcpStream::local_address() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->local_address();
}

Result<SocketAddress> TcpStream::peer_address() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->peer_address();
}

VoidResult TcpStream::set_nodelay(bool enable) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_nodelay(enable);
}

VoidResult TcpStream::set_keepalive(const KeepAliveOptions& options) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_keepalive(options);
}

Result<int> TcpStream::recv_buffer_size() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->recv_buffer_size();
}

VoidResult TcpStream::set_recv_buffer_size(int bytes) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_recv_buffer_size(bytes);
}

Result<int> TcpStream::send_buffer_size() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_buffer_size();
}

VoidResult TcpStream::set_send_buffer_size(int bytes) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_send_buffer_size(bytes);
}

TcpListener::TcpListener(Loop& loop) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::TcpListenerImpl>(impl->shared_from_this());
}

TcpListener::~TcpListener() = default;

VoidResult TcpListener::bind(const SocketAddress& local, const BindOptions& options) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->bind(local, options);
}

VoidResult TcpListener::listen(int backlog, AcceptCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->listen(backlog, std::move(cb));
}

Result<SocketAddress> TcpListener::local_address() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->local_address();
}

bool TcpListener::is_open() const {
  return impl_ && impl_->is_open();
}

void TcpListener::close() {
  if (impl_) {
    impl_->close();
  }
}

} // namespace uvnet
