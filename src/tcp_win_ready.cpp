#include "uvnet/tcp.hpp"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <utility>
#include <vector>

#include "detail/buffer_pool.hpp"
#include "detail/platform.hpp"
#include "detail/ready_poller.hpp"
#include "detail/tcp_impl_win.hpp"
#include "detail/win_socket.hpp"
#include "detail/wepoll/wepoll.h"

namespace uvnet {
namespace detail {

namespace {

constexpr std::size_t kReadBufferSize = 64 * 1024;
inline constexpr std::size_t kMaxFreeWrites = 8;
constexpr std::size_t kMaxAcceptPerEvent = 16;
constexpr int kDefaultTcpBuffer = 256 * 1024;

} // namespace

struct ReadyWriteItem {
  std::vector<std::byte> owned;
  const std::byte* data = nullptr;
  std::size_t size = 0;
  std::size_t offset = 0;
  WriteCallback callback;
  std::shared_ptr<const void> keepalive;
};

// 就绪式 TCP 连接：非阻塞 socket + wepoll 就绪通知 + 同步 recv/send。
// 写操作在队列为空时直接 send，全部收下就同步回调，避免完成端口往返。
class ReadyTcpStreamImpl final : public TcpStreamImpl,
                                 public ReadyHandler,
                                 public std::enable_shared_from_this<ReadyTcpStreamImpl> {
 public:
  explicit ReadyTcpStreamImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}
  ~ReadyTcpStreamImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local, const BindOptions& options) override {
    if (socket_ != INVALID_SOCKET && (connecting_ || connected_)) {
      return std::unexpected(make_error(ErrorCode::AlreadyConnected));
    }
    if (local.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!ensure_socket(local.family())) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }

    auto applied = win_apply_bind_options(socket_, local.family(), options);
    if (!applied) {
      return applied;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(local, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (::bind(socket_, reinterpret_cast<sockaddr*>(&storage), length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    bound_ = true;
    return {};
  }

  VoidResult connect(const SocketAddress& remote, ConnectCallback callback) override {
    if (socket_ != INVALID_SOCKET && connecting_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (socket_ != INVALID_SOCKET && connected_) {
      return std::unexpected(make_error(ErrorCode::AlreadyConnected));
    }
    if (remote.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!ensure_socket(remote.family())) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(remote, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    connect_callback_ = std::move(callback);
    const int result =
        ::connect(socket_, reinterpret_cast<sockaddr*>(&storage), length);
    if (result == 0) {
      connected_ = true;
      auto queued = std::move(connect_callback_);
      connect_callback_ = nullptr;
      // 与 IOCP/Linux 语义一致：即使立即成功也异步回调
      auto loop = loop_;
      loop_->post([queued = std::move(queued)] {
        if (queued) {
          queued({});
        }
      });
      return {};
    }

    const int error = WSAGetLastError();
    if (error != WSAEWOULDBLOCK && error != WSAEINPROGRESS) {
      connect_callback_ = nullptr;
      return std::unexpected(from_socket_error(error));
    }

    connecting_ = true;
    if (!update_interest()) {
      connecting_ = false;
      connect_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  VoidResult set_read_options(const ReadOptions& options) override {
    if (read_started_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (options.slots == 0 || options.slots > 64 || options.chunk_size < 1024 ||
        options.chunk_size > 1024 * 1024) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    // 就绪式后端一次事件只入队一次 recv，slots 字段忽略
    read_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult start_read(ReadCallback callback) override {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!connected_ || socket_ == INVALID_SOCKET || closing_) {
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
    if (!read_pool_) {
      read_pool_ = BufferPool::create(read_chunk_size_, 32);
    }
    if (!update_interest()) {
      reading_ = false;
      read_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  VoidResult start_read(SpanReadCallback callback) override {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (!connected_ || socket_ == INVALID_SOCKET || closing_) {
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
    if (span_buffer_.empty()) {
      span_buffer_.resize(read_chunk_size_);
    }
    if (!update_interest()) {
      reading_ = false;
      span_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  void stop_read() override {
    reading_ = false;
    read_callback_ = nullptr;
    span_callback_ = nullptr;
    (void)update_interest();
  }

  VoidResult write(std::span<const std::byte> data, WriteCallback callback) override {
    if (socket_ == INVALID_SOCKET || !connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_issued_) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }
    if (data.empty()) {
      return {};
    }

    // 写队列空闲时直接发送：成功后同步回调，省掉写项分配与 deque 操作。
    // 部分发送才回落到排队路径，只把剩余字节拷进队列。
    if (writes_.empty() && !flushing_) {
      const int n = ::send(socket_,
                           reinterpret_cast<const char*>(data.data()),
                           static_cast<int>(data.size()),
                           0);
      if (n > 0) {
        const std::size_t sent = static_cast<std::size_t>(n);
        if (sent == data.size()) {
          if (callback) {
            callback({});
          }
          return {};
        }
        ReadyWriteItem* item = acquire_write_item();
        item->owned.assign(data.data() + sent, data.data() + data.size());
        item->data = item->owned.data();
        item->size = item->owned.size();
        item->offset = 0;
        item->keepalive.reset();
        item->callback = std::move(callback);
        writes_.push_back(item);
        flush_writes();
        return {};
      }
      // n == 0 或 WSAEWOULDBLOCK：回落到正常排队路径
    }

    ReadyWriteItem* item = acquire_write_item();
    item->owned.assign(data.begin(), data.end());
    item->data = item->owned.data();
    item->size = item->owned.size();
    item->offset = 0;
    item->keepalive.reset();
    item->callback = std::move(callback);
    writes_.push_back(item);

    flush_writes();
    return {};
  }

  VoidResult write(WriteBuffer buffer, WriteCallback callback) override {
    if (socket_ == INVALID_SOCKET || !connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_issued_) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }
    if (buffer.data.empty()) {
      return {};
    }

    // 同 write(span)：队列空闲时直接发送，部分发送才入队
    if (writes_.empty() && !flushing_) {
      const int n = ::send(socket_,
                           reinterpret_cast<const char*>(buffer.data.data()),
                           static_cast<int>(buffer.data.size()),
                           0);
      if (n > 0) {
        const std::size_t sent = static_cast<std::size_t>(n);
        if (sent == buffer.data.size()) {
          if (callback) {
            callback({});
          }
          return {};
        }
        ReadyWriteItem* item = acquire_write_item();
        item->owned.clear();
        item->data = buffer.data.data() + sent;
        item->size = buffer.data.size() - sent;
        item->offset = 0;
        item->keepalive = std::move(buffer.keepalive);
        item->callback = std::move(callback);
        writes_.push_back(item);
        flush_writes();
        return {};
      }
    }

    ReadyWriteItem* item = acquire_write_item();
    item->owned.clear();
    item->data = buffer.data.data();
    item->size = buffer.data.size();
    item->offset = 0;
    item->keepalive = std::move(buffer.keepalive);
    item->callback = std::move(callback);
    writes_.push_back(item);

    flush_writes();
    return {};
  }

  VoidResult shutdown(WriteCallback callback) override {
    if (socket_ == INVALID_SOCKET || !connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_issued_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }

    shutdown_callback_ = std::move(callback);
    shutdown_pending_ = true;
    if (writes_.empty()) {
      do_shutdown();
    } else {
      (void)update_interest();
    }
    return {};
  }

  void close(bool reset) override { close_impl(reset); }

  [[nodiscard]] bool is_open() const override { return socket_ != INVALID_SOCKET; }
  [[nodiscard]] bool is_connected() const override { return connected_; }

  Result<SocketAddress> local_address() const override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_local_address(socket_);
  }

  Result<SocketAddress> peer_address() const override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_peer_address(socket_);
  }

  VoidResult set_nodelay(bool enable) override {
    nodelay_ = enable;
    if (socket_ == INVALID_SOCKET) {
      return {};
    }
    const int value = enable ? 1 : 0;
    if (win_socket_setopt(socket_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  VoidResult set_keepalive(const KeepAliveOptions& options) override {
    keepalive_ = options;
    if (socket_ == INVALID_SOCKET) {
      return {};
    }
    return win_apply_keepalive(socket_, options);
  }

  Result<int> recv_buffer_size() const override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    int length = static_cast<int>(sizeof(value));
    if (win_socket_getopt(socket_, SOL_SOCKET, SO_RCVBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return value;
  }

  VoidResult set_recv_buffer_size(int bytes) override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (bytes < 0) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (win_socket_setopt(socket_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  Result<int> send_buffer_size() const override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    int length = static_cast<int>(sizeof(value));
    if (win_socket_getopt(socket_, SOL_SOCKET, SO_SNDBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return value;
  }

  VoidResult set_send_buffer_size(int bytes) override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (bytes < 0) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (win_socket_setopt(socket_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  void on_ready(std::uint32_t events) override {
    if (closing_ || socket_ == INVALID_SOCKET) {
      return;
    }
    auto self = shared_from_this();

    if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
      handle_poll_error(events);
      return;
    }

    if ((events & EPOLLOUT) != 0) {
      if (connecting_) {
        finish_connect();
      } else {
        flush_writes();
      }
      if (closing_) {
        return;
      }
    }

    if ((events & EPOLLIN) != 0) {
      read_once();
    }
  }

  // 由监听器在 accept 后调用：socket 已经是非阻塞、已连接状态
  void adopt(SOCKET socket, Family family) {
    socket_ = socket;
    family_ = family;
    bound_ = true;
    connected_ = true;
    loop_->begin_work();
    work_ref_ = true;
    apply_default_options();
  }

 private:
  void apply_default_options() {
    if (socket_ == INVALID_SOCKET) {
      return;
    }
    (void)win_socket_setopt(socket_,
                            SOL_SOCKET,
                            SO_RCVBUF,
                            &kDefaultTcpBuffer,
                            static_cast<int>(sizeof(kDefaultTcpBuffer)));
    (void)win_socket_setopt(socket_,
                            SOL_SOCKET,
                            SO_SNDBUF,
                            &kDefaultTcpBuffer,
                            static_cast<int>(sizeof(kDefaultTcpBuffer)));
    if (nodelay_) {
      const int value = 1;
      (void)win_socket_setopt(socket_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
    }
    if (keepalive_.enable) {
      (void)win_apply_keepalive(socket_, keepalive_);
    }
  }

  bool ensure_socket(Family family) {
    if (socket_ != INVALID_SOCKET) {
      return family_ == family;
    }

    ensure_winsock();
    const SOCKET socket = ::socket(AddressCodec::native_family(family), SOCK_STREAM, IPPROTO_TCP);
    if (socket == INVALID_SOCKET) {
      return false;
    }
    if (!win_set_nonblocking(socket)) {
      (void)::closesocket(socket);
      return false;
    }

    socket_ = socket;
    family_ = family;
    loop_->begin_work();
    work_ref_ = true;
    apply_default_options();
    return true;
  }

  // 事件掩码按当前状态推导：读需要 EPOLLIN，连接中/有积压写需要 EPOLLOUT
  bool update_interest() {
    if (socket_ == INVALID_SOCKET || closing_) {
      return false;
    }

    std::uint32_t events = 0;
    if (reading_ && !eof_) {
      events |= EPOLLIN;
    }
    if (connecting_ || !writes_.empty()) {
      events |= EPOLLOUT;
    }

    if (events == 0) {
      if (attached_) {
        (void)ready_poller(*loop_).detach(socket_);
        attached_ = false;
      }
      interest_ = 0;
      return true;
    }

    // 事件掩码未变化时不重复 epoll_ctl（每次 MOD 都是一次内核往返，热路径很贵）
    if (attached_ && events == interest_) {
      return true;
    }

    if (!attached_) {
      if (!ready_poller(*loop_).attach(socket_, this, events)) {
        return false;
      }
      attached_ = true;
      interest_ = events;
      return true;
    }

    if (!ready_poller(*loop_).modify(socket_, this, events)) {
      return false;
    }
    interest_ = events;
    return true;
  }

  void finish_connect() {
    connecting_ = false;

    int error = 0;
    int length = static_cast<int>(sizeof(error));
    if (::getsockopt(socket_,
                     SOL_SOCKET,
                     SO_ERROR,
                     reinterpret_cast<char*>(&error),
                     &length) != 0) {
      error = WSAGetLastError();
    }

    auto callback = std::move(connect_callback_);
    connect_callback_ = nullptr;

    if (error != 0) {
      const Error mapped = from_socket_error(error);
      (void)update_interest();
      if (callback) {
        callback(std::unexpected(mapped));
      }
      return;
    }

    connected_ = true;
    (void)update_interest();
    if (callback) {
      callback({});
    }
  }

  void flush_writes() {
    if (flushing_ || closing_ || socket_ == INVALID_SOCKET) {
      return;
    }
    flushing_ = true;
    auto self = shared_from_this();

    while (!writes_.empty() && !closing_) {
      ReadyWriteItem* item = writes_.front();
      while (item->offset < item->size) {
        const int n = ::send(socket_,
                             reinterpret_cast<const char*>(item->data + item->offset),
                             static_cast<int>(item->size - item->offset),
                             0);
        if (n > 0) {
          item->offset += static_cast<std::size_t>(n);
          continue;
        }
        if (n < 0) {
          const int error = WSAGetLastError();
          if (error != WSAEWOULDBLOCK) {
            flushing_ = false;
            fail_writes(from_socket_error(error));
            (void)update_interest();
            return;
          }
        }
        break;
      }

      if (item->offset < item->size) {
        flushing_ = false;
        (void)update_interest();
        return;
      }

      auto callback = std::move(item->callback);
      item->callback = nullptr;
      writes_.pop_front();
      recycle_write_item(item);

      if (callback) {
        callback({});
        self = shared_from_this();
        if (closing_) {
          flushing_ = false;
          return;
        }
      }
    }

    flushing_ = false;
    (void)update_interest();
    if (!closing_ && writes_.empty() && shutdown_pending_ && !shutdown_issued_) {
      do_shutdown();
    }
  }

  void fail_writes(Error error) {
    std::deque<ReadyWriteItem*> pending;
    pending.swap(writes_);
    for (auto* item : pending) {
      auto callback = std::move(item->callback);
      item->callback = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
      recycle_write_item(item);
    }
  }

  void do_shutdown() {
    if (shutdown_issued_) {
      return;
    }
    shutdown_pending_ = false;
    shutdown_issued_ = true;

    VoidResult result;
    if (socket_ == INVALID_SOCKET) {
      result = std::unexpected(make_error(ErrorCode::NotConnected));
    } else if (::shutdown(socket_, SD_SEND) != 0) {
      result = std::unexpected(from_socket_error(WSAGetLastError()));
    }

    auto callback = std::move(shutdown_callback_);
    shutdown_callback_ = nullptr;
    (void)update_interest();
    if (callback) {
      callback(result);
    }
  }

  void read_once() {
    if (!reading_ || closing_ || socket_ == INVALID_SOCKET) {
      return;
    }

    std::span<std::byte> target;
    std::shared_ptr<const void> keepalive;
    if (keepalive_reads_) {
      if (!read_pool_) {
        read_pool_ = BufferPool::create(read_chunk_size_, 32);
      }
      BufferPool::Handle block = read_pool_->acquire();
      target = block.data;
      keepalive = std::move(block.keepalive);
    } else {
      if (span_buffer_.empty()) {
        span_buffer_.resize(read_chunk_size_);
      }
      target = std::span<std::byte>(span_buffer_.data(), span_buffer_.size());
    }

    const int n = ::recv(socket_,
                         reinterpret_cast<char*>(target.data()),
                         static_cast<int>(target.size()),
                         0);
    if (n > 0) {
      auto self = shared_from_this();
      if (keepalive_reads_) {
        auto callback = read_callback_;
        if (callback) {
          callback(ReadChunk{std::span<const std::byte>(target.data(),
                                                        static_cast<std::size_t>(n)),
                             keepalive});
        }
      } else {
        auto callback = span_callback_;
        if (callback) {
          callback(std::span<const std::byte>(target.data(), static_cast<std::size_t>(n)));
        }
      }
      return;
    }

    if (n == 0) {
      eof_ = true;
      reading_ = false;
      deliver_read_error(make_error(ErrorCode::EndOfFile));
      (void)update_interest();
      return;
    }

    const int error = WSAGetLastError();
    if (error == WSAEWOULDBLOCK) {
      return;
    }
    reading_ = false;
    deliver_read_error(from_socket_error(error));
    (void)update_interest();
  }

  void handle_poll_error(std::uint32_t events) {
    int error = 0;
    int length = static_cast<int>(sizeof(error));
    if (::getsockopt(socket_,
                     SOL_SOCKET,
                     SO_ERROR,
                     reinterpret_cast<char*>(&error),
                     &length) != 0) {
      error = WSAGetLastError();
    }

    if (error == 0) {
      // 没有具体错误：对端优雅关闭时按可读处理，靠 recv==0 产出 EOF
      if (reading_ || (events & EPOLLIN) != 0) {
        read_once();
      }
      return;
    }

    const Error mapped = from_socket_error(error);

    connecting_ = false;
    auto connect_callback = std::move(connect_callback_);
    connect_callback_ = nullptr;
    if (connect_callback) {
      connect_callback(std::unexpected(mapped));
    }

    reading_ = false;
    deliver_read_error(mapped);
    fail_writes(mapped);
    (void)update_interest();
  }

  void deliver_read_error(Error error) {
    if (keepalive_reads_) {
      auto callback = std::move(read_callback_);
      read_callback_ = nullptr;
      span_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    } else {
      auto callback = std::move(span_callback_);
      span_callback_ = nullptr;
      read_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    }
  }

  ReadyWriteItem* acquire_write_item() {
    if (!free_writes_.empty()) {
      auto* item = free_writes_.back();
      free_writes_.pop_back();
      return item;
    }
    return new ReadyWriteItem{};
  }

  void recycle_write_item(ReadyWriteItem* item) {
    item->keepalive.reset();
    item->data = nullptr;
    item->size = 0;
    item->offset = 0;
    item->callback = nullptr;
    if (free_writes_.size() >= kMaxFreeWrites) {
      delete item;
      return;
    }
    free_writes_.push_back(item);
  }

  void close_impl(bool reset) {
    if (closing_) {
      return;
    }
    closing_ = true;
    connected_ = false;
    reading_ = false;
    connecting_ = false;

    read_callback_ = nullptr;
    span_callback_ = nullptr;

    auto connect_callback = std::move(connect_callback_);
    connect_callback_ = nullptr;
    auto shutdown_callback = std::move(shutdown_callback_);
    shutdown_callback_ = nullptr;

    std::deque<ReadyWriteItem*> queued;
    queued.swap(writes_);

    if (socket_ != INVALID_SOCKET) {
      if (attached_) {
        (void)ready_poller(*loop_).detach(socket_);
        attached_ = false;
      }
      if (reset) {
        linger option{};
        option.l_onoff = 1;
        option.l_linger = 0;
        (void)win_socket_setopt(socket_, SOL_SOCKET, SO_LINGER, &option, sizeof(option));
      }
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }

    for (auto* item : queued) {
      auto callback = std::move(item->callback);
      item->callback = nullptr;
      if (callback) {
        callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      recycle_write_item(item);
    }

    if (connect_callback) {
      connect_callback(std::unexpected(make_error(ErrorCode::Canceled)));
    }
    if (shutdown_callback) {
      shutdown_callback(std::unexpected(make_error(ErrorCode::Canceled)));
    }

    release_ref();
  }

  void destroy() {
    closing_ = true;
    connected_ = false;
    reading_ = false;
    connecting_ = false;

    if (socket_ != INVALID_SOCKET) {
      if (attached_) {
        (void)ready_poller(*loop_).detach(socket_);
        attached_ = false;
      }
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }

    read_callback_ = nullptr;
    span_callback_ = nullptr;
    connect_callback_ = nullptr;
    shutdown_callback_ = nullptr;

    for (auto* item : writes_) {
      delete item;
    }
    writes_.clear();
    for (auto* item : free_writes_) {
      delete item;
    }
    free_writes_.clear();

    read_pool_.reset();
    span_buffer_.clear();
    release_ref();
  }

  void release_ref() {
    if (work_ref_) {
      work_ref_ = false;
      loop_->end_work();
    }
  }

  std::shared_ptr<LoopImpl> loop_;
  SOCKET socket_ = INVALID_SOCKET;
  Family family_ = Family::Unspecified;
  bool bound_ = false;
  bool connecting_ = false;
  bool connected_ = false;
  bool reading_ = false;
  bool eof_ = false;
  bool closing_ = false;
  bool attached_ = false;
  std::uint32_t interest_ = 0;
  bool work_ref_ = false;
  bool flushing_ = false;

  bool nodelay_ = true;
  KeepAliveOptions keepalive_{false, std::chrono::seconds(60)};

  ReadCallback read_callback_;
  SpanReadCallback span_callback_;
  bool keepalive_reads_ = false;
  std::size_t read_chunk_size_ = kReadBufferSize;
  bool read_started_ = false;
  std::shared_ptr<BufferPool> read_pool_;
  std::vector<std::byte> span_buffer_;

  ConnectCallback connect_callback_;
  WriteCallback shutdown_callback_;
  bool shutdown_pending_ = false;
  bool shutdown_issued_ = false;

  std::deque<ReadyWriteItem*> writes_;
  std::vector<ReadyWriteItem*> free_writes_;
};

// 就绪式监听器：非阻塞 accept，边沿不存在的 level-triggered 由 wepoll 反复通知
class ReadyTcpListenerImpl final : public TcpListenerImpl,
                                   public ReadyHandler,
                                   public std::enable_shared_from_this<ReadyTcpListenerImpl> {
 public:
  explicit ReadyTcpListenerImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}
  ~ReadyTcpListenerImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local, const BindOptions& options) override {
    if (socket_ != INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::AlreadyExists));
    }
    if (local.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    ensure_winsock();
    const SOCKET socket = ::socket(AddressCodec::native_family(local.family()),
                                   SOCK_STREAM,
                                   IPPROTO_TCP);
    if (socket == INVALID_SOCKET) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    if (!win_set_nonblocking(socket)) {
      (void)::closesocket(socket);
      return std::unexpected(make_error(ErrorCode::Unknown));
    }

    auto applied = win_apply_bind_options(socket, local.family(), options);
    if (!applied) {
      (void)::closesocket(socket);
      return applied;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(local, storage, length)) {
      (void)::closesocket(socket);
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (::bind(socket, reinterpret_cast<sockaddr*>(&storage), length) != 0) {
      const Error error = from_socket_error(WSAGetLastError());
      (void)::closesocket(socket);
      return std::unexpected(error);
    }

    socket_ = socket;
    family_ = local.family();
    loop_->begin_work();
    work_ref_ = true;
    return {};
  }

  VoidResult listen(int backlog, AcceptCallback callback) override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (listening_) {
      return std::unexpected(make_error(ErrorCode::AlreadyExists));
    }

    if (::listen(socket_, backlog) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }

    if (!ready_poller(*loop_).attach(socket_, this, EPOLLIN)) {
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    attached_ = true;
    listening_ = true;
    accept_callback_ = std::move(callback);
    return {};
  }

  void on_ready(std::uint32_t events) override {
    if (closing_ || !listening_ || socket_ == INVALID_SOCKET) {
      return;
    }
    auto self = shared_from_this();

    if ((events & (EPOLLERR | EPOLLHUP)) != 0 && (events & EPOLLIN) == 0) {
      int error = 0;
      int length = static_cast<int>(sizeof(error));
      (void)win_socket_getopt(socket_, SOL_SOCKET, SO_ERROR, &error, &length);
      auto callback = accept_callback_;
      if (callback) {
        callback(std::unexpected(from_socket_error(error != 0 ? error : WSAECONNRESET)));
      }
      if (closing_) {
        return;
      }
    }

    if ((events & EPOLLIN) == 0) {
      return;
    }

    // 一次事件里最多取 kMaxAcceptPerEvent 个，避免单连接洪峰饿死其他句柄
    for (std::size_t i = 0; i < kMaxAcceptPerEvent && listening_ && !closing_; ++i) {
      const SOCKET accepted = ::accept(socket_, nullptr, nullptr);
      if (accepted == INVALID_SOCKET) {
        const int error = WSAGetLastError();
        if (error == WSAEWOULDBLOCK) {
          return;
        }
        auto callback = accept_callback_;
        if (callback) {
          callback(std::unexpected(from_socket_error(error)));
        }
        return;
      }

      (void)win_set_nonblocking(accepted);
      auto stream = std::make_shared<ReadyTcpStreamImpl>(loop_);
      stream->adopt(accepted, family_);

      auto callback = accept_callback_;
      if (callback) {
        callback(adopt_stream(stream));
      }
    }
  }

  [[nodiscard]] Result<SocketAddress> local_address() const override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_local_address(socket_);
  }

  [[nodiscard]] bool is_open() const override { return socket_ != INVALID_SOCKET; }

  void close() override { close_impl(); }

 private:
  void close_impl() {
    if (closing_) {
      return;
    }
    closing_ = true;
    listening_ = false;
    accept_callback_ = nullptr;

    if (socket_ != INVALID_SOCKET) {
      if (attached_) {
        (void)ready_poller(*loop_).detach(socket_);
        attached_ = false;
      }
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }
    release_ref();
  }

  void destroy() {
    close_impl();
  }

  void release_ref() {
    if (work_ref_) {
      work_ref_ = false;
      loop_->end_work();
    }
  }

  std::shared_ptr<LoopImpl> loop_;
  SOCKET socket_ = INVALID_SOCKET;
  Family family_ = Family::Unspecified;
  bool listening_ = false;
  bool closing_ = false;
  bool attached_ = false;
  bool work_ref_ = false;
  AcceptCallback accept_callback_;
};

std::shared_ptr<TcpStreamImpl> create_ready_stream_impl(std::shared_ptr<LoopImpl> loop) {
  return std::make_shared<ReadyTcpStreamImpl>(std::move(loop));
}

std::shared_ptr<TcpListenerImpl> create_ready_listener_impl(std::shared_ptr<LoopImpl> loop) {
  return std::make_shared<ReadyTcpListenerImpl>(std::move(loop));
}

} // namespace detail
} // namespace uvnet
