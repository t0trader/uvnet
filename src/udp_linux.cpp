#include "uvnet/udp.hpp"

#include <cerrno>
#include <cstring>
#include <deque>
#include <netinet/in.h>
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

constexpr std::size_t kMaxDatagramSize = 65536;

struct SendItem {
  std::vector<std::byte> owned;
  const std::byte* data = nullptr;
  std::size_t size = 0;
  SocketAddress destination;
  WriteCallback callback;
  bool has_destination = false;
  std::shared_ptr<const void> keepalive;
};

VoidResult apply_udp_options(int fd, Family family, const UdpOptions& options) {
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

int create_udp_socket(Family family) {
  return ::socket(detail::AddressCodec::native_family(family),
                  SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                  IPPROTO_UDP);
}

} // namespace

namespace detail {

class UdpSocketImpl final : public IoWatcher,
                            public std::enable_shared_from_this<UdpSocketImpl> {
 public:
  UdpSocketImpl(std::shared_ptr<LoopImpl> loop, Family family, UdpOptions options)
      : loop_(std::move(loop)), family_(family), options_(options) {}

  ~UdpSocketImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local) {
    if (local.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(local, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    if (::bind(fd_, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult connect(const SocketAddress& remote) {
    if (remote.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(remote, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }

    connected_ = true;
    connected_peer_ = remote;
    return {};
  }

  VoidResult disconnect() {
    if (fd_ < 0 || !connected_) {
      return {};
    }

    sockaddr_storage storage{};
    std::memset(&storage, 0, sizeof(storage));
    storage.ss_family = AF_UNSPEC;
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&storage), sizeof(sockaddr)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    connected_ = false;
    return {};
  }

  VoidResult send(std::span<const std::byte> data, WriteCallback callback) {
    if (!connected_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.owned.assign(data.begin(), data.end());
    item.data = item.owned.data();
    item.size = item.owned.size();
    item.callback = std::move(callback);
    item.has_destination = false;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult send(WriteBuffer buffer, WriteCallback callback) {
    if (!connected_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.data = buffer.data.data();
    item.size = buffer.data.size();
    item.keepalive = std::move(buffer.keepalive);
    item.callback = std::move(callback);
    item.has_destination = false;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult send_to(const SocketAddress& remote,
                     std::span<const std::byte> data,
                     WriteCallback callback) {
    if (remote.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.owned.assign(data.begin(), data.end());
    item.data = item.owned.data();
    item.size = item.owned.size();
    item.destination = remote;
    item.callback = std::move(callback);
    item.has_destination = true;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult send_to(const SocketAddress& remote,
                     WriteBuffer buffer,
                     WriteCallback callback) {
    if (remote.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.data = buffer.data.data();
    item.size = buffer.data.size();
    item.destination = remote;
    item.keepalive = std::move(buffer.keepalive);
    item.callback = std::move(callback);
    item.has_destination = true;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult set_recv_options(const UdpRecvOptions& options) {
    if (recv_started_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (options.slots == 0 || options.slots > 64 || options.chunk_size < 1024 ||
        options.chunk_size > 1024 * 1024) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    recv_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult start_recv(UdpRecvCallback callback) {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    keepalive_reads_ = true;
    view_callback_ = nullptr;
    recv_callback_ = std::move(callback);
    receiving_ = true;
    recv_started_ = true;
    update_interest();
    return {};
  }

  VoidResult start_recv(UdpRecvViewCallback callback) {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    keepalive_reads_ = false;
    recv_callback_ = nullptr;
    view_callback_ = std::move(callback);
    receiving_ = true;
    recv_started_ = true;
    update_interest();
    return {};
  }

  void stop_recv() {
    receiving_ = false;
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    update_interest();
  }

  VoidResult set_broadcast(bool enable) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    const int value = enable ? 1 : 0;
    if (::setsockopt(fd_, SOL_SOCKET, SO_BROADCAST, &value, sizeof(value)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult set_ttl(int ttl) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_UNICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    } else {
      if (::setsockopt(fd_, IPPROTO_IP, IP_TTL, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    }
    return {};
  }

  VoidResult set_multicast_ttl(int ttl) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    } else {
      unsigned char value = static_cast<unsigned char>(ttl);
      if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    }
    return {};
  }

  VoidResult set_multicast_loop(bool enable) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      const int value = enable ? 1 : 0;
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    } else {
      const unsigned char value = enable ? 1 : 0;
      if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    }
    return {};
  }

  VoidResult set_multicast_interface(const SocketAddress& iface) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    if (family_ == Family::IPv6) {
      const unsigned int index = iface.scope_id();
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_MULTICAST_IF, &index, sizeof(index)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
      return {};
    }

    if (iface.family() != Family::IPv4) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    in_addr address{};
    std::memcpy(&address, iface.bytes_.data(), 4);
    if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult set_membership(const SocketAddress& group,
                            const SocketAddress& iface,
                            bool join) {
    if (group.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    if (family_ == Family::IPv6) {
      ipv6_mreq request{};
      std::memcpy(&request.ipv6mr_multiaddr, group.bytes_.data(), 16);
      request.ipv6mr_interface = iface.scope_id();
      const int option = join ? IPV6_JOIN_GROUP : IPV6_LEAVE_GROUP;
      if (::setsockopt(fd_, IPPROTO_IPV6, option, &request, sizeof(request)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
      return {};
    }

    ip_mreq request{};
    std::memcpy(&request.imr_multiaddr, group.bytes_.data(), 4);
    if (iface.family() == Family::IPv4) {
      std::memcpy(&request.imr_interface, iface.bytes_.data(), 4);
    } else {
      request.imr_interface.s_addr = htonl(INADDR_ANY);
    }
    const int option = join ? IP_ADD_MEMBERSHIP : IP_DROP_MEMBERSHIP;
    if (::setsockopt(fd_, IPPROTO_IP, option, &request, sizeof(request)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  Result<SocketAddress> local_address() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage),
                                     static_cast<int>(length));
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
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
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
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  [[nodiscard]] bool is_open() const noexcept { return fd_ >= 0; }
  [[nodiscard]] bool is_connected() const noexcept { return connected_; }

  void close() { destroy_with_callbacks(); }

  void on_io(std::uint32_t events) override {
    auto self = shared_from_this();
    if (fd_ < 0) {
      return;
    }
    if ((events & (EPOLLIN | EPOLLERR | EPOLLHUP)) != 0) {
      do_recv();
    }
    if ((events & (EPOLLOUT | EPOLLERR)) != 0) {
      flush_sends();
    }
  }

 private:
  VoidResult ensure_socket() {
    if (fd_ >= 0) {
      return {};
    }

    const int fd = create_udp_socket(family_);
    if (fd < 0) {
      return std::unexpected(from_socket_error(errno));
    }

    const auto applied = apply_udp_options(fd, family_, options_);
    if (!applied) {
      (void)::close(fd);
      return applied;
    }

    fd_ = fd;
    loop_->begin_work();
    work_ref_ = true;
    return {};
  }

  void update_interest() {
    if (fd_ < 0) {
      return;
    }

    std::uint32_t wanted = 0;
    if (receiving_) {
      wanted |= EPOLLIN;
    }
    if (!sends_.empty()) {
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
    } else if (wanted != interest_) {
      (void)poller.mod(this, wanted);
      interest_ = wanted;
    }
  }

  void deliver_recv_error(Error error) {
    if (keepalive_reads_) {
      auto callback = std::move(recv_callback_);
      recv_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    } else {
      auto callback = std::move(view_callback_);
      view_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    }
  }

  void do_recv() {
    if (!receiving_ || fd_ < 0) {
      return;
    }

    if (keepalive_reads_ && !recv_pool_) {
      recv_pool_ = BufferPool::create(recv_chunk_size_, 32);
    }
    if (!keepalive_reads_ && span_recv_buffer_.empty()) {
      span_recv_buffer_.resize(recv_chunk_size_);
    }

    auto self = shared_from_this();
    while (receiving_ && fd_ >= 0) {
      sockaddr_storage storage{};
      socklen_t length = sizeof(storage);
      BufferPool::Handle block;
      std::byte* data = nullptr;
      std::size_t capacity = 0;
      if (keepalive_reads_) {
        block = recv_pool_->acquire();
        data = block.data.data();
        capacity = block.data.size();
      } else {
        data = span_recv_buffer_.data();
        capacity = span_recv_buffer_.size();
      }

      const ssize_t count = ::recvfrom(fd_,
                                       data,
                                       capacity,
                                       0,
                                       reinterpret_cast<sockaddr*>(&storage),
                                       &length);

      if (count >= 0) {
        const SocketAddress source =
            AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage),
                                      static_cast<int>(length));
        const std::span<const std::byte> bytes(data, static_cast<std::size_t>(count));
        if (keepalive_reads_) {
          auto callback = recv_callback_;
          if (callback) {
            callback(Datagram{bytes, source, block.keepalive});
          }
        } else {
          auto callback = view_callback_;
          if (callback) {
            callback(DatagramView{bytes, source});
          }
        }
        continue;
      }

      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      if (error == EAGAIN || error == EWOULDBLOCK) {
        return;
      }

      receiving_ = false;
      update_interest();
      deliver_recv_error(from_socket_error(error));
      return;
    }
  }

  void flush_sends() {
    auto self = shared_from_this();

    while (!sends_.empty() && fd_ >= 0) {
      SendItem& item = sends_.front();
      ssize_t count = 0;

      if (item.has_destination) {
        sockaddr_storage storage{};
        int length = 0;
        if (!AddressCodec::to_native(item.destination, storage, length)) {
          WriteCallback callback = std::move(item.callback);
          if (callback) {
            callback(std::unexpected(make_error(ErrorCode::InvalidArgument)));
          }
          sends_.pop_front();
          continue;
        }
        count = ::sendto(fd_,
                         item.data,
                         item.size,
                         0,
                         reinterpret_cast<sockaddr*>(&storage),
                         static_cast<socklen_t>(length));
      } else {
        count = ::send(fd_, item.data, item.size, 0);
      }

      if (count >= 0) {
        WriteCallback callback = std::move(item.callback);
        auto keepalive = std::move(item.keepalive);
        auto owned = std::move(item.owned);
        sends_.pop_front();
        if (callback) {
          // 与 Windows/IOCP 语义一致：发送完成回调异步投递，避免调用方递归；
          // 同时把底层存储保持到回调执行。
          loop_->post([callback = std::move(callback),
                       keepalive = std::move(keepalive),
                       owned = std::move(owned)]() mutable { callback({}); });
        }
        continue;
      }

      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      if (error == EAGAIN || error == EWOULDBLOCK) {
        break;
      }

      fail_sends(from_socket_error(error));
      return;
    }

    update_interest();
  }

  void fail_sends(Error error) {
    std::deque<SendItem> pending;
    pending.swap(sends_);
    for (auto& item : pending) {
      if (item.callback) {
        loop_->post([callback = std::move(item.callback),
                     keepalive = std::move(item.keepalive),
                     owned = std::move(item.owned),
                     error]() mutable { callback(std::unexpected(error)); });
      }
    }
    update_interest();
  }

  void destroy_with_callbacks() {
    if (fd_ < 0 && !work_ref_) {
      return;
    }

    auto self = shared_from_this();
    if (fd_ >= 0 && watching_) {
      linux_poller(*loop_).del(this);
      watching_ = false;
    }
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
    connected_ = false;
    receiving_ = false;

    auto recv_cb = std::move(recv_callback_);
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    std::deque<SendItem> pending;
    pending.swap(sends_);
    release_ref();

    if (recv_cb) {
      recv_cb(std::unexpected(make_error(ErrorCode::Canceled)));
    }
    for (auto& item : pending) {
      if (item.callback) {
        item.callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
    }
  }

  void destroy() noexcept {
    if (fd_ >= 0 && watching_) {
      linux_poller(*loop_).del(this);
      watching_ = false;
    }
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    sends_.clear();
    release_ref();
  }

  void release_ref() {
    if (work_ref_) {
      work_ref_ = false;
      loop_->end_work();
    }
  }

  std::shared_ptr<LoopImpl> loop_;
  Family family_ = Family::IPv4;
  UdpOptions options_;
  int fd_ = -1;
  bool connected_ = false;
  bool receiving_ = false;
  bool watching_ = false;
  bool work_ref_ = false;
  std::uint32_t interest_ = 0;
  SocketAddress connected_peer_;
  UdpRecvCallback recv_callback_;
  UdpRecvViewCallback view_callback_;
  bool keepalive_reads_ = false;
  std::size_t recv_chunk_size_ = kMaxDatagramSize;
  bool recv_started_ = false;
  std::vector<std::byte> span_recv_buffer_;
  std::shared_ptr<BufferPool> recv_pool_;
  std::deque<SendItem> sends_;
};

} // namespace detail

UdpSocket::UdpSocket(Loop& loop, Family family, const UdpOptions& options) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::UdpSocketImpl>(impl->shared_from_this(), family, options);
}

UdpSocket::~UdpSocket() = default;

VoidResult UdpSocket::bind(const SocketAddress& local) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->bind(local);
}

VoidResult UdpSocket::connect(const SocketAddress& remote) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->connect(remote);
}

VoidResult UdpSocket::disconnect() {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->disconnect();
}

VoidResult UdpSocket::send(std::span<const std::byte> data, WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send(data, std::move(cb));
}

VoidResult UdpSocket::send(WriteBuffer buffer, WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send(std::move(buffer), std::move(cb));
}

VoidResult UdpSocket::send_to(const SocketAddress& remote,
                              std::span<const std::byte> data,
                              WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_to(remote, data, std::move(cb));
}

VoidResult UdpSocket::send_to(const SocketAddress& remote,
                              WriteBuffer buffer,
                              WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_to(remote, std::move(buffer), std::move(cb));
}

VoidResult UdpSocket::set_recv_options(const UdpRecvOptions& options) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_recv_options(options);
}

VoidResult UdpSocket::start_recv(UdpRecvCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->start_recv(std::move(cb));
}

VoidResult UdpSocket::start_recv(UdpRecvViewCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->start_recv(std::move(cb));
}

void UdpSocket::stop_recv() {
  if (impl_) {
    impl_->stop_recv();
  }
}

VoidResult UdpSocket::set_broadcast(bool enable) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_broadcast(enable);
}

VoidResult UdpSocket::set_ttl(int ttl) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_ttl(ttl);
}

VoidResult UdpSocket::set_multicast_ttl(int ttl) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_multicast_ttl(ttl);
}

VoidResult UdpSocket::set_multicast_loop(bool enable) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_multicast_loop(enable);
}

VoidResult UdpSocket::set_multicast_interface(const SocketAddress& iface) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_multicast_interface(iface);
}

VoidResult UdpSocket::set_membership(const SocketAddress& group,
                                     const SocketAddress& iface,
                                     bool join) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_membership(group, iface, join);
}

Result<SocketAddress> UdpSocket::local_address() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->local_address();
}

Result<int> UdpSocket::recv_buffer_size() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->recv_buffer_size();
}

VoidResult UdpSocket::set_recv_buffer_size(int bytes) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_recv_buffer_size(bytes);
}

Result<int> UdpSocket::send_buffer_size() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_buffer_size();
}

VoidResult UdpSocket::set_send_buffer_size(int bytes) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_send_buffer_size(bytes);
}

bool UdpSocket::is_open() const {
  return impl_ && impl_->is_open();
}

bool UdpSocket::is_connected() const {
  return impl_ && impl_->is_connected();
}

void UdpSocket::close() {
  if (impl_) {
    impl_->close();
  }
}

} // namespace uvnet
