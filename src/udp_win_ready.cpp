#include "uvnet/udp.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <utility>
#include <vector>

#include "detail/buffer_pool.hpp"
#include "detail/platform.hpp"
#include "detail/ready_poller.hpp"
#include "detail/udp_impl_win.hpp"
#include "detail/win_socket.hpp"
#include "detail/wepoll/wepoll.h"

namespace uvnet {
namespace detail {

namespace {

constexpr std::size_t kMaxDatagramSize = 65536;
constexpr std::size_t kMaxFreeSends = 8;
constexpr int kDefaultUdpBuffer = 256 * 1024;

} // namespace

struct ReadyUdpSend {
  std::vector<std::byte> owned;
  const std::byte* data = nullptr;
  std::size_t size = 0;
  WriteCallback callback;
  std::shared_ptr<const void> keepalive;
  sockaddr_storage destination{};
  int destination_length = 0;
  bool has_destination = false;
};

// 就绪式 UDP：非阻塞 recvfrom/sendto + wepoll 就绪通知。
// 发送队列为空时直接 sendto 并同步回调；遇到 WSAEWOULDBLOCK 才等 EPOLLOUT。
class ReadyUdpSocketImpl final : public UdpSocketImpl,
                                 public ReadyHandler,
                                 public std::enable_shared_from_this<ReadyUdpSocketImpl> {
 public:
  ReadyUdpSocketImpl(std::shared_ptr<LoopImpl> loop, Family family, UdpOptions options)
      : loop_(std::move(loop)), family_(family), options_(options) {}

  ~ReadyUdpSocketImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local) override {
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
    if (::bind(socket_, reinterpret_cast<sockaddr*>(&storage), length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  VoidResult connect(const SocketAddress& remote) override {
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
    if (::connect(socket_, reinterpret_cast<sockaddr*>(&storage), length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    connected_ = true;
    connected_peer_ = remote;
    return {};
  }

  VoidResult disconnect() override {
    return std::unexpected(make_error(ErrorCode::OperationNotSupported));
  }

  VoidResult send(std::span<const std::byte> data, WriteCallback callback) override {
    if (!connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    return queue_send(nullptr, 0, data, std::move(callback));
  }

  VoidResult send(WriteBuffer buffer, WriteCallback callback) override {
    if (!connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    return queue_send_buffer(nullptr, 0, std::move(buffer), std::move(callback));
  }

  VoidResult send_to(const SocketAddress& remote,
                     std::span<const std::byte> data,
                     WriteCallback callback) override {
    sockaddr_storage destination{};
    int length = 0;
    if (remote.family() != family_ || closing_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (!AddressCodec::to_native(remote, destination, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    return queue_send(&destination, length, data, std::move(callback));
  }

  VoidResult send_to(const SocketAddress& remote,
                     WriteBuffer buffer,
                     WriteCallback callback) override {
    sockaddr_storage destination{};
    int length = 0;
    if (remote.family() != family_ || closing_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (!AddressCodec::to_native(remote, destination, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    return queue_send_buffer(&destination, length, std::move(buffer), std::move(callback));
  }

  VoidResult start_recv(UdpRecvCallback callback) override {
    if (!callback || closing_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    keepalive_recv_ = true;
    view_callback_ = nullptr;
    recv_callback_ = std::move(callback);
    receiving_ = true;
    recv_started_ = true;
    if (!recv_pool_) {
      recv_pool_ = BufferPool::create(recv_chunk_size_, 32);
    }
    if (!update_interest()) {
      receiving_ = false;
      recv_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  VoidResult start_recv(UdpRecvViewCallback callback) override {
    if (!callback || closing_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    keepalive_recv_ = false;
    recv_callback_ = nullptr;
    view_callback_ = std::move(callback);
    receiving_ = true;
    recv_started_ = true;
    if (view_buffer_.empty()) {
      view_buffer_.resize(recv_chunk_size_);
    }
    if (!update_interest()) {
      receiving_ = false;
      view_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  void stop_recv() override {
    receiving_ = false;
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    (void)update_interest();
  }

  VoidResult set_recv_options(const UdpRecvOptions& options) override {
    if (recv_started_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (options.slots == 0 || options.slots > 64 || options.chunk_size < 1024 ||
        options.chunk_size > 1024 * 1024 || options.batch == 0 || options.batch > 64) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    // 就绪式后端用 batch 控制每次事件最多收多少个 datagram（slots 忽略）
    recv_batch_ = options.batch;
    recv_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult set_broadcast(bool enable) override {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    const int value = enable ? 1 : 0;
    if (win_socket_setopt(socket_, SOL_SOCKET, SO_BROADCAST, &value, sizeof(value)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  VoidResult set_ttl(int ttl) override {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (win_socket_setopt(socket_, IPPROTO_IPV6, IPV6_UNICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    } else {
      if (win_socket_setopt(socket_, IPPROTO_IP, IP_TTL, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    }
    return {};
  }

  VoidResult set_multicast_ttl(int ttl) override {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (win_socket_setopt(socket_, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    } else {
      if (win_socket_setopt(socket_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    }
    return {};
  }

  VoidResult set_multicast_loop(bool enable) override {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    const int value = enable ? 1 : 0;
    if (family_ == Family::IPv6) {
      if (win_socket_setopt(socket_, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, &value, sizeof(value)) !=
          0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    } else {
      if (win_socket_setopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    }
    return {};
  }

  VoidResult set_multicast_interface(const SocketAddress& iface) override {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    if (family_ == Family::IPv6) {
      const unsigned int index = static_cast<unsigned int>(iface.scope_id());
      if (win_socket_setopt(socket_, IPPROTO_IPV6, IPV6_MULTICAST_IF, &index, sizeof(index)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
      return {};
    }

    if (iface.family() != Family::IPv4) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    in_addr address{};
    std::memcpy(&address, address_bytes(iface).data(), 4);
    if (win_socket_setopt(socket_, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  VoidResult set_membership(const SocketAddress& group,
                            const SocketAddress& iface,
                            bool join) override {
    if (group.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    if (family_ == Family::IPv6) {
      ipv6_mreq request{};
      std::memcpy(&request.ipv6mr_multiaddr, address_bytes(group).data(), 16);
      request.ipv6mr_interface = iface.scope_id();
      const int option = join ? IPV6_JOIN_GROUP : IPV6_LEAVE_GROUP;
      if (win_socket_setopt(socket_, IPPROTO_IPV6, option, &request, sizeof(request)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
      return {};
    }

    ip_mreq request{};
    std::memcpy(&request.imr_multiaddr, address_bytes(group).data(), 4);
    if (iface.family() == Family::IPv4) {
      std::memcpy(&request.imr_interface, address_bytes(iface).data(), 4);
    } else {
      request.imr_interface.s_addr = htonl(INADDR_ANY);
    }
    const int option = join ? IP_ADD_MEMBERSHIP : IP_DROP_MEMBERSHIP;
    if (win_socket_setopt(socket_, IPPROTO_IP, option, &request, sizeof(request)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  Result<SocketAddress> local_address() const override {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_local_address(socket_);
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
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
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
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (win_socket_setopt(socket_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  [[nodiscard]] bool is_open() const override { return socket_ != INVALID_SOCKET; }
  [[nodiscard]] bool is_connected() const override { return connected_; }

  void close() override { close_impl(); }

  void on_ready(std::uint32_t events) override {
    if (closing_ || socket_ == INVALID_SOCKET) {
      return;
    }
    auto self = shared_from_this();

    if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
      int error = 0;
      int length = static_cast<int>(sizeof(error));
      (void)::getsockopt(socket_,
                         SOL_SOCKET,
                         SO_ERROR,
                         reinterpret_cast<char*>(&error),
                         &length);
      if (error != 0) {
        const Error mapped = from_socket_error(error);
        if (receiving_) {
          receiving_ = false;
          deliver_recv_error(mapped);
        }
        (void)update_interest();
      }
    }

    if ((events & EPOLLOUT) != 0 && !closing_) {
      flush_sends();
    }
    if ((events & EPOLLIN) != 0 && !closing_) {
      read_once();
    }
  }

 private:
  VoidResult ensure_socket() {
    if (socket_ != INVALID_SOCKET) {
      return {};
    }
    if (options_.reuse_port) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }

    ensure_winsock();
    const SOCKET socket =
        ::socket(AddressCodec::native_family(family_), SOCK_DGRAM, IPPROTO_UDP);
    if (socket == INVALID_SOCKET) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    if (!win_set_nonblocking(socket)) {
      (void)::closesocket(socket);
      return std::unexpected(make_error(ErrorCode::Unknown));
    }

    const int one = 1;
    if (options_.reuse_address &&
        win_socket_setopt(socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
      const Error error = from_socket_error(WSAGetLastError());
      (void)::closesocket(socket);
      return std::unexpected(error);
    }

    // 与 IOCP 后端一致的默认 256KB 收发缓冲
    (void)win_socket_setopt(socket,
                            SOL_SOCKET,
                            SO_RCVBUF,
                            &kDefaultUdpBuffer,
                            static_cast<int>(sizeof(kDefaultUdpBuffer)));
    (void)win_socket_setopt(socket,
                            SOL_SOCKET,
                            SO_SNDBUF,
                            &kDefaultUdpBuffer,
                            static_cast<int>(sizeof(kDefaultUdpBuffer)));

    if (family_ == Family::IPv6) {
      const int v6only = options_.ipv6_only ? 1 : 0;
      if (win_socket_setopt(socket, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) != 0) {
        const Error error = from_socket_error(WSAGetLastError());
        (void)::closesocket(socket);
        return std::unexpected(error);
      }
    }

    socket_ = socket;
    loop_->begin_work();
    work_ref_ = true;
    return {};
  }

  bool update_interest() {
    if (socket_ == INVALID_SOCKET || closing_) {
      return false;
    }

    std::uint32_t events = 0;
    if (receiving_) {
      events |= EPOLLIN;
    }
    if (!sends_.empty()) {
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

    // 掩码未变化就不重复 epoll_ctl
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

  VoidResult queue_send(const sockaddr_storage* destination,
                        int destination_length,
                        std::span<const std::byte> data,
                        WriteCallback callback) {
    ReadyUdpSend* item = acquire_send_item();
    item->owned.assign(data.begin(), data.end());
    item->data = item->owned.data();
    item->size = item->owned.size();
    item->keepalive.reset();
    item->callback = std::move(callback);
    if (destination != nullptr) {
      item->destination = *destination;
      item->destination_length = destination_length;
      item->has_destination = true;
    } else {
      item->has_destination = false;
      item->destination_length = 0;
    }
    sends_.push_back(item);
    flush_sends();
    return {};
  }

  VoidResult queue_send_buffer(const sockaddr_storage* destination,
                               int destination_length,
                               WriteBuffer buffer,
                               WriteCallback callback) {
    ReadyUdpSend* item = acquire_send_item();
    item->owned.clear();
    item->data = buffer.data.data();
    item->size = buffer.data.size();
    item->keepalive = std::move(buffer.keepalive);
    item->callback = std::move(callback);
    if (destination != nullptr) {
      item->destination = *destination;
      item->destination_length = destination_length;
      item->has_destination = true;
    } else {
      item->has_destination = false;
      item->destination_length = 0;
    }
    sends_.push_back(item);
    flush_sends();
    return {};
  }

  void flush_sends() {
    if (flushing_ || closing_ || socket_ == INVALID_SOCKET) {
      return;
    }
    flushing_ = true;
    auto self = shared_from_this();

    while (!sends_.empty() && !closing_) {
      ReadyUdpSend* item = sends_.front();

      int result = 0;
      if (item->has_destination) {
        result = ::sendto(socket_,
                          reinterpret_cast<const char*>(item->data),
                          static_cast<int>(item->size),
                          0,
                          reinterpret_cast<const sockaddr*>(&item->destination),
                          item->destination_length);
      } else {
        result = ::send(socket_,
                        reinterpret_cast<const char*>(item->data),
                        static_cast<int>(item->size),
                        0);
      }

      if (result == static_cast<int>(item->size)) {
        auto callback = std::move(item->callback);
        item->callback = nullptr;
        sends_.pop_front();
        recycle_send_item(item);
        if (callback) {
          callback({});
          self = shared_from_this();
          if (closing_) {
            break;
          }
        }
        continue;
      }

      if (result < 0) {
        const int error = WSAGetLastError();
        if (error == WSAEWOULDBLOCK) {
          break;
        }
        auto callback = std::move(item->callback);
        item->callback = nullptr;
        sends_.pop_front();
        if (callback) {
          callback(std::unexpected(from_socket_error(error)));
        }
        recycle_send_item(item);
        continue;
      }

      // UDP 不应出现部分发送；保险起见按错误处理
      auto callback = std::move(item->callback);
      item->callback = nullptr;
      sends_.pop_front();
      if (callback) {
        callback(std::unexpected(make_error(ErrorCode::Unknown)));
      }
      recycle_send_item(item);
    }

    flushing_ = false;
    (void)update_interest();
  }

  void fail_sends(Error error) {
    std::deque<ReadyUdpSend*> pending;
    pending.swap(sends_);
    for (auto* item : pending) {
      auto callback = std::move(item->callback);
      item->callback = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
      recycle_send_item(item);
    }
  }

  void read_once() {
    if (!receiving_ || closing_ || socket_ == INVALID_SOCKET) {
      return;
    }

    auto self = shared_from_this();

    // 一次可读事件里连续收到 WSAEWOULDBLOCK 为止（上限 recv_batch_）。
    // 每个包一次 epoll_wait 会把内核等待摊到每个 datagram 上，吞吐明显受限；
    // 批量后等待次数约降到 1/batch。空闲时多出的那次 recvfrom 在下一包到达之前，
    // 不在往返关键路径上，pingpong 延迟不受影响。
    for (std::size_t i = 0; i < recv_batch_ && receiving_ && !closing_; ++i) {
      std::span<std::byte> target;
      std::shared_ptr<const void> keepalive;
      if (keepalive_recv_) {
        BufferPool::Handle block = recv_pool_->acquire();
        target = block.data;
        keepalive = std::move(block.keepalive);
      } else {
        target = std::span<std::byte>(view_buffer_.data(), view_buffer_.size());
      }

      sockaddr_storage source{};
      int source_length = static_cast<int>(sizeof(source));
      const int n = ::recvfrom(socket_,
                               reinterpret_cast<char*>(target.data()),
                               static_cast<int>(target.size()),
                               0,
                               reinterpret_cast<sockaddr*>(&source),
                               &source_length);
      if (n < 0) {
        const int error = WSAGetLastError();
        if (error == WSAEWOULDBLOCK) {
          return;
        }
        receiving_ = false;
        deliver_recv_error(from_socket_error(error));
        (void)update_interest();
        return;
      }

      const SocketAddress from =
          AddressCodec::from_native(reinterpret_cast<sockaddr*>(&source), source_length);
      if (keepalive_recv_) {
        auto callback = recv_callback_;
        if (callback) {
          callback(Datagram{std::span<const std::byte>(target.data(),
                                                       static_cast<std::size_t>(n)),
                            from,
                            keepalive});
        }
      } else {
        auto callback = view_callback_;
        if (callback) {
          callback(DatagramView{
              std::span<const std::byte>(target.data(), static_cast<std::size_t>(n)), from});
        }
      }
    }
  }

  void deliver_recv_error(Error error) {
    if (keepalive_recv_) {
      auto callback = std::move(recv_callback_);
      recv_callback_ = nullptr;
      view_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    } else {
      auto callback = std::move(view_callback_);
      view_callback_ = nullptr;
      recv_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    }
  }

  ReadyUdpSend* acquire_send_item() {
    if (!free_sends_.empty()) {
      auto* item = free_sends_.back();
      free_sends_.pop_back();
      return item;
    }
    return new ReadyUdpSend{};
  }

  void recycle_send_item(ReadyUdpSend* item) {
    item->keepalive.reset();
    item->data = nullptr;
    item->size = 0;
    item->callback = nullptr;
    item->has_destination = false;
    item->destination_length = 0;
    if (free_sends_.size() >= kMaxFreeSends) {
      delete item;
      return;
    }
    free_sends_.push_back(item);
  }

  void close_impl() {
    if (closing_) {
      return;
    }
    closing_ = true;
    receiving_ = false;
    connected_ = false;
    recv_callback_ = nullptr;
    view_callback_ = nullptr;

    std::deque<ReadyUdpSend*> queued;
    queued.swap(sends_);

    if (socket_ != INVALID_SOCKET) {
      if (attached_) {
        (void)ready_poller(*loop_).detach(socket_);
        attached_ = false;
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
      recycle_send_item(item);
    }

    release_ref();
  }

  void destroy() {
    closing_ = true;
    receiving_ = false;
    connected_ = false;

    if (socket_ != INVALID_SOCKET) {
      if (attached_) {
        (void)ready_poller(*loop_).detach(socket_);
        attached_ = false;
      }
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }

    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    for (auto* item : sends_) {
      delete item;
    }
    sends_.clear();
    for (auto* item : free_sends_) {
      delete item;
    }
    free_sends_.clear();
    recv_pool_.reset();
    view_buffer_.clear();
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
  SOCKET socket_ = INVALID_SOCKET;
  bool connected_ = false;
  bool receiving_ = false;
  bool closing_ = false;
  bool attached_ = false;
  std::uint32_t interest_ = 0;
  bool work_ref_ = false;
  bool flushing_ = false;
  SocketAddress connected_peer_;

  UdpRecvCallback recv_callback_;
  UdpRecvViewCallback view_callback_;
  bool keepalive_recv_ = false;
  bool recv_started_ = false;
  std::size_t recv_chunk_size_ = kMaxDatagramSize;
  std::size_t recv_batch_ = 16;
  std::shared_ptr<BufferPool> recv_pool_;
  std::vector<std::byte> view_buffer_;

  std::deque<ReadyUdpSend*> sends_;
  std::vector<ReadyUdpSend*> free_sends_;
};

std::shared_ptr<UdpSocketImpl> create_ready_udp_socket_impl(std::shared_ptr<LoopImpl> loop,
                                                            Family family,
                                                            const UdpOptions& options) {
  return std::make_shared<ReadyUdpSocketImpl>(std::move(loop), family, options);
}

} // namespace detail
} // namespace uvnet
