#include "uvnet/udp.hpp"

#include <array>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <utility>
#include <vector>

#include "detail/buffer_pool.hpp"
#include "detail/platform.hpp"
#include "detail/poller_win.hpp"
#include "detail/udp_impl_win.hpp"
#include "detail/win_socket.hpp"

namespace uvnet {
namespace {

constexpr std::size_t kMaxDatagramSize = 65536;

int socket_setopt(SOCKET socket, int level, int name, const void* value, int length) {
  return ::setsockopt(socket, level, name, reinterpret_cast<const char*>(value), length);
}

int socket_getopt(SOCKET socket, int level, int name, void* value, int* length) {
  return ::getsockopt(socket, level, name, reinterpret_cast<char*>(value), length);
}

bool bind_any_udp(SOCKET socket, Family family) {
  sockaddr_storage storage{};
  int length = 0;
  if (family == Family::IPv4) {
    auto* v4 = reinterpret_cast<sockaddr_in*>(&storage);
    v4->sin_family = AF_INET;
    v4->sin_port = 0;
    v4->sin_addr.s_addr = INADDR_ANY;
    length = static_cast<int>(sizeof(sockaddr_in));
  } else {
    auto* v6 = reinterpret_cast<sockaddr_in6*>(&storage);
    v6->sin6_family = AF_INET6;
    v6->sin6_port = 0;
    v6->sin6_addr = in6addr_any;
    length = static_cast<int>(sizeof(sockaddr_in6));
  }
  return ::bind(socket, reinterpret_cast<sockaddr*>(&storage), length) == 0;
}

} // namespace

namespace detail {

class IocpUdpSocketImpl;

// Windows 单 socket 只有一个待完成收包时 pps 很低；
// 这里维护多个常驻收包槽位，完成一个立即补投一个。
inline constexpr std::size_t kDefaultRecvSlots = 4;

struct UdpRecvOp final : WinOperation {
  std::shared_ptr<IocpUdpSocketImpl> owner;
  WSABUF wsabuf{};
  std::shared_ptr<const void> keepalive;
  sockaddr_storage source{};
  int source_length = sizeof(sockaddr_storage);
  std::size_t slot = 0;
  bool in_flight = false;

  void on_complete(LoopImpl& loop, DWORD error) override;
};

struct UdpSendOp final : WinOperation {
  std::shared_ptr<IocpUdpSocketImpl> owner;
  std::vector<std::byte> owned;
  const std::byte* external = nullptr;
  std::size_t external_size = 0;
  WSABUF wsabuf{};
  sockaddr_storage destination{};
  int destination_length = 0;
  bool has_destination = false;
  WriteCallback callback;
  std::shared_ptr<const void> keepalive;

  void on_complete(LoopImpl& loop, DWORD error) override;
};

class IocpUdpSocketImpl final : public UdpSocketImpl,
                                 public std::enable_shared_from_this<IocpUdpSocketImpl> {
 public:
  IocpUdpSocketImpl(std::shared_ptr<LoopImpl> loop, Family family, UdpOptions options)
      : loop_(std::move(loop)), family_(family), options_(options) {}

  ~IocpUdpSocketImpl() { destroy(); }

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
    if (::bind(socket_, reinterpret_cast<sockaddr*>(&storage), length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
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
    if (::connect(socket_, reinterpret_cast<sockaddr*>(&storage), length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    connected_ = true;
    connected_peer_ = remote;
    return {};
  }

  VoidResult disconnect() {
    return std::unexpected(make_error(ErrorCode::OperationNotSupported));
  }

  VoidResult send(std::span<const std::byte> data, WriteCallback callback) {
    if (!connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    auto* operation = acquire_send_op();
    operation->owned.assign(data.begin(), data.end());
    operation->external = nullptr;
    operation->external_size = 0;
    operation->keepalive.reset();
    operation->wsabuf.buf = reinterpret_cast<char*>(operation->owned.data());
    operation->wsabuf.len = static_cast<ULONG>(operation->owned.size());
    operation->callback = std::move(callback);

    sends_.push_back(operation);
    if (outstanding_sends_ < kMaxOutstandingSends) {
      submit_sends();
    }
    return {};
  }

  VoidResult send(WriteBuffer buffer, WriteCallback callback) {
    if (!connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    auto* operation = acquire_send_op();
    operation->external = buffer.data.data();
    operation->external_size = buffer.data.size();
    operation->keepalive = std::move(buffer.keepalive);
    operation->wsabuf.buf =
        const_cast<char*>(reinterpret_cast<const char*>(operation->external));
    operation->wsabuf.len = static_cast<ULONG>(operation->external_size);
    operation->callback = std::move(callback);

    sends_.push_back(operation);
    if (outstanding_sends_ < kMaxOutstandingSends) {
      submit_sends();
    }
    return {};
  }

  VoidResult send_to(const SocketAddress& remote,
                     std::span<const std::byte> data,
                     WriteCallback callback) {
    if (remote.family() != family_ || closing_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    auto* operation = acquire_send_op();
    operation->owned.assign(data.begin(), data.end());
    operation->external = nullptr;
    operation->external_size = 0;
    operation->keepalive.reset();
    operation->wsabuf.buf = reinterpret_cast<char*>(operation->owned.data());
    operation->wsabuf.len = static_cast<ULONG>(operation->owned.size());
    operation->callback = std::move(callback);
    operation->has_destination = true;

    if (!AddressCodec::to_native(remote,
                                 operation->destination,
                                 operation->destination_length)) {
      delete operation;
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    sends_.push_back(operation);
    if (outstanding_sends_ < kMaxOutstandingSends) {
      submit_sends();
    }
    return {};
  }

  VoidResult send_to(const SocketAddress& remote,
                     WriteBuffer buffer,
                     WriteCallback callback) {
    if (remote.family() != family_ || closing_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    auto* operation = acquire_send_op();
    operation->external = buffer.data.data();
    operation->external_size = buffer.data.size();
    operation->keepalive = std::move(buffer.keepalive);
    operation->wsabuf.buf =
        const_cast<char*>(reinterpret_cast<const char*>(operation->external));
    operation->wsabuf.len = static_cast<ULONG>(operation->external_size);
    operation->callback = std::move(callback);
    operation->has_destination = true;

    if (!AddressCodec::to_native(remote,
                                 operation->destination,
                                 operation->destination_length)) {
      delete operation;
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    sends_.push_back(operation);
    if (outstanding_sends_ < kMaxOutstandingSends) {
      submit_sends();
    }
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
    recv_slots_ = options.slots;
    recv_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult start_recv(UdpRecvCallback callback) {
    if (!callback || closing_) {
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
    if (!recv_pool_ready_) {
      ensure_recv_pool();
    }
    submit_recvs();
    return {};
  }

  VoidResult start_recv(UdpRecvViewCallback callback) {
    if (!callback || closing_) {
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
    if (!recv_pool_ready_) {
      ensure_recv_pool();
    }
    for (auto& buffer : span_recv_buffers_) {
      if (buffer.empty()) {
        buffer.resize(recv_chunk_size_);
      }
    }
    submit_recvs();
    return {};
  }

  void stop_recv() {
    receiving_ = false;
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
  }

  VoidResult set_broadcast(bool enable) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    const int value = enable ? 1 : 0;
    if (socket_setopt(socket_, SOL_SOCKET, SO_BROADCAST, &value, sizeof(value)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  VoidResult set_ttl(int ttl) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (socket_setopt(socket_, IPPROTO_IPV6, IPV6_UNICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    } else {
      if (socket_setopt(socket_, IPPROTO_IP, IP_TTL, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
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
      if (socket_setopt(socket_, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    } else {
      if (socket_setopt(socket_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    }
    return {};
  }

  VoidResult set_multicast_loop(bool enable) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    const int value = enable ? 1 : 0;
    if (family_ == Family::IPv6) {
      if (socket_setopt(socket_, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
    } else {
      if (socket_setopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
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
      const unsigned int index = static_cast<unsigned int>(iface.scope_id());
      if (socket_setopt(socket_, IPPROTO_IPV6, IPV6_MULTICAST_IF, &index, sizeof(index)) != 0) {
        return std::unexpected(from_socket_error(WSAGetLastError()));
      }
      return {};
    }

    if (iface.family() != Family::IPv4) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    in_addr address{};
    std::memcpy(&address, address_bytes(iface).data(), 4);
    if (socket_setopt(socket_, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
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
      std::memcpy(&request.ipv6mr_multiaddr, address_bytes(group).data(), 16);
      request.ipv6mr_interface = iface.scope_id();
      const int option = join ? IPV6_JOIN_GROUP : IPV6_LEAVE_GROUP;
      if (socket_setopt(socket_, IPPROTO_IPV6, option, &request, sizeof(request)) != 0) {
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
    if (socket_setopt(socket_, IPPROTO_IP, option, &request, sizeof(request)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  Result<SocketAddress> local_address() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    sockaddr_storage storage{};
    int length = sizeof(storage);
    if (::getsockname(socket_, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage), length);
  }

  Result<int> recv_buffer_size() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    int length = sizeof(value);
    if (socket_getopt(socket_, SOL_SOCKET, SO_RCVBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return value;
  }

  VoidResult set_recv_buffer_size(int bytes) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (socket_setopt(socket_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  Result<int> send_buffer_size() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    int length = sizeof(value);
    if (socket_getopt(socket_, SOL_SOCKET, SO_SNDBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return value;
  }

  VoidResult set_send_buffer_size(int bytes) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (socket_setopt(socket_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  [[nodiscard]] bool is_open() const noexcept { return socket_ != INVALID_SOCKET; }
  [[nodiscard]] bool is_connected() const noexcept { return connected_; }

  void close() { close_socket_impl(); }

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

  void handle_recv(UdpRecvOp* operation, DWORD error) {
    auto self = shared_from_this();
    --pending_ops_;
    operation->in_flight = false;
    operation->owner.reset();

    if (closing_) {
      operation->keepalive.reset();
      finish_close_if_ready();
      return;
    }

    if (error != 0) {
      receiving_ = false;
      operation->keepalive.reset();
      deliver_recv_error(from_socket_error(static_cast<int>(error)));
      finish_close_if_ready();
      return;
    }

    const std::size_t bytes = static_cast<std::size_t>(operation->ov.InternalHigh);
    const SocketAddress source =
        AddressCodec::from_native(reinterpret_cast<sockaddr*>(&operation->source),
                                  operation->source_length);
    const std::span<const std::byte> data(
        reinterpret_cast<const std::byte*>(operation->wsabuf.buf), bytes);

    if (keepalive_reads_) {
      auto callback = recv_callback_;
      if (callback) {
        callback(Datagram{data, source, operation->keepalive});
      }
    } else {
      auto callback = view_callback_;
      if (callback) {
        callback(DatagramView{data, source});
      }
    }

    operation->keepalive.reset();

    if (receiving_ && !closing_) {
      submit_one(operation);
    }
  }

  void handle_send(UdpSendOp* operation, DWORD error) {
    --pending_ops_;
    --outstanding_sends_;

    auto callback = std::move(operation->callback);
    operation->callback = nullptr;

    if (closing_) {
      if (callback) {
        callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      delete operation;
      finish_close_if_ready();
      return;
    }

    if (error != 0) {
      const Error mapped = from_socket_error(static_cast<int>(error));
      if (callback) {
        callback(std::unexpected(mapped));
      }
      fail_sends(mapped);
      recycle_send_op(operation);
      return;
    }

    if (callback) {
      callback({});
    }

    if (closing_) {
      delete operation;
      finish_close_if_ready();
      return;
    }

    recycle_send_op(operation);

    if (!sends_.empty() && outstanding_sends_ < kMaxOutstandingSends) {
      submit_sends();
    }
  }

 private:
  friend struct UdpRecvOp;
  friend struct UdpSendOp;

  VoidResult ensure_socket() {
    if (socket_ != INVALID_SOCKET) {
      return {};
    }
    if (options_.reuse_port) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }

    ensure_winsock();
    const SOCKET socket = ::WSASocketW(AddressCodec::native_family(family_),
                                       SOCK_DGRAM,
                                       IPPROTO_UDP,
                                       nullptr,
                                       0,
                                       WSA_FLAG_OVERLAPPED);
    if (socket == INVALID_SOCKET) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }

    if (!win_poller(*loop_).associate(socket)) {
      (void)::closesocket(socket);
      return std::unexpected(make_error(ErrorCode::Unknown));
    }

    const int one = 1;
    if (options_.reuse_address &&
        socket_setopt(socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
      const Error error = from_socket_error(WSAGetLastError());
      (void)::closesocket(socket);
      return std::unexpected(error);
    }

    // 默认 UDP 收发缓冲 256KB：真实网卡 + 高 PPS 场景显著降低丢包
    // loopback 场景几乎无影响；失败仅 warn，不阻塞 socket 创建
    constexpr int kDefaultUdpBuf = 256 * 1024;
    (void)socket_setopt(socket, SOL_SOCKET, SO_RCVBUF,
                        &kDefaultUdpBuf, sizeof(kDefaultUdpBuf));
    (void)socket_setopt(socket, SOL_SOCKET, SO_SNDBUF,
                        &kDefaultUdpBuf, sizeof(kDefaultUdpBuf));

    if (family_ == Family::IPv6) {
      const int v6only = options_.ipv6_only ? 1 : 0;
      if (socket_setopt(socket, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) != 0) {
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

  void ensure_recv_pool() {
    recv_ops_.assign(recv_slots_, nullptr);
    span_recv_buffers_.assign(recv_slots_, {});
    recv_pool_ = BufferPool::create(recv_chunk_size_, 32);
    for (std::size_t i = 0; i < recv_slots_; ++i) {
      auto* operation = new UdpRecvOp{};
      operation->delete_after_completion = false;
      operation->slot = i;
      recv_ops_[i] = operation;
    }
    recv_pool_ready_ = true;
  }

  void submit_recvs() {
    for (auto* operation : recv_ops_) {
      if (operation != nullptr) {
        submit_one(operation);
      }
    }
  }

  void submit_one(UdpRecvOp* operation) {
    if (!receiving_ || socket_ == INVALID_SOCKET || closing_ ||
        operation->in_flight) {
      return;
    }

    auto self = shared_from_this();
    BufferPool::Handle block;
    const std::byte* data = nullptr;
    std::size_t capacity = 0;

    if (keepalive_reads_) {
      block = recv_pool_->acquire();
      data = block.data.data();
      capacity = block.data.size();
    } else {
      auto& buffer = span_recv_buffers_[operation->slot];
      if (buffer.empty()) {
        buffer.resize(recv_chunk_size_);
      }
      data = buffer.data();
      capacity = buffer.size();
    }

    std::memset(&operation->source, 0, sizeof(operation->source));
    operation->source_length = sizeof(sockaddr_storage);
    operation->wsabuf.buf = const_cast<char*>(reinterpret_cast<const char*>(data));
    operation->wsabuf.len = static_cast<ULONG>(capacity);
    operation->keepalive = std::move(block.keepalive);
    operation->file = reinterpret_cast<HANDLE>(socket_);
    operation->owner = shared_from_this();
    operation->completed_synchronically = false;
    operation->in_flight = true;
    ++pending_ops_;

    DWORD bytes = 0;
    DWORD flags = 0;
    const int result = ::WSARecvFrom(socket_,
                                     &operation->wsabuf,
                                     1,
                                     &bytes,
                                     &flags,
                                     reinterpret_cast<sockaddr*>(&operation->source),
                                     &operation->source_length,
                                     &operation->ov,
                                     nullptr);

    if (result != 0) {
      const int error = WSAGetLastError();
      if (error != WSA_IO_PENDING) {
        operation->in_flight = false;
        operation->owner.reset();
        operation->keepalive.reset();
        --pending_ops_;
        receiving_ = false;
        auto callback = std::move(recv_callback_);
        recv_callback_ = nullptr;
        if (callback) {
          callback(std::unexpected(from_socket_error(error)));
        }
        finish_close_if_ready();
      }
    } else {
      // 同步成功时 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 不会投递完成包
      // 通过 post 走完成路径，避免递归 submit→drain→submit 爆栈
      operation->ov.InternalHigh = bytes;
      operation->completed_synchronically = true;
      auto loop = loop_;
      loop_->post([operation, loop] { operation->on_complete(*loop, 0); });
    }
  }

  UdpSendOp* acquire_send_op() {
    if (!free_sends_.empty()) {
      auto* operation = free_sends_.back();
      free_sends_.pop_back();
      return operation;
    }
    auto* operation = new UdpSendOp{};
    operation->delete_after_completion = false;
    return operation;
  }

  void recycle_send_op(UdpSendOp* operation) {
    operation->external = nullptr;
    operation->external_size = 0;
    operation->keepalive.reset();
    operation->destination_length = 0;
    operation->has_destination = false;
    if (free_sends_.size() >= 8) {
      delete operation;
      return;
    }
    free_sends_.push_back(operation);
  }

  // 提交 UDP 发送队列中的 op，最多到 kMaxOutstandingSends 并发
  void submit_sends() {
    while (!sends_.empty() && socket_ != INVALID_SOCKET && !closing_ &&
           outstanding_sends_ < kMaxOutstandingSends) {
      auto* operation = sends_.front();
      sends_.pop_front();
      operation->owner = shared_from_this();
      operation->file = reinterpret_cast<HANDLE>(socket_);
      operation->completed_synchronically = false;
      ++pending_ops_;
      ++outstanding_sends_;

      DWORD bytes = 0;
      const int result =
          operation->has_destination
              ? ::WSASendTo(socket_,
                            &operation->wsabuf,
                            1,
                            &bytes,
                            0,
                            reinterpret_cast<sockaddr*>(&operation->destination),
                            operation->destination_length,
                            &operation->ov,
                            nullptr)
              : ::WSASend(socket_, &operation->wsabuf, 1, &bytes, 0, &operation->ov, nullptr);

      if (result != 0) {
        const int error = WSAGetLastError();
        if (error != WSA_IO_PENDING) {
          --outstanding_sends_;
          --pending_ops_;
          auto callback = std::move(operation->callback);
          operation->callback = nullptr;
          const Error mapped = from_socket_error(error);
          if (callback) {
            callback(std::unexpected(mapped));
          }
          fail_sends(mapped);
          recycle_send_op(operation);
          finish_close_if_ready();
        }
      } else {
        // 同步成功时 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 不会投递完成包
        // 通过 post 走完成路径，避免递归 submit→drain→submit 爆栈
        operation->ov.InternalHigh = bytes;
        operation->completed_synchronically = true;
        auto loop = loop_;
        loop_->post([operation, loop] { operation->on_complete(*loop, 0); });
      }
    }
  }

  void fail_sends(Error error) {
    std::deque<UdpSendOp*> pending;
    pending.swap(sends_);
    for (auto* operation : pending) {
      auto callback = std::move(operation->callback);
      operation->callback = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
      delete operation;
    }
  }

  void close_socket_impl() {
    if (closing_) {
      return;
    }
    closing_ = true;
    receiving_ = false;
    connected_ = false;
    recv_callback_ = nullptr;
    view_callback_ = nullptr;

    std::deque<UdpSendOp*> queued;
    queued.swap(sends_);

    for (auto* operation : queued) {
      auto callback = std::move(operation->callback);
      operation->callback = nullptr;
      if (callback) {
        callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      delete operation;
    }

    if (socket_ != INVALID_SOCKET) {
      (void)::CancelIoEx(reinterpret_cast<HANDLE>(socket_), nullptr);
      if (pending_ops_ == 0) {
        finish_close_if_ready();
      }
    } else {
      release_ref();
    }
  }

  void finish_close_if_ready() {
    if (!closing_ || pending_ops_ > 0) {
      return;
    }
    if (socket_ != INVALID_SOCKET) {
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }
    release_ref();
  }

  void destroy() {
    closing_ = true;
    receiving_ = false;
    connected_ = false;
    for (auto* operation : sends_) {
      delete operation;
    }
    sends_.clear();
    if (socket_ != INVALID_SOCKET) {
      (void)::CancelIoEx(reinterpret_cast<HANDLE>(socket_), nullptr);
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    for (auto* operation : recv_ops_) {
      if (operation != nullptr) {
        operation->keepalive.reset();
      }
      delete operation;
    }
    recv_ops_.clear();
    span_recv_buffers_.clear();
    recv_pool_.reset();
    for (auto* operation : free_sends_) {
      delete operation;
    }
    free_sends_.clear();
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
  bool work_ref_ = false;
  int pending_ops_ = 0;
  SocketAddress connected_peer_;
  UdpRecvCallback recv_callback_;
  UdpRecvViewCallback view_callback_;
  bool keepalive_reads_ = false;
  bool recv_pool_ready_ = false;
  std::size_t recv_slots_ = kDefaultRecvSlots;
  std::size_t recv_chunk_size_ = kMaxDatagramSize;
  bool recv_started_ = false;
  std::shared_ptr<BufferPool> recv_pool_;
  std::vector<UdpRecvOp*> recv_ops_;
  std::vector<std::vector<std::byte>> span_recv_buffers_;
  static constexpr int kMaxOutstandingSends = 4;
  int outstanding_sends_ = 0;
  std::deque<UdpSendOp*> sends_;
  std::vector<UdpSendOp*> free_sends_;
};

void UdpRecvOp::on_complete(LoopImpl&, DWORD error) {
  owner->handle_recv(this, error);
}

void UdpSendOp::on_complete(LoopImpl&, DWORD error) {
  owner->handle_send(this, error);
}

std::shared_ptr<UdpSocketImpl> create_iocp_udp_socket_impl(std::shared_ptr<LoopImpl> loop,
                                                          Family family,
                                                          const UdpOptions& options) {
  return std::make_shared<IocpUdpSocketImpl>(std::move(loop), family, options);
}

std::shared_ptr<UdpSocketImpl> create_udp_socket_impl(std::shared_ptr<LoopImpl> loop,
                                                      Family family,
                                                      const UdpOptions& options) {
  const Backend backend = loop->backend();
  if (backend == Backend::Readiness || backend == Backend::Select) {
    return create_ready_udp_socket_impl(std::move(loop), family, options);
  }
  return create_iocp_udp_socket_impl(std::move(loop), family, options);
}

} // namespace detail

UdpSocket::UdpSocket(Loop& loop, Family family, const UdpOptions& options) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = detail::create_udp_socket_impl(impl->shared_from_this(), family, options);
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
