#include "uvnet/tcp.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#include "detail/buffer_pool.hpp"
#include "detail/platform.hpp"
#include "detail/poller_win.hpp"
#include "detail/tcp_impl_win.hpp"
#include "detail/win_socket.hpp"

#include <mswsock.h>

namespace uvnet {
namespace {

constexpr std::size_t kReadBufferSize = 64 * 1024;
constexpr std::size_t kDefaultReadSlots = 4;
inline constexpr std::size_t kMaxFreeWrites = 8;
constexpr int kAcceptAddressLength = static_cast<int>(sizeof(SOCKADDR_STORAGE) + 16);

std::once_flag g_extension_once;
LPFN_ACCEPTEX g_acceptex = nullptr;
LPFN_CONNECTEX g_connectex = nullptr;

void load_extensions() {
  std::call_once(g_extension_once, [] {
    detail::ensure_winsock();
    SOCKET probe =
        ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (probe == INVALID_SOCKET) {
      return;
    }

    DWORD bytes = 0;
    GUID accept_guid = WSAID_ACCEPTEX;
    (void)::WSAIoctl(probe,
                     SIO_GET_EXTENSION_FUNCTION_POINTER,
                     &accept_guid,
                     static_cast<DWORD>(sizeof(accept_guid)),
                     &g_acceptex,
                     static_cast<DWORD>(sizeof(g_acceptex)),
                     &bytes,
                     nullptr,
                     nullptr);

    GUID connect_guid = WSAID_CONNECTEX;
    (void)::WSAIoctl(probe,
                     SIO_GET_EXTENSION_FUNCTION_POINTER,
                     &connect_guid,
                     static_cast<DWORD>(sizeof(connect_guid)),
                     &g_connectex,
                     static_cast<DWORD>(sizeof(g_connectex)),
                     &bytes,
                     nullptr,
                     nullptr);

    (void)::closesocket(probe);
  });
}

SOCKET create_tcp_socket(Family family) {
  detail::ensure_winsock();
  return ::WSASocketW(detail::AddressCodec::native_family(family),
                      SOCK_STREAM,
                      IPPROTO_TCP,
                      nullptr,
                      0,
                      WSA_FLAG_OVERLAPPED);
}

bool bind_wildcard(SOCKET socket, Family family) {
  sockaddr_storage storage{};
  int length = 0;

  if (family == Family::IPv4) {
    auto* v4 = reinterpret_cast<sockaddr_in*>(&storage);
    v4->sin_family = AF_INET;
    v4->sin_port = 0;
    v4->sin_addr.s_addr = INADDR_ANY;
    length = static_cast<int>(sizeof(sockaddr_in));
  } else if (family == Family::IPv6) {
    auto* v6 = reinterpret_cast<sockaddr_in6*>(&storage);
    v6->sin6_family = AF_INET6;
    v6->sin6_port = 0;
    v6->sin6_addr = in6addr_any;
    length = static_cast<int>(sizeof(sockaddr_in6));
  } else {
    return false;
  }

  return ::bind(socket, reinterpret_cast<sockaddr*>(&storage), length) == 0;
}

} // namespace

namespace detail {

class IocpTcpStreamImpl;
class IocpTcpListenerImpl;

struct StreamReadOp final : WinOperation {
  std::shared_ptr<IocpTcpStreamImpl> owner;
  WSABUF wsabuf{};
  std::shared_ptr<const void> keepalive;
  std::size_t slot = 0;
  std::uint64_t sequence = 0;
  bool in_flight = false;
  // 已完成但尚未按序交付，此时槽位缓冲不能复用
  bool completed_pending = false;

  void on_complete(LoopImpl& loop, DWORD error) override;
};

struct StreamWriteOp final : WinOperation {
  std::shared_ptr<IocpTcpStreamImpl> owner;
  std::vector<std::byte> owned;
  const std::byte* external = nullptr;
  std::size_t external_size = 0;
  WSABUF wsabuf{};
  WriteCallback callback;
  std::shared_ptr<const void> keepalive;

  void on_complete(LoopImpl& loop, DWORD error) override;
};

struct ReadCompletion {
  StreamReadOp* operation = nullptr;
  std::size_t bytes = 0;
  int error = 0;
  bool eof = false;
  bool valid = false;
};

struct StreamConnectOp final : WinOperation {
  std::shared_ptr<IocpTcpStreamImpl> owner;

  void on_complete(LoopImpl& loop, DWORD error) override;
};

class IocpTcpStreamImpl final : public TcpStreamImpl,
                                public std::enable_shared_from_this<IocpTcpStreamImpl> {
 public:
  explicit IocpTcpStreamImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}

  ~IocpTcpStreamImpl() { destroy(); }

  VoidResult bind(const SocketAddress& local, const BindOptions& options) {
    if (socket_ != INVALID_SOCKET && (connecting_ || connected_)) {
      return std::unexpected(make_error(ErrorCode::AlreadyConnected));
    }
    if (!ensure_socket(local.family())) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
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

  VoidResult connect(const SocketAddress& remote, ConnectCallback callback) {
    if (socket_ != INVALID_SOCKET && connecting_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (socket_ != INVALID_SOCKET && connected_) {
      return std::unexpected(make_error(ErrorCode::AlreadyConnected));
    }
    if (remote.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    load_extensions();
    if (g_connectex == nullptr) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }

    if (!ensure_socket(remote.family())) {
      return std::unexpected(from_socket_error(last_socket_error()));
    }

    if (!bound_ && !bind_wildcard(socket_, remote.family())) {
      const Error error = from_socket_error(WSAGetLastError());
      close_socket_impl(true);
      return std::unexpected(error);
    }
    bound_ = true;

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(remote, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    auto* operation = new StreamConnectOp{};
    operation->owner = shared_from_this();
    operation->file = reinterpret_cast<HANDLE>(socket_);
    connect_operation_ = operation;
    connect_callback_ = std::move(callback);
    connecting_ = true;
    ++pending_ops_;

    DWORD bytes = 0;
    const BOOL ok = g_connectex(socket_,
                                reinterpret_cast<sockaddr*>(&storage),
                                length,
                                nullptr,
                                0,
                                nullptr,
                                &operation->ov);

    if (ok == FALSE) {
      const int error = WSAGetLastError();
      if (error != WSA_IO_PENDING) {
        connect_operation_ = nullptr;
        connecting_ = false;
        connect_callback_ = nullptr;
        --pending_ops_;
        delete operation;
        close_socket_impl(true);
        return std::unexpected(from_socket_error(error));
      }
    } else {
      // 同步成功时 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 不会投递完成包
      // 通过 post 走完成路径，避免递归 submit→drain→submit 爆栈
      (void)bytes;
      operation->completed_synchronically = true;
      auto loop = loop_;
      loop_->post([operation, loop] {
        operation->on_complete(*loop, 0);
        delete operation;
      });
    }

    return {};
  }

  VoidResult set_read_options(const ReadOptions& options) {
    if (read_started_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (options.slots == 0 || options.slots > 64 || options.chunk_size < 1024 ||
        options.chunk_size > 1024 * 1024) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    read_slots_ = options.slots;
    read_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult start_read(ReadCallback callback) {
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
    if (!read_pool_ready_) {
      ensure_read_pool();
    }
    submit_reads();
    return {};
  }

  VoidResult start_read(SpanReadCallback callback) {
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
    if (!read_pool_ready_) {
      ensure_read_pool();
    }
    for (auto& buffer : span_read_buffers_) {
      if (buffer.empty()) {
        buffer.resize(kReadBufferSize);
      }
    }
    submit_reads();
    return {};
  }

  void stop_read() {
    reading_ = false;
    read_callback_ = nullptr;
    span_callback_ = nullptr;
  }

  VoidResult write(std::span<const std::byte> data, WriteCallback callback) {
    if (socket_ == INVALID_SOCKET || !connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_issued_) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }
    if (data.empty()) {
      return {};
    }

    auto* operation = acquire_write_op();
    operation->owned.assign(data.begin(), data.end());
    operation->external = nullptr;
    operation->external_size = 0;
    operation->keepalive.reset();
    operation->wsabuf.buf = reinterpret_cast<char*>(operation->owned.data());
    operation->wsabuf.len = static_cast<ULONG>(operation->owned.size());
    operation->callback = std::move(callback);

    writes_.push_back(operation);
    if (current_write_ == nullptr) {
      submit_next_write();
    }
    return {};
  }

  VoidResult write(WriteBuffer buffer, WriteCallback callback) {
    if (socket_ == INVALID_SOCKET || !connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_issued_) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }
    if (buffer.data.empty()) {
      return {};
    }

    auto* operation = acquire_write_op();
    operation->external = buffer.data.data();
    operation->external_size = buffer.data.size();
    operation->keepalive = std::move(buffer.keepalive);
    operation->wsabuf.buf =
        const_cast<char*>(reinterpret_cast<const char*>(operation->external));
    operation->wsabuf.len = static_cast<ULONG>(operation->external_size);
    operation->callback = std::move(callback);

    writes_.push_back(operation);
    if (current_write_ == nullptr) {
      submit_next_write();
    }
    return {};
  }

  VoidResult shutdown(WriteCallback callback) {
    if (socket_ == INVALID_SOCKET || !connected_ || closing_) {
      return std::unexpected(make_error(ErrorCode::NotConnected));
    }
    if (shutdown_pending_ || shutdown_issued_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }

    shutdown_callback_ = std::move(callback);
    shutdown_pending_ = true;
    if (current_write_ == nullptr && writes_.empty()) {
      do_shutdown();
    }
    return {};
  }

  void close(bool reset) { close_socket_impl(reset); }

  [[nodiscard]] bool is_open() const noexcept { return socket_ != INVALID_SOCKET; }
  [[nodiscard]] bool is_connected() const noexcept { return connected_; }

  Result<SocketAddress> local_address() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_local_address(socket_);
  }

  Result<SocketAddress> peer_address() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_peer_address(socket_);
  }

  VoidResult set_nodelay(bool enable) {
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

  VoidResult set_keepalive(const KeepAliveOptions& options) {
    keepalive_ = options;
    if (socket_ == INVALID_SOCKET) {
      return {};
    }
    return win_apply_keepalive(socket_, options);
  }

  Result<int> recv_buffer_size() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    int length = sizeof(value);
    if (win_socket_getopt(socket_, SOL_SOCKET, SO_RCVBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return value;
  }

  VoidResult set_recv_buffer_size(int bytes) {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (win_socket_setopt(socket_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
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
    if (win_socket_getopt(socket_, SOL_SOCKET, SO_SNDBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return value;
  }

  VoidResult set_send_buffer_size(int bytes) {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (win_socket_setopt(socket_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    return {};
  }

  void adopt_socket(SOCKET socket, Family family) {
    socket_ = socket;
    family_ = family;
    connected_ = true;
    bound_ = true;
    loop_->begin_work();
    work_ref_ = true;

    if (nodelay_) {
      const int value = 1;
      (void)win_socket_setopt(socket_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
    }
    if (keepalive_.enable) {
      (void)win_apply_keepalive(socket_, keepalive_);
    }
  }

  void handle_read(StreamReadOp* operation, DWORD error) {
    auto self = shared_from_this();
    --pending_ops_;
    operation->in_flight = false;
    operation->owner.reset();

    if (closing_) {
      operation->keepalive.reset();
      finish_close_if_ready();
      return;
    }

    const std::size_t bytes = static_cast<std::size_t>(operation->ov.InternalHigh);
    const int completion_error = static_cast<int>(error);
    const bool completion_eof = error == 0 && bytes == 0;

    // 常见情况：完成顺序与提交顺序一致，直接交付，完全跳过环形槽的记账；
    // 只有真正乱序的完成才落环，等前序补洞后由 drain_reads 交付
    if (operation->sequence == next_deliver_seq_) {
      ++next_deliver_seq_;
      deliver_read(operation, bytes, completion_error, completion_eof);
      if (ring_pending_ != 0) {
        drain_reads();
      }
      return;
    }

    operation->completed_pending = true;
    read_completions_[static_cast<std::size_t>(operation->sequence % read_slots_)] =
        ReadCompletion{operation, bytes, completion_error, completion_eof, true};
    ++ring_pending_;
    drain_reads();
  }

  void handle_write(StreamWriteOp* operation, DWORD error) {
    --pending_ops_;
    current_write_ = nullptr;

    auto callback = std::move(operation->callback);
    operation->callback = nullptr;
    operation->owner.reset();

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
      fail_writes(mapped);
      operation->keepalive.reset();
      operation->external = nullptr;
      operation->external_size = 0;
      recycle_write_op(operation);
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

    operation->keepalive.reset();
    operation->external = nullptr;
    operation->external_size = 0;
    recycle_write_op(operation);

    if (!writes_.empty() && current_write_ == nullptr) {
      submit_next_write();
    } else if (shutdown_pending_ && !shutdown_issued_) {
      do_shutdown();
    }
  }

  void handle_connect(DWORD error) {
    --pending_ops_;
    connect_operation_ = nullptr;
    connecting_ = false;

    auto callback = std::move(connect_callback_);
    connect_callback_ = nullptr;

    if (closing_) {
      if (callback) {
        callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      finish_close_if_ready();
      return;
    }

    if (error == 0) {
      connected_ = true;
      (void)win_socket_setopt(socket_, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);
      if (callback) {
        callback({});
      }
      return;
    }

    if (callback) {
      callback(std::unexpected(from_socket_error(static_cast<int>(error))));
    }
  }

 private:
  friend struct StreamReadOp;
  friend struct StreamWriteOp;
  friend struct StreamConnectOp;

  bool ensure_socket(Family family) {
    if (socket_ != INVALID_SOCKET) {
      return family_ == family;
    }

    load_extensions();

    const SOCKET socket = create_tcp_socket(family);
    if (socket == INVALID_SOCKET) {
      return false;
    }
    if (!win_poller(*loop_).associate(socket)) {
      (void)::closesocket(socket);
      return false;
    }

    socket_ = socket;
    family_ = family;
    loop_->begin_work();
    work_ref_ = true;

    // 默认 TCP 收发缓冲 256KB：适配高带宽延迟积场景；失败仅 warn
    constexpr int kDefaultTcpBuf = 256 * 1024;
    (void)win_socket_setopt(socket_, SOL_SOCKET, SO_RCVBUF,
                        &kDefaultTcpBuf, sizeof(kDefaultTcpBuf));
    (void)win_socket_setopt(socket_, SOL_SOCKET, SO_SNDBUF,
                        &kDefaultTcpBuf, sizeof(kDefaultTcpBuf));

    if (nodelay_) {
      const int value = 1;
      (void)win_socket_setopt(socket_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
    }
    if (keepalive_.enable) {
      (void)win_apply_keepalive(socket_, keepalive_);
    }
    return true;
  }

  void ensure_read_pool() {
    read_ops_.assign(read_slots_, nullptr);
    read_completions_.assign(read_slots_, ReadCompletion{});
    span_read_buffers_.assign(read_slots_, {});
    read_pool_ = BufferPool::create(read_chunk_size_, 32);
    for (std::size_t i = 0; i < read_slots_; ++i) {
      auto* operation = new StreamReadOp{};
      operation->delete_after_completion = false;
      operation->slot = i;
      read_ops_[i] = operation;
    }
    read_pool_ready_ = true;
  }

  void submit_reads() {
    if (!reading_ || socket_ == INVALID_SOCKET || closing_) {
      return;
    }
    for (auto* operation : read_ops_) {
      if (operation != nullptr) {
        submit_one_read(operation);
      }
    }
  }

  void submit_one_read(StreamReadOp* operation) {
    if (!reading_ || socket_ == INVALID_SOCKET || closing_ ||
        operation->in_flight || operation->completed_pending) {
      return;
    }

    auto self = shared_from_this();
    BufferPool::Handle block;
    const std::byte* data = nullptr;
    std::size_t capacity = 0;

    if (keepalive_reads_) {
      block = read_pool_->acquire();
      data = block.data.data();
      capacity = block.data.size();
    } else {
      auto& buffer = span_read_buffers_[operation->slot];
      if (buffer.empty()) {
        buffer.resize(read_chunk_size_);
      }
      data = buffer.data();
      capacity = buffer.size();
    }

    operation->sequence = next_submit_seq_++;
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
    const int result = ::WSARecv(
        socket_, &operation->wsabuf, 1, &bytes, &flags, &operation->ov, nullptr);

    if (result != 0) {
      const int error = WSAGetLastError();
      if (error != WSA_IO_PENDING) {
        operation->in_flight = false;
        operation->owner.reset();
        operation->keepalive.reset();
        --pending_ops_;
        reading_ = false;
        auto callback = std::move(read_callback_);
        read_callback_ = nullptr;
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

  enum class ReadDelivery { resubmitted, consumed, stopped };

  // 交付一个已完成读：回调用户并在读仍继续时重新提交该槽
  ReadDelivery deliver_read(StreamReadOp* operation, std::size_t bytes, int error, bool eof) {
    operation->completed_pending = false;
    auto keepalive = std::move(operation->keepalive);

    if (!reading_) {
      // 读已停止：只消费完成，保持交付游标继续前进
      if (error == 0 && eof) {
        eof_ = true;
      }
      return ReadDelivery::consumed;
    }

    if (error != 0) {
      reading_ = false;
      keepalive.reset();
      deliver_read_error(from_socket_error(error));
      return ReadDelivery::stopped;
    }

    if (eof) {
      eof_ = true;
      reading_ = false;
      keepalive.reset();
      deliver_read_error(make_error(ErrorCode::EndOfFile));
      return ReadDelivery::stopped;
    }

    if (!keepalive_reads_) {
      auto span_callback = span_callback_;
      if (span_callback) {
        span_callback(std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(operation->wsabuf.buf), bytes));
      }
    } else {
      auto callback = read_callback_;
      if (callback) {
        callback(ReadChunk{
            std::span<const std::byte>(
                reinterpret_cast<const std::byte*>(operation->wsabuf.buf), bytes),
            keepalive});
      }
    }

    // 回调未保留时释放本地引用，使同一块可立即被下一次读复用
    keepalive.reset();

    if (closing_ || !reading_) {
      return ReadDelivery::consumed;
    }
    submit_one_read(operation);
    return ReadDelivery::resubmitted;
  }

  // 补洞：按提交序号从环形槽里取出已到达的乱序完成并交付
  void drain_reads() {
    while (!closing_ && ring_pending_ != 0) {
      const std::size_t index = static_cast<std::size_t>(next_deliver_seq_ % read_slots_);
      ReadCompletion& entry = read_completions_[index];
      if (!entry.valid || entry.operation == nullptr ||
          entry.operation->sequence != next_deliver_seq_) {
        break;
      }

      ReadCompletion completion = entry;
      entry = ReadCompletion{};
      --ring_pending_;
      ++next_deliver_seq_;

      if (deliver_read(completion.operation, completion.bytes, completion.error, completion.eof) ==
          ReadDelivery::stopped) {
        return;
      }
    }
  }

  StreamWriteOp* acquire_write_op() {
    if (!free_writes_.empty()) {
      auto* operation = free_writes_.back();
      free_writes_.pop_back();
      return operation;
    }
    auto* operation = new StreamWriteOp{};
    operation->delete_after_completion = false;
    return operation;
  }

  void recycle_write_op(StreamWriteOp* operation) {
    operation->keepalive.reset();
    operation->external = nullptr;
    operation->external_size = 0;
    if (free_writes_.size() >= kMaxFreeWrites) {
      delete operation;
      return;
    }
    free_writes_.push_back(operation);
  }

  void submit_next_write() {
    if (writes_.empty() || socket_ == INVALID_SOCKET || closing_ ||
        current_write_ != nullptr) {
      return;
    }

    auto* operation = writes_.front();
    writes_.pop_front();
    current_write_ = operation;
    operation->owner = shared_from_this();
    operation->file = reinterpret_cast<HANDLE>(socket_);
    operation->completed_synchronically = false;
    ++pending_ops_;

    DWORD bytes = 0;
    const int result = ::WSASend(
        socket_, &operation->wsabuf, 1, &bytes, 0, &operation->ov, nullptr);

    if (result != 0) {
      const int error = WSAGetLastError();
      if (error != WSA_IO_PENDING) {
        current_write_ = nullptr;
        --pending_ops_;
        auto callback = std::move(operation->callback);
        operation->callback = nullptr;
        const Error mapped = from_socket_error(error);
        if (callback) {
          callback(std::unexpected(mapped));
        }
        fail_writes(mapped);
        recycle_write_op(operation);
        finish_close_if_ready();
      }
    } else {
      // 同步成功时 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 不会再投递完成包。
      // 直接走完成路径可省掉一次 post + 一轮 loop 迭代（pingpong 延迟主要来自这里）；
      // 但只允许一层直调：写回调里再 write 时，第二次同步完成改为 post，
      // 避免 write -> 回调 -> write 的递归爆栈。
      operation->ov.InternalHigh = bytes;
      operation->completed_synchronically = true;

      if (sync_write_depth_ == 0) {
        // 直调期间会释放 operation->owner，这里自己再持一次，防止实现对象被回收
        auto keepalive = shared_from_this();
        ++sync_write_depth_;
        handle_write(operation, 0);
        --sync_write_depth_;
        return;
      }

      auto loop = loop_;
      loop_->post([operation, loop] { operation->on_complete(*loop, 0); });
    }
  }

  void do_shutdown() {
    shutdown_pending_ = false;
    shutdown_issued_ = true;
    if (socket_ != INVALID_SOCKET) {
      (void)::shutdown(socket_, SD_SEND);
    }
    auto callback = std::move(shutdown_callback_);
    shutdown_callback_ = nullptr;
    if (callback) {
      callback({});
    }
  }

  void fail_writes(Error error) {
    std::deque<StreamWriteOp*> pending;
    pending.swap(writes_);
    for (auto* operation : pending) {
      auto callback = std::move(operation->callback);
      operation->callback = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
      delete operation;
    }
  }

  void close_socket_impl(bool reset) {
    if (closing_) {
      return;
    }
    closing_ = true;
    reading_ = false;
    connected_ = false;

    if (reset && socket_ != INVALID_SOCKET) {
      linger option{};
      option.l_onoff = 1;
      option.l_linger = 0;
      (void)win_socket_setopt(socket_, SOL_SOCKET, SO_LINGER, &option, sizeof(option));
    }

    read_callback_ = nullptr;
    span_callback_ = nullptr;
    for (auto& entry : read_completions_) {
      entry = ReadCompletion{};
    }
    for (auto* operation : read_ops_) {
      if (operation != nullptr && !operation->in_flight) {
        operation->keepalive.reset();
      }
    }

    std::deque<StreamWriteOp*> queued;
    queued.swap(writes_);

    auto callback = std::move(connect_callback_);
    connect_callback_ = nullptr;
    auto shutdown_callback = std::move(shutdown_callback_);
    shutdown_callback_ = nullptr;

    for (auto* operation : queued) {
      auto write_callback = std::move(operation->callback);
      operation->callback = nullptr;
      if (write_callback) {
        write_callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
      delete operation;
    }

    if (callback) {
      callback(std::unexpected(make_error(ErrorCode::Canceled)));
    }
    if (shutdown_callback) {
      shutdown_callback(std::unexpected(make_error(ErrorCode::Canceled)));
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
    connected_ = false;
    reading_ = false;
    for (auto& entry : read_completions_) {
      entry = ReadCompletion{};
    }
    for (auto* operation : writes_) {
      delete operation;
    }
    writes_.clear();
    for (auto* operation : read_ops_) {
      if (operation != nullptr) {
        operation->keepalive.reset();
      }
      delete operation;
    }
    read_ops_.clear();
    read_completions_.clear();
    span_read_buffers_.clear();
    read_pool_.reset();
    for (auto* operation : free_writes_) {
      delete operation;
    }
    free_writes_.clear();
    if (socket_ != INVALID_SOCKET) {
      (void)::CancelIoEx(reinterpret_cast<HANDLE>(socket_), nullptr);
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
    }
    read_callback_ = nullptr;
    span_callback_ = nullptr;
    connect_callback_ = nullptr;
    shutdown_callback_ = nullptr;
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
  bool work_ref_ = false;
  int pending_ops_ = 0;

  bool nodelay_ = true;  // 默认开启 TCP_NODELAY，避免 Nagle 攒包延迟
  KeepAliveOptions keepalive_{false, std::chrono::seconds(60)};

  ReadCallback read_callback_;
  SpanReadCallback span_callback_;
  bool keepalive_reads_ = false;
  std::size_t read_slots_ = kDefaultReadSlots;
  std::size_t read_chunk_size_ = kReadBufferSize;
  bool read_started_ = false;
  std::vector<std::vector<std::byte>> span_read_buffers_;
  ConnectCallback connect_callback_;
  WriteCallback shutdown_callback_;
  bool shutdown_pending_ = false;
  bool shutdown_issued_ = false;

  bool read_pool_ready_ = false;
  std::shared_ptr<BufferPool> read_pool_;
  std::vector<StreamReadOp*> read_ops_;
  std::vector<ReadCompletion> read_completions_;
  std::uint64_t next_submit_seq_ = 0;
  std::uint64_t next_deliver_seq_ = 0;
  // 落环等待前序的乱序完成数量；为 0 时完全绕过环形槽
  std::size_t ring_pending_ = 0;

  StreamConnectOp* connect_operation_ = nullptr;
  StreamWriteOp* current_write_ = nullptr;
  std::deque<StreamWriteOp*> writes_;
  std::vector<StreamWriteOp*> free_writes_;
  // 同步写完成时允许直调回调的深度：只放一层，更深走 post，避免递归爆栈
  int sync_write_depth_ = 0;
};

void StreamReadOp::on_complete(LoopImpl&, DWORD error) {
  owner->handle_read(this, error);
}

void StreamWriteOp::on_complete(LoopImpl&, DWORD error) {
  owner->handle_write(this, error);
}

void StreamConnectOp::on_complete(LoopImpl&, DWORD error) {
  owner->handle_connect(error);
}

struct AcceptOp final : WinOperation {
  std::shared_ptr<IocpTcpListenerImpl> owner;
  SOCKET accept_socket = INVALID_SOCKET;
  char buffer[2 * kAcceptAddressLength]{};

  void on_complete(LoopImpl& loop, DWORD error) override;
};

class IocpTcpListenerImpl final : public TcpListenerImpl,
                                  public std::enable_shared_from_this<IocpTcpListenerImpl> {
 public:
  explicit IocpTcpListenerImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}

  ~IocpTcpListenerImpl() { destroy(); }

  VoidResult bind(const SocketAddress& local, const BindOptions& options) {
    if (socket_ != INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::AlreadyExists));
    }
    if (local.family() == Family::Unspecified) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (options.reuse_port) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }

    load_extensions();
    if (g_acceptex == nullptr) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }

    const SOCKET socket = create_tcp_socket(local.family());
    if (socket == INVALID_SOCKET) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
    if (!win_poller(*loop_).associate(socket)) {
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

  VoidResult listen(int backlog, AcceptCallback callback) {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    if (listening_) {
      return std::unexpected(make_error(ErrorCode::AlreadyExists));
    }
    if (g_acceptex == nullptr) {
      return std::unexpected(make_error(ErrorCode::OperationNotSupported));
    }

    if (::listen(socket_, backlog) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }

    listening_ = true;
    accept_callback_ = std::move(callback);
    if (!post_accept()) {
      listening_ = false;
      accept_callback_ = nullptr;
      return std::unexpected(make_error(ErrorCode::Unknown));
    }
    return {};
  }

  [[nodiscard]] Result<SocketAddress> local_address() const {
    if (socket_ == INVALID_SOCKET) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    return win_socket_local_address(socket_);
  }

  [[nodiscard]] bool is_open() const noexcept { return socket_ != INVALID_SOCKET; }

  void close() { close_impl(); }

  void handle_accept(AcceptOp* operation, DWORD error) {
    --pending_ops_;
    // 从 pending list 移除（同步成功时可能已被 post_accept 移除过，忽略）
    accept_ops_.erase(std::remove(accept_ops_.begin(), accept_ops_.end(), operation),
                      accept_ops_.end());

    if (closing_) {
      (void)::closesocket(operation->accept_socket);
      operation->accept_socket = INVALID_SOCKET;
      finish_close_if_ready();
      return;
    }

    if (error != 0) {
      (void)::closesocket(operation->accept_socket);
      operation->accept_socket = INVALID_SOCKET;

      auto callback = accept_callback_;
      if (callback) {
        callback(std::unexpected(from_socket_error(static_cast<int>(error))));
      }
      if (listening_) {
        post_accept();
      }
      return;
    }

    const SOCKET accepted = operation->accept_socket;
    operation->accept_socket = INVALID_SOCKET;
    if (accepted == INVALID_SOCKET) {
      if (listening_) {
        post_accept();
      }
      return;
    }

    sockaddr* local_address_native = nullptr;
    sockaddr* remote_address_native = nullptr;
    int local_length = 0;
    int remote_length = 0;
    ::GetAcceptExSockaddrs(operation->buffer,
                           0,
                           kAcceptAddressLength,
                           kAcceptAddressLength,
                           &local_address_native,
                           &local_length,
                           &remote_address_native,
                           &remote_length);

    const Family family = remote_address_native != nullptr
                              ? AddressCodec::family_from_native(remote_address_native->sa_family)
                              : family_;

    if (!win_poller(*loop_).associate(accepted)) {
      (void)::closesocket(accepted);
      auto callback = accept_callback_;
      if (callback) {
        callback(std::unexpected(make_error(ErrorCode::Unknown)));
      }
      if (listening_) {
        post_accept();
      }
      return;
    }

    (void)win_socket_setopt(accepted,
                       SOL_SOCKET,
                       SO_UPDATE_ACCEPT_CONTEXT,
                       reinterpret_cast<char*>(&socket_),
                       sizeof(socket_));

    auto stream = std::make_shared<IocpTcpStreamImpl>(loop_);
    stream->adopt_socket(accepted, family);

    auto callback = accept_callback_;
    if (callback) {
      callback(adopt_stream(stream));
    }

    if (listening_ && !closing_) {
      post_accept();
    }
  }

 private:
  friend struct AcceptOp;

  bool post_accept() {
    if (socket_ == INVALID_SOCKET || !listening_ || closing_) {
      return false;
    }

    // 维护多个 outstanding AcceptOp — 多短连接同时到来时不再排队
    static constexpr std::size_t kMaxAcceptOps = 4;
    while (accept_ops_.size() < kMaxAcceptOps) {
      const SOCKET accepted =
          ::WSASocketW(AddressCodec::native_family(family_),
                       SOCK_STREAM,
                       IPPROTO_TCP,
                       nullptr,
                       0,
                       WSA_FLAG_OVERLAPPED);
      if (accepted == INVALID_SOCKET) {
        return false;
      }

      auto* operation = new AcceptOp{};
      operation->owner = shared_from_this();
      operation->file = reinterpret_cast<HANDLE>(socket_);
      operation->accept_socket = accepted;
      accept_ops_.push_back(operation);
      ++pending_ops_;

      DWORD bytes = 0;
      const BOOL ok = g_acceptex(socket_,
                                 accepted,
                                 operation->buffer,
                                 0,
                                 kAcceptAddressLength,
                                 kAcceptAddressLength,
                                 &bytes,
                                 &operation->ov);

      if (ok == FALSE) {
        const int error = WSAGetLastError();
        if (error != WSA_IO_PENDING) {
          // 提交失败，移除 + 清理
          accept_ops_.pop_back();
          --pending_ops_;
          (void)::closesocket(accepted);
          operation->accept_socket = INVALID_SOCKET;
          delete operation;
          return accept_ops_.empty();  // 如果全空了就报告失败，否则继续补其他
        }
      } else {
        // 同步成功时 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 不会投递完成包
        // 通过 post 走完成路径，避免递归 submit→drain→submit 爆栈
        auto it = std::find(accept_ops_.begin(), accept_ops_.end(), operation);
        if (it != accept_ops_.end()) {
          accept_ops_.erase(it);  // 从 pending list 移除
        }
        // 注意：不 --pending_ops_，保留给 handle_accept 去减（与原来单槽行为一致）
        operation->completed_synchronically = true;
        auto loop = loop_;
        loop_->post([operation, loop] {
          operation->on_complete(*loop, 0);
          delete operation;
        });
      }
    }
    return true;
  }

  void close_impl() {
    if (closing_) {
      return;
    }
    closing_ = true;
    listening_ = false;

    accept_callback_ = nullptr;

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
    listening_ = false;
    if (socket_ != INVALID_SOCKET) {
      (void)::CancelIoEx(reinterpret_cast<HANDLE>(socket_), nullptr);
      (void)::closesocket(socket_);
      socket_ = INVALID_SOCKET;
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

  std::shared_ptr<LoopImpl> loop_;
  SOCKET socket_ = INVALID_SOCKET;
  Family family_ = Family::Unspecified;
  bool listening_ = false;
  bool closing_ = false;
  bool work_ref_ = false;
  int pending_ops_ = 0;
  AcceptCallback accept_callback_;
  std::vector<AcceptOp*> accept_ops_;
};

void AcceptOp::on_complete(LoopImpl&, DWORD error) {
  owner->handle_accept(this, error);
}

std::shared_ptr<TcpStreamImpl> create_iocp_stream_impl(std::shared_ptr<LoopImpl> loop) {
  return std::make_shared<IocpTcpStreamImpl>(std::move(loop));
}

std::shared_ptr<TcpListenerImpl> create_iocp_listener_impl(std::shared_ptr<LoopImpl> loop) {
  return std::make_shared<IocpTcpListenerImpl>(std::move(loop));
}

std::shared_ptr<TcpStreamImpl> create_tcp_stream_impl(std::shared_ptr<LoopImpl> loop) {
  const Backend backend = loop->backend();
  if (backend == Backend::Readiness || backend == Backend::Select) {
    return create_ready_stream_impl(std::move(loop));
  }
  return create_iocp_stream_impl(std::move(loop));
}

std::shared_ptr<TcpListenerImpl> create_tcp_listener_impl(std::shared_ptr<LoopImpl> loop) {
  const Backend backend = loop->backend();
  if (backend == Backend::Readiness || backend == Backend::Select) {
    return create_ready_listener_impl(std::move(loop));
  }
  return create_iocp_listener_impl(std::move(loop));
}

} // namespace detail

TcpStream::TcpStream(Loop& loop) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = detail::create_tcp_stream_impl(impl->shared_from_this());
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
    impl_->close(true);
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
  impl_ = detail::create_tcp_listener_impl(impl->shared_from_this());
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
