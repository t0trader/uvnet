#pragma once

#if !defined(_WIN32)
#error "tcp_impl_win.hpp 仅用于 Windows"
#endif

#include <memory>
#include <span>

#include "uvnet/tcp.hpp"
#include "detail/loop.hpp"

namespace uvnet::detail {

// Windows 两套 TCP 后端的公共接口：
//   Iocp（默认）：overlapped I/O + 完成端口，吞吐优先
//   Readiness：wepoll 就绪通知 + 非阻塞 recv/send，延迟优先
class TcpStreamImpl {
 public:
  virtual ~TcpStreamImpl() = default;

  virtual VoidResult bind(const SocketAddress& local, const BindOptions& options) = 0;
  virtual VoidResult connect(const SocketAddress& remote, ConnectCallback cb) = 0;
  virtual VoidResult set_read_options(const ReadOptions& options) = 0;
  virtual VoidResult start_read(ReadCallback cb) = 0;
  virtual VoidResult start_read(SpanReadCallback cb) = 0;
  virtual void stop_read() = 0;
  virtual VoidResult write(std::span<const std::byte> data, WriteCallback cb) = 0;
  virtual VoidResult write(WriteBuffer buffer, WriteCallback cb) = 0;
  virtual VoidResult shutdown(WriteCallback cb) = 0;
  virtual void close(bool reset) = 0;
  [[nodiscard]] virtual bool is_open() const = 0;
  [[nodiscard]] virtual bool is_connected() const = 0;
  [[nodiscard]] virtual Result<SocketAddress> local_address() const = 0;
  [[nodiscard]] virtual Result<SocketAddress> peer_address() const = 0;
  virtual VoidResult set_nodelay(bool enable) = 0;
  virtual VoidResult set_keepalive(const KeepAliveOptions& options) = 0;
  [[nodiscard]] virtual Result<int> recv_buffer_size() const = 0;
  virtual VoidResult set_recv_buffer_size(int bytes) = 0;
  [[nodiscard]] virtual Result<int> send_buffer_size() const = 0;
  virtual VoidResult set_send_buffer_size(int bytes) = 0;
};

class TcpListenerImpl {
 public:
  virtual ~TcpListenerImpl() = default;

  virtual VoidResult bind(const SocketAddress& local, const BindOptions& options) = 0;
  virtual VoidResult listen(int backlog, AcceptCallback cb) = 0;
  [[nodiscard]] virtual Result<SocketAddress> local_address() const = 0;
  [[nodiscard]] virtual bool is_open() const = 0;
  virtual void close() = 0;

 protected:
  // 由派生监听器在 accept 完成时构造对外句柄（TcpStream 的构造函数是私有的）
  [[nodiscard]] static TcpStream adopt_stream(std::shared_ptr<TcpStreamImpl> impl) {
    return TcpStream(std::move(impl));
  }
};

// 按 Loop 后端选择实现
std::shared_ptr<TcpStreamImpl> create_tcp_stream_impl(std::shared_ptr<LoopImpl> loop);
std::shared_ptr<TcpListenerImpl> create_tcp_listener_impl(std::shared_ptr<LoopImpl> loop);

std::shared_ptr<TcpStreamImpl> create_iocp_stream_impl(std::shared_ptr<LoopImpl> loop);
std::shared_ptr<TcpListenerImpl> create_iocp_listener_impl(std::shared_ptr<LoopImpl> loop);
std::shared_ptr<TcpStreamImpl> create_ready_stream_impl(std::shared_ptr<LoopImpl> loop);
std::shared_ptr<TcpListenerImpl> create_ready_listener_impl(std::shared_ptr<LoopImpl> loop);

} // namespace uvnet::detail
