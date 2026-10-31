#pragma once

#if !defined(_WIN32)
#error "udp_impl_win.hpp 仅用于 Windows"
#endif

#include <array>
#include <memory>
#include <span>

#include "uvnet/udp.hpp"
#include "detail/loop.hpp"

namespace uvnet::detail {

// Windows 两套 UDP 后端的公共接口：
//   Iocp（默认）：overlapped 收发 + 完成端口，吞吐优先
//   Readiness：wepoll 就绪通知 + 非阻塞 recvfrom/sendto，延迟优先
class UdpSocketImpl {
 public:
  virtual ~UdpSocketImpl() = default;

  virtual VoidResult bind(const SocketAddress& local) = 0;
  virtual VoidResult connect(const SocketAddress& remote) = 0;
  virtual VoidResult disconnect() = 0;

  virtual VoidResult send(std::span<const std::byte> data, WriteCallback cb) = 0;
  virtual VoidResult send(WriteBuffer buffer, WriteCallback cb) = 0;
  virtual VoidResult send_to(const SocketAddress& remote,
                             std::span<const std::byte> data,
                             WriteCallback cb) = 0;
  virtual VoidResult send_to(const SocketAddress& remote,
                             WriteBuffer buffer,
                             WriteCallback cb) = 0;

  virtual VoidResult start_recv(UdpRecvCallback cb) = 0;
  virtual VoidResult start_recv(UdpRecvViewCallback cb) = 0;
  virtual void stop_recv() = 0;
  virtual VoidResult set_recv_options(const UdpRecvOptions& options) = 0;

  virtual VoidResult set_broadcast(bool enable) = 0;
  virtual VoidResult set_ttl(int ttl) = 0;
  virtual VoidResult set_multicast_ttl(int ttl) = 0;
  virtual VoidResult set_multicast_loop(bool enable) = 0;
  virtual VoidResult set_multicast_interface(const SocketAddress& iface) = 0;
  virtual VoidResult set_membership(const SocketAddress& group,
                                    const SocketAddress& iface,
                                    bool join) = 0;

  [[nodiscard]] virtual Result<SocketAddress> local_address() const = 0;
  [[nodiscard]] virtual Result<int> recv_buffer_size() const = 0;
  virtual VoidResult set_recv_buffer_size(int bytes) = 0;
  [[nodiscard]] virtual Result<int> send_buffer_size() const = 0;
  virtual VoidResult set_send_buffer_size(int bytes) = 0;

  [[nodiscard]] virtual bool is_open() const = 0;
  [[nodiscard]] virtual bool is_connected() const = 0;

  virtual void close() = 0;

 protected:
  // SocketAddress 把 detail::UdpSocketImpl 声明为友元（用于组播选项取原始字节）；
  // 友元不继承，派生实现统一走这层访问
  [[nodiscard]] static const std::array<std::byte, 16>& address_bytes(
      const SocketAddress& address) {
    return address.bytes_;
  }
};

// 按 Loop 后端选择实现
std::shared_ptr<UdpSocketImpl> create_udp_socket_impl(std::shared_ptr<LoopImpl> loop,
                                                      Family family,
                                                      const UdpOptions& options);

std::shared_ptr<UdpSocketImpl> create_iocp_udp_socket_impl(std::shared_ptr<LoopImpl> loop,
                                                           Family family,
                                                           const UdpOptions& options);
std::shared_ptr<UdpSocketImpl> create_ready_udp_socket_impl(std::shared_ptr<LoopImpl> loop,
                                                            Family family,
                                                            const UdpOptions& options);

} // namespace uvnet::detail
