#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <span>

#include "uvnet/address.hpp"
#include "uvnet/loop.hpp"
#include "uvnet/tcp.hpp"

namespace uvnet {

namespace detail {
class UdpSocketImpl;
} // namespace detail

struct UdpOptions {
  bool reuse_address = false;
  bool reuse_port = false;
  bool ipv6_only = false;
};

// UDP 收包窗口配置（必须在首次 start_recv 之前设置）
struct UdpRecvOptions {
  // Windows 多 outstanding 收包槽数量；Linux 忽略该字段
  std::size_t slots = 4;
  std::size_t chunk_size = 64 * 1024;
};

struct Datagram {
  std::span<const std::byte> data;
  SocketAddress source;
  // 保持底层存储到回调返回；可直接用于零拷贝转发
  std::shared_ptr<const void> keepalive;
};

struct DatagramView {
  std::span<const std::byte> data;
  SocketAddress source;
};

using UdpRecvCallback = std::function<void(Result<Datagram>)>;
using UdpRecvViewCallback = std::function<void(Result<DatagramView>)>;

// UDP socket，支持 IPv4 / IPv6、组播、广播与常用 socket 选项。
class UdpSocket {
 public:
  explicit UdpSocket(Loop& loop,
                     Family family = Family::IPv4,
                     const UdpOptions& options = {});
  ~UdpSocket();

  UdpSocket(const UdpSocket&) = default;
  UdpSocket& operator=(const UdpSocket&) = default;
  UdpSocket(UdpSocket&&) noexcept = default;
  UdpSocket& operator=(UdpSocket&&) noexcept = default;

  VoidResult bind(const SocketAddress& local);
  VoidResult connect(const SocketAddress& remote);
  // Windows 不支持断开 UDP 连接，会返回 OperationNotSupported
  VoidResult disconnect();

  // 已 connect 时使用默认目标；未 connect 时返回 InvalidArgument
  VoidResult send(std::span<const std::byte> data, WriteCallback cb = {});
  // 零拷贝发送：buffer.data 需在完成回调返回前保持有效
  VoidResult send(WriteBuffer buffer, WriteCallback cb = {});
  VoidResult send_to(const SocketAddress& remote,
                     std::span<const std::byte> data,
                     WriteCallback cb = {});
  VoidResult send_to(const SocketAddress& remote,
                     WriteBuffer buffer,
                     WriteCallback cb = {});

  // 零拷贝接收：回调携带 keepalive，可直接转交零拷贝写
  VoidResult start_recv(UdpRecvCallback cb);
  // 低开销接收：不构造 keepalive，数据仅在回调期间有效
  VoidResult start_recv(UdpRecvViewCallback cb);
  void stop_recv();

  // 必须在首次 start_recv 之前调用
  VoidResult set_recv_options(const UdpRecvOptions& options);

  VoidResult set_broadcast(bool enable);
  VoidResult set_ttl(int ttl);
  VoidResult set_multicast_ttl(int ttl);
  VoidResult set_multicast_loop(bool enable);
  VoidResult set_multicast_interface(const SocketAddress& iface);
  VoidResult set_membership(const SocketAddress& group,
                            const SocketAddress& iface,
                            bool join);

  [[nodiscard]] Result<SocketAddress> local_address() const;

  [[nodiscard]] Result<int> recv_buffer_size() const;
  VoidResult set_recv_buffer_size(int bytes);
  [[nodiscard]] Result<int> send_buffer_size() const;
  VoidResult set_send_buffer_size(int bytes);

  [[nodiscard]] bool is_open() const;
  [[nodiscard]] bool is_connected() const;

  void close();

 private:
  std::shared_ptr<detail::UdpSocketImpl> impl_;
};

} // namespace uvnet
