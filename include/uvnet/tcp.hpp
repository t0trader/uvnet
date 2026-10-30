#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>

#include "uvnet/address.hpp"
#include "uvnet/loop.hpp"

namespace uvnet {

namespace detail {
class TcpStreamImpl;
class TcpListenerImpl;
} // namespace detail

struct BindOptions {
  bool reuse_address = false;
  // Linux 使用 SO_REUSEPORT；Windows 不支持，请求时会返回 OperationNotSupported
  bool reuse_port = false;
  bool ipv6_only = false;
};

struct KeepAliveOptions {
  bool enable = true;
  std::chrono::seconds delay{60};
};

// 读窗口配置（必须在首次 start_read 之前设置）
struct ReadOptions {
  // Windows 多 outstanding 读槽数量；Linux 忽略该字段
  std::size_t slots = 4;
  // 单次读缓冲大小
  std::size_t chunk_size = 64 * 1024;
};

using ConnectCallback = std::function<void(VoidResult)>;
using WriteCallback = std::function<void(VoidResult)>;

// 读块：keepalive 用于把底层存储保持到读回调返回；
// 也可直接转交给零拷贝写，实现无拷贝转发。
struct ReadChunk {
  std::span<const std::byte> data;
  std::shared_ptr<const void> keepalive;
};

using ReadCallback = std::function<void(Result<ReadChunk>)>;
// 低开销读回调：不构造 keepalive，数据仅在回调期间有效
using SpanReadCallback = std::function<void(Result<std::span<const std::byte>>)>;

// 零拷贝写视图：数据不会被库复制。
// keepalive 用于把底层存储保持到写完成回调返回；为空表示调用方自行保证生命周期。
struct WriteBuffer {
  std::span<const std::byte> data;
  std::shared_ptr<const void> keepalive;
};

// TCP 连接。句柄可拷贝，底层共享同一个连接。
class TcpStream {
 public:
  explicit TcpStream(Loop& loop);
  ~TcpStream();

  TcpStream(const TcpStream&) = default;
  TcpStream& operator=(const TcpStream&) = default;
  TcpStream(TcpStream&&) noexcept = default;
  TcpStream& operator=(TcpStream&&) noexcept = default;

  // 绑定本地地址（可选，connect 前调用可指定源地址）
  VoidResult bind(const SocketAddress& local, const BindOptions& options = {});
  // 异步连接；回调总是在循环线程异步执行
  VoidResult connect(const SocketAddress& remote, ConnectCallback cb = {});

  // 必须在首次 start_read 之前调用
  VoidResult set_read_options(const ReadOptions& options);

  // 零拷贝读：回调携带 keepalive，可直接转交零拷贝写
  VoidResult start_read(ReadCallback cb);
  // 低开销读：不分配 keepalive，数据仅在回调期间有效
  VoidResult start_read(SpanReadCallback cb);
  void stop_read();

  // 数据会被复制进内部缓冲区，调用返回后即可释放
  VoidResult write(std::span<const std::byte> data, WriteCallback cb = {});
  // 零拷贝写：buffer.data 指向的存储需在完成回调返回前保持有效
  VoidResult write(WriteBuffer buffer, WriteCallback cb = {});
  // 等待已排队的写完成后关闭写方向
  VoidResult shutdown(WriteCallback cb = {});

  // 取消所有未完成操作并关闭
  void close();
  // 以 RST 方式关闭
  void close_reset();

  [[nodiscard]] bool is_open() const;
  [[nodiscard]] bool is_connected() const;

  [[nodiscard]] Result<SocketAddress> local_address() const;
  [[nodiscard]] Result<SocketAddress> peer_address() const;

  VoidResult set_nodelay(bool enable);
  VoidResult set_keepalive(const KeepAliveOptions& options = {});

  [[nodiscard]] Result<int> recv_buffer_size() const;
  VoidResult set_recv_buffer_size(int bytes);
  [[nodiscard]] Result<int> send_buffer_size() const;
  VoidResult set_send_buffer_size(int bytes);

 private:
  explicit TcpStream(std::shared_ptr<detail::TcpStreamImpl> impl);
  std::shared_ptr<detail::TcpStreamImpl> impl_;

  friend class detail::TcpListenerImpl;
};

using AcceptCallback = std::function<void(Result<TcpStream>)>;

// TCP 监听器。句柄可拷贝，底层共享同一个 socket。
class TcpListener {
 public:
  explicit TcpListener(Loop& loop);
  ~TcpListener();

  TcpListener(const TcpListener&) = default;
  TcpListener& operator=(const TcpListener&) = default;
  TcpListener(TcpListener&&) noexcept = default;
  TcpListener& operator=(TcpListener&&) noexcept = default;

  VoidResult bind(const SocketAddress& local, const BindOptions& options = {});
  VoidResult listen(int backlog, AcceptCallback cb);

  [[nodiscard]] Result<SocketAddress> local_address() const;
  [[nodiscard]] bool is_open() const;

  void close();

 private:
  std::shared_ptr<detail::TcpListenerImpl> impl_;
};

} // namespace uvnet
