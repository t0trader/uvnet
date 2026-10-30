# uvnet

从 libuv 分离出来的独立跨平台 C++ 网络库，不依赖 libuv，使用 C++23 `std::expected` 风格 API。

## 特性

- Linux：epoll + eventfd；Windows：IOCP + AcceptEx / ConnectEx
- IPv4 / IPv6，TCP 与 UDP
- 异步 DNS：线程池 + `getaddrinfo` / `getnameinfo`，回调回到循环线程
- TCP：bind / listen / accept / connect / read / write / shutdown / RST 关闭、
  `TCP_NODELAY`、keepalive、收发缓冲区大小、本地/对端地址
- UDP：bind / connect / send / send_to / recv、广播、TTL、组播加入/退出、
  组播回环与出口接口、收发缓冲区大小
- 事件循环原语：定时器（一次/重复）、跨线程 `Async` 唤醒、`Loop::post`
- 语义：所有句柄操作须在循环线程执行；`Async::send()` / `Loop::post()` 线程安全
- 写接口：默认 `write(span)` 由库复制数据；`write(WriteBuffer)` 零拷贝，
  由 `keepalive` 保证缓冲区存活到完成回调
- 读接口：`start_read(ReadCallback)` / `start_recv(UdpRecvCallback)` 返回带
  `keepalive` 的 `ReadChunk` / `Datagram`，可直接零拷贝转发；
  `start_read(SpanReadCallback)` / `start_recv(UdpRecvViewCallback)` 为低开销
  视图模式，不产生 keepalive，数据仅在回调期间有效
- Windows TCP 4 个 outstanding 读按序号在固定环形槽重排，无每块 map 分配
- 读窗口可配置：`TcpStream::set_read_options`（`slots` / `chunk_size`）、
  `UdpSocket::set_recv_options`（同名字段）；Windows 槽位数量生效，
  Linux 仅 chunk_size 生效。必须在首次 `start_read` / `start_recv` 前设置

## 目录

```
uvnet/
  CMakeLists.txt
  include/uvnet/       # 公共头文件
  src/                 # 公共实现 + *_linux.cpp / *_win.cpp 后端
  tests/               # ctest 用例
```

## 构建

Windows（VS2026，x64，/MD 或 /MT 由 `CMAKE_MSVC_RUNTIME_LIBRARY` 控制）：

```bat
cmake -S uvnet -B build-uvnet -G "Visual Studio 18 2026" -A x64 ^
      -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreadedDLL
cmake --build build-uvnet --config Release
ctest --test-dir build-uvnet -C Release --output-on-failure
```

Linux（WSL2 / gcc）：

```bash
cmake -S uvnet -B build-uvnet -DCMAKE_BUILD_TYPE=Release
cmake --build build-uvnet -j
ctest --test-dir build-uvnet --output-on-failure
```

链接目标：`uvnet`（静态库，CMake 导出 `uvnet` target）。

性能基准（默认不构建）：

```bash
cmake -S uvnet -B build-uvnet -DCMAKE_BUILD_TYPE=Release \
      -DUVNET_BUILD_BENCHMARKS=ON -DUVNET_BUILD_TESTS=OFF
cmake --build build-uvnet -j
./build-uvnet/benchmarks/uvnet_bench_tcp
./build-uvnet/benchmarks/uvnet_bench_udp
./build-uvnet/benchmarks/uvnet_bench_dns
```

## 示例

```cpp
#include <uvnet/uvnet.hpp>

using namespace uvnet;

int main() {
  Loop loop;

  TcpListener listener(loop);
  if (!listener.bind(SocketAddress::any(Family::IPv6, 9000), BindOptions{.ipv6_only = false})) {
    return 1;
  }
  listener.listen(128, [](Result<TcpStream> accepted) {
    if (!accepted) {
      return;
    }
    auto stream = std::make_shared<TcpStream>(std::move(*accepted));
    stream->start_read([stream](Result<std::span<const std::byte>> chunk) {
      if (!chunk) {
        stream->close();
        return;
      }
      stream->write(*chunk);  // 回显
    });
  });

  loop.run();
}
```

异步解析：

```cpp
Resolver resolver(loop);
resolver.resolve("example.com", 443, ResolveOptions{.socket_type = SocketType::Stream},
                 [](Result<std::vector<SocketAddress>> addresses) {
                   if (addresses) {
                     // 使用 addresses
                   }
                 });
```

零拷贝写（TCP / UDP 通用，`keepalive` 为空时由调用方自行保证生命周期）：

```cpp
auto payload = std::make_shared<std::vector<std::byte>>(4096);
stream->write(WriteBuffer{*payload, payload}, [](VoidResult result) {
  // 回调返回前 payload 一直有效
});
```

零拷贝转发（读到的数据不再复制，直接写回/转发）：

```cpp
stream->start_read([stream](Result<ReadChunk> chunk) {
  if (!chunk) {
    return;
  }
  stream->write(WriteBuffer{chunk->data, chunk->keepalive}, {});
});
```

不需要转发时优先使用低开销视图读：

```cpp
stream->start_read([](Result<std::span<const std::byte>> chunk) {
  // chunk 数据仅在回调期间有效
});
```

## 平台差异

- `BindOptions::reuse_port` / `UdpOptions::reuse_port`：Windows 不支持，返回 `OperationNotSupported`
- `UdpSocket::disconnect`：Windows 不支持，返回 `OperationNotSupported`
- Windows 的 IOCP 完成通知通过 `GetQueuedCompletionStatusEx` 批量获取；
  定时器基于单调时钟的最小堆，精度受系统调度粒度影响
- Windows TCP 在视图读模式下复用可配置数量（默认 4）的 64KB 读槽位；零拷贝读模式
  按需从缓冲池取块。Windows UDP 视图模式复用可配置数量（默认 4）的 64KB 收包槽位，
  零拷贝模式按需取块；未启动读写时不分配
- 关闭 TCP 连接时，排队中的写回调会收到 `ErrorCode::Canceled`；
  监听器关闭不会再次触发 accept 回调
