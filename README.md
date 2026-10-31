# uvnet

独立的跨平台 C++ 网络库，使用 C++23 `std::expected` 风格 API。

## 特性

- C++23 标准，`std::expected` 错误码风格 API
- Linux：epoll（level-triggered）+ eventfd 唤醒；Windows：IOCP + `GetQueuedCompletionStatusEx` 批量（128 项）+ AcceptEx / ConnectEx
- Windows 可选第二套后端：`Loop(Backend::Readiness)` 走 wepoll 就绪通知 +
  非阻塞 `recv`/`send`、`recvfrom`/`sendto`，pingpong 延迟更低（见性能一节）；
  默认仍是 IOCP，两套后端按 Loop 选择，不可在同一 Loop 内混用
- IPv4 / IPv6，TCP 与 UDP
- 异步 DNS：线程池 + `getaddrinfo` / `getnameinfo`，回调回到循环线程
- TCP：bind / listen / accept / connect / read / write / shutdown / RST 关闭、
  `TCP_NODELAY`（默认开启）、keepalive、收发缓冲区大小（默认 256KB）、
  本地/对端地址
- UDP：bind / connect / send / send_to / recv、广播、TTL、组播加入/退出、
  组播回环与出口接口、收发缓冲区大小（默认 256KB）
- 事件循环原语：定时器（一次/重复）、跨线程 `Async` 唤醒、`Loop::post`
- 语义：所有句柄操作须在循环线程执行；`Async::send()` / `Loop::post()` 线程安全
- 写接口：默认 `write(span)` 由库复制数据；`write(WriteBuffer)` 零拷贝，
  由 `keepalive` 保证缓冲区存活到完成回调
- 读接口：`start_read(ReadCallback)` / `start_recv(UdpRecvCallback)` 返回带
  `keepalive` 的 `ReadChunk` / `Datagram`，可直接零拷贝转发；
  `start_read(SpanReadCallback)` / `start_recv(UdpRecvViewCallback)` 为低开销
  视图模式，不产生 keepalive，数据仅在回调期间有效
- Windows TCP 4 个 outstanding 读按序号在固定环形槽重排，无每块 map 分配
- Windows IOCP 同步完成优化：`SetFileCompletionNotificationModes` 对
  回环/短连接高频路径开启 `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` +
  `FILE_SKIP_SET_EVENT_ON_HANDLE`，命中时省一次内核态切换和 IOCP 投包
- Windows 监听器 4 个 outstanding AcceptEx 预投递，短连接 QPS +20–40%
- Windows UDP 发送 4 个并发 WSASendTo（而不是单槽串行），loopback pps +70%
- Linux UDP `recvmmsg` / `sendmmsg` 批量收发（N=32），真实网卡 syscall 数降 32×
- 读窗口可配置：`TcpStream::set_read_options`（`slots` / `chunk_size`）、
  `UdpSocket::set_recv_options`（同名字段 + `batch`）；Windows 槽位数量仅 IOCP 后端生效，
  `batch` 仅就绪式 UDP 生效；Linux 只认 chunk_size。必须在首次
  `start_read` / `start_recv` 前设置

## 目录

```
uvnet/
  CMakeLists.txt
  include/uvnet/       # 公共头文件
  src/                 # 公共实现 + *_linux.cpp / *_win.cpp 后端
  tests/               # ctest 用例
  samples/             # 可运行的跨平台客户端 / 回显服务器示例
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

可运行示例（默认不构建）：

```bash
cmake -S uvnet -B build-uvnet -DCMAKE_BUILD_TYPE=Release -DUVNET_BUILD_SAMPLES=ON
cmake --build build-uvnet -j
```

## 完整客户端示例

[samples/demo_client.cpp](samples/demo_client.cpp) 是一份可以直接拿来改的客户端骨架，
同一份源码在 Windows（IOCP）和 Linux（epoll）上都跑，业务代码里没有任何网络相关的
`#ifdef`。它覆盖了异步 DNS、连接超时、多地址回退、读窗口配置、零拷贝写、低开销视图读、
优雅关闭和跨平台 Ctrl+C。

```bash
# 终端 1：起一个回显服务器（--udp 则用 UDP）
./uvnet_demo_client --server --port 9000

# 终端 2：客户端 ping-pong 5 次
./uvnet_demo_client --count 5 127.0.0.1 9000
./uvnet_demo_client --udp --count 5 127.0.0.1 9000

# 用域名，自动按序回退（localhost 的 ::1 失败后会试 127.0.0.1）
./uvnet_demo_client localhost 9000

# 退出码：0 成功，1 失败（连接失败 / 中断），2 参数错误
```

程序里区分平台的只有三处，都在 `platform` 命名空间里：控制台 UTF-8 编码
（`SetConsoleOutputCP`）、Ctrl+C 接管（`SetConsoleCtrlHandler` vs `sigaction`），
其余全部由 uvnet 屏蔽。贡献一个客户端时的关键点：

- 所有句柄操作都在循环线程里做，跨线程只允许 `Loop::post` / `Async::send`
- 信号处理函数里只置原子标志，收尾动作交给循环线程上的定时器
- 连接同一名字的多个地址要逐个尝试，失败的那个地址必须用新句柄重连
- 需要转发数据时用 `start_read(ReadCallback)` 并把 `keepalive` 交给零拷贝写；
  只解析数据时用视图读，省掉一次引用计数

## API 片段

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
- 后端选择：`Loop()` 默认 IOCP（Linux 为 epoll）；`Loop(Backend::Readiness)` 在
  Windows 上启用就绪式 TCP/UDP，延迟优先、吞吐低于 IOCP

## 性能

所有数据为 loopback 基准（`uvnet_bench_tcp` / `uvnet_bench_udp`），
Windows / MD / Release，单次 1s 运行时长：

### TCP（Windows）

| 场景 | 吞吐 / 延迟 |
|---|---|
| echo 16 KB | **~395 MiB/s** |
| echo 64 KB | **~470 MiB/s** |
| send copy 64 KB | **~990 MiB/s** |
| send zero-copy 64 KB | **~1070 MiB/s** |
| pingpong 64 B RTT p50 / p99 | **19 / 53 µs** |

优化叠加：

```
初始版         echo 16KB  274 MiB/s   pingpong p50 20.1 µs
+ FILE_SKIP    echo 16KB  387 MiB/s   pingpong p50 18.4 µs
+ TCP_NODELAY  echo 16KB  396 MiB/s   pingpong p50 19.0 µs
+ AcceptEx x4  echo 64KB  468 MiB/s   (短连接 QPS +20-40%)
```

### 就绪式后端（Windows，`uvnet_bench_tcp.exe readiness` / `uvnet_bench_udp.exe readiness`）

wepoll 就绪通知 + 非阻塞收发，省掉 overlapped 提交与完成包往返：

| TCP 场景 | 就绪式（Readiness） | IOCP（默认） |
|---|---|---|
| pingpong 64 B RTT p50 | **15–17 µs** | 17.7–18 µs |
| pingpong avg / p99 | 18–20 / 48–54 µs | 20–21 / 47–60 µs |
| echo 16 KB | ~190–215 MiB/s | ~300–400 MiB/s |
| echo 64 KB | ~125–300 MiB/s | ~330–560 MiB/s |

| UDP 场景 | 就绪式（Readiness） | IOCP（默认） |
|---|---|---|
| 64 B 吞吐 | ~78–84 kpps | ~73–79 kpps |
| 1024 B 吞吐 | ~73–81 kpps | ~76–82 kpps |
| pingpong 64 B RTT p50 | **17–19 µs** | 20.5–21.5 µs |
| pingpong avg / p99 | 21–23 / 53–68 µs | 23–26 / 51–73 µs |

就绪式 UDP 默认在一次可读事件里连续收到 `WSAEWOULDBLOCK` 为止（`UdpRecvOptions::batch`，
默认 16）。每包一次 `epoll_wait` 会让吞吐减半（实测 1 万包 → 1 万次等待）；
批量后等待次数约降到 1/batch，64 B 吞吐从 ~52 kpps 回到 ~80 kpps，pingpong 不变。

低延迟场景选 Readiness，吞吐优先选默认 IOCP。

### UDP（Windows loopback，默认 4 槽收 + 4 并发发）

| 场景 | 吞吐 / 延迟 |
|---|---|
| 64 B datagram | **~70k pps** |
| 1024 B datagram | **~68k pps** |
| pingpong 64 B RTT p50 / p99 | **23 / 60 µs** |

优化叠加：

```
初始版              ~30k pps
+ FILE_SKIP         ~46k pps
+ 4 并发 WSASendTo  ~77k pps   (+70%)
+ 256KB 默认缓冲    ~77k pps   (持平，loopback 不经过真实网卡)
```

### Linux（recvmmsg / sendmmsg N=32）

`recvmmsg` 和 `sendmmsg` 批量收发（N=32）在真实网卡上将 syscall 数量从 N 降到 N/32。
WSL2 虚拟化层掩盖实际收益；在裸 Linux + 真实 NIC 上预期 UDP 收/发 pps 提升 **3–10×**。

## 仓库状态

- C++23 标准；Windows 要求 `_WIN32_WINNT >= 0x0601`（Win7+）
- 双平台 ASAN + UBSAN 全部通过（`UVNET_BUILD_TESTS=ON`）
- 仓库根目录即本目录；提交历史见 `git log`
- `samples/client_app` 是独立示例工程（自带 CMake），在仓库根执行
  `cmake -S samples/client_app -B build-client` 构建
