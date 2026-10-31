# client_app

基于 [uvnet](../../) 的跨平台网络客户端工程骨架。同一份源码在 Windows（IOCP）和
Linux（epoll）上编译运行，业务代码里没有网络相关的 `#ifdef`。

线协议是行格式 ping/pong：

```
client -> PING <seq>\n
server -> PONG <seq>\n
```

## 目录结构

```
client_app/
  CMakeLists.txt
  include/app/
    config.hpp        # 运行参数与命令行解析
    logger.hpp        # 分级日志（线程安全）
    platform.hpp      # 控制台编码 + Ctrl+C 接管（唯一区分平台的地方）
    protocol.hpp      # 线协议编解码 + 增量行解码器
    session.hpp       # 客户端会话：连接/回退/重试/请求响应/优雅关闭
    echo_server.hpp   # 本地联调用回显服务器
  src/
    main.cpp          # 客户端入口：参数 -> 循环 -> 退出码
    config.cpp
    logger.cpp
    platform.cpp
    protocol.cpp
    session.cpp       # 会话层，全部网络时序都在这里
    echo_server.cpp
  tools/
    echo_server.cpp   # 回显服务器命令行入口
  tests/
    test_protocol.cpp # 纯逻辑单元测试（不碰网络）
    test_session.cpp  # 同进程端到端测试（TCP / 域名回退 / UDP）
    test_support.hpp
```

## 构建

依赖 uvnet 源码树，默认取仓库根目录（`../..`）；放到别处时用 `-DUVNET_ROOT=<路径>` 指定。
以下命令均在仓库根目录执行。

Windows（VS2026）：

```bat
cmake -S samples/client_app -B build-client -G "Visual Studio 18 2026" -A x64 ^
      -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreadedDLL
cmake --build build-client --config Release
ctest --test-dir build-client -C Release --output-on-failure
```

Linux（gcc）：

```bash
cmake -S samples/client_app -B build-client -DCMAKE_BUILD_TYPE=Release
cmake --build build-client -j
ctest --test-dir build-client --output-on-failure
```

产物：

- `client_app` — 客户端
- `echo_server` — TCP / UDP 回显服务器，用于本地联调

## 运行

```bash
# 终端 1：起服务器（--udp 则用 UDP）
./build-client/echo_server --port 9000

# 终端 2：客户端
./build-client/client_app --count 5 --interval 20 127.0.0.1 9000
./build-client/client_app --udp --count 5 127.0.0.1 9000

# 域名：自动按解析顺序回退（localhost 的 ::1 失败后会试 127.0.0.1）
./build-client/client_app localhost 9000

# 常用参数
--timeout MS          单轮响应超时（默认 3000），超时视为连接失效并重连
--connect-timeout MS  单个候选地址的连接超时（默认 3000）
--retries N           失败后的重试次数（默认 2），指数退避 200ms/400ms…
--slots N             读/收包槽位数量（默认 4，仅 Windows 生效）
--chunk BYTES         单次读缓冲（默认 32768）
--log-level LEVEL     debug / info / warn / error
```

退出码：`0` 成功，`1` 失败（连接/超时/中断），`2` 参数错误。

## 分层与职责

| 层 | 文件 | 职责 |
|---|---|---|
| 平台适配 | `platform.cpp` | 控制台 UTF-8、Ctrl+C 接管。信号处理函数只置原子标志 |
| 协议 | `protocol.cpp` | 线格式编解码；`LineDecoder` 处理半行/多行 |
| 会话 | `session.cpp` | 异步 DNS、多地址回退、连接超时、单轮超时、重连退避、优雅关闭 |
| 入口 | `main.cpp` | 参数解析、事件循环、Ctrl+C 轮询、退出码 |

要点：

- 所有 uvnet 句柄操作都在循环线程执行；跨线程只用 `Loop::post`
- 回调里统一用 `shared_from_this()` 保活，避免回调期间对象被销毁
- 连接失败后重试一律新建句柄，不复用已失败的 socket
- UDP 在回调栈里不能立刻析构 `UdpSocket`，`teardown_udp(true)` 把释放挪到队列尾部
- 成功路径走 `shutdown()` 等排队写落地，失败路径直接 `close()`

## 扩展指引

**换协议**：只改 `protocol.cpp` 的 `encode_message` / `decode_message`；
`session.cpp` 只需在 `on_line` 里匹配自己的消息类型。二进制协议把 `LineDecoder`
换成"长度前缀"解码器即可，`feed()` 的调用点不变。

**加 TLS**：在 `ClientSession::on_connected` 里把 `TcpStream` 包一层 TLS 会话，
读回调用 TLS 解密后的明文喂给 `decoder_`，写走 TLS 加密；会话层的重试/超时逻辑不受影响。

**加业务请求排队**：`send_next()` 目前是严格 ping-pong；要支持多并发请求，
把单个 `pending_sequence_` 换成 `std::map<seq, 发送时间>`，响应按 seq 匹配并统计 RTT。

**加指标**：会话已有 `completed_requests()` 和内部 RTT 样本；需要正式指标时把
`rtt_samples_us_` 换成环形缓冲，并对外暴露 p50/p99 查询。

## 测试

```bash
ctest --test-dir build-client -C Release --output-on-failure
```

- `app_protocol`：编解码边界（空行、缺字段、非数字、CRLF、半行、多行）
- `app_session`：同进程起回显服务器，验证 TCP 回环、域名多地址回退、UDP 回环

两个用例都带 15 秒看门狗定时器，卡住会失败而不是挂起。
