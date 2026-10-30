// uvnet/samples/demo_client.cpp
//
// 一个完整的跨平台客户端示例：同一份源码在 Windows（IOCP）与 Linux（epoll）
// 上行为一致，业务代码里没有任何和网络相关的 #ifdef。
//
// 客户端做的事：
//   1. 异步 DNS 解析（Resolver，线程池，回调回到循环线程）
//   2. 连接超时保护（Timer）
//   3. 配置读窗口 / TCP_NODELAY / keepalive
//   4. 零拷贝写：payload 的 shared_ptr 作为 keepalive
//   5. 低开销视图读：不构造 keepalive，数据只在回调期间有效
//   6. 逐包 ping-pong，统计 RTT（min/avg/p50/p99/max）
//   7. 优雅关闭（shutdown 等排队的写落地后再 close）
//   8. 跨平台 Ctrl+C：信号处理函数只置原子标志，收尾在循环线程上做
//
// 另外带一个 --server 回显服务器模式，方便本地联调（演示零拷贝转发）。

#include <uvnet/uvnet.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#endif

using namespace uvnet;

namespace {

using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

// ---------------------------------------------------------------------------
// 平台适配层：整个示例里唯一需要区分平台的地方
// ---------------------------------------------------------------------------

namespace platform {

std::atomic<bool> g_stop_requested{false};

void init_console() {
#ifdef _WIN32
  // Windows 控制台默认不是 UTF-8，不设置的话中文会乱码
  ::SetConsoleOutputCP(CP_UTF8);
  ::SetConsoleCP(CP_UTF8);
#endif
}

#ifdef _WIN32
BOOL WINAPI console_ctrl_handler(DWORD event) {
  if (event == CTRL_C_EVENT || event == CTRL_BREAK_EVENT || event == CTRL_CLOSE_EVENT) {
    g_stop_requested.store(true, std::memory_order_relaxed);
    return TRUE;  // 已接管，阻止运行时默认的立即退出
  }
  return FALSE;
}
#else
// 信号处理函数里只能做 async-signal-safe 操作：这里只写一个原子标志，
// 真正的收尾工作交给循环线程上的 tick 定时器。
extern "C" void posix_signal_handler(int) {
  g_stop_requested.store(true, std::memory_order_relaxed);
}
#endif

void install_stop_handler() {
#ifdef _WIN32
  ::SetConsoleCtrlHandler(console_ctrl_handler, TRUE);
#else
  struct sigaction action {};
  action.sa_handler = posix_signal_handler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  ::sigaction(SIGINT, &action, nullptr);
  ::sigaction(SIGTERM, &action, nullptr);
#endif
}

} // namespace platform

// ---------------------------------------------------------------------------
// 命令行参数
// ---------------------------------------------------------------------------

struct Options {
  bool udp = false;
  bool server = false;
  std::string host = "127.0.0.1";
  std::uint16_t port = 9000;
  int count = 5;
  int interval_ms = 20;
};

void print_usage(const char* program) {
  std::printf(
      "用法:\n"
      "  %s [选项] <host> <port>        客户端模式\n"
      "  %s --server [--udp] [--port N]  回显服务器模式（本地联调）\n"
      "\n选项:\n"
      "  --udp            使用 UDP（默认 TCP）\n"
      "  --count N        收发包数（默认 5）\n"
      "  --interval MS    两次发送之间的间隔，毫秒（默认 20）\n"
      "  --port N         服务器模式监听端口（默认 9000）\n"
      "  --help           显示本帮助\n",
      program, program);
}

enum class ArgResult {
  Run,
  Exit,
  Error,
};

ArgResult parse_options(int argc, char** argv, Options& options) {
  std::vector<std::string> positional;

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      return ArgResult::Exit;
    }
    if (arg == "--udp") {
      options.udp = true;
    } else if (arg == "--server") {
      options.server = true;
    } else if (arg == "--count") {
      if (++i >= argc) {
        std::fprintf(stderr, "--count 缺少参数\n");
        return ArgResult::Error;
      }
      options.count = std::atoi(argv[i]);
    } else if (arg == "--interval") {
      if (++i >= argc) {
        std::fprintf(stderr, "--interval 缺少参数\n");
        return ArgResult::Error;
      }
      options.interval_ms = std::atoi(argv[i]);
    } else if (arg == "--port") {
      if (++i >= argc) {
        std::fprintf(stderr, "--port 缺少参数\n");
        return ArgResult::Error;
      }
      options.port = static_cast<std::uint16_t>(std::atoi(argv[i]));
    } else if (arg.size() > 2 && arg.substr(0, 2) == "--") {
      std::fprintf(stderr, "未知选项: %.*s\n", static_cast<int>(arg.size()), arg.data());
      return ArgResult::Error;
    } else {
      positional.emplace_back(arg);
    }
  }

  if (options.server) {
    if (positional.size() == 1) {
      options.port = static_cast<std::uint16_t>(std::atoi(positional[0].c_str()));
    } else if (!positional.empty()) {
      std::fprintf(stderr, "服务器模式不需要位置参数\n");
      return ArgResult::Error;
    }
    if (options.port == 0) {
      std::fprintf(stderr, "端口无效\n");
      return ArgResult::Error;
    }
    return ArgResult::Run;
  }

  if (positional.size() < 2) {
    std::fprintf(stderr, "缺少 host 或 port\n");
    print_usage(argv[0]);
    return ArgResult::Error;
  }
  options.host = positional[0];
  options.port = static_cast<std::uint16_t>(std::atoi(positional[1].c_str()));

  if (options.port == 0 || options.count <= 0 || options.interval_ms < 0) {
    std::fprintf(stderr, "port / count / interval 取值无效\n");
    return ArgResult::Error;
  }
  return ArgResult::Run;
}

// ---------------------------------------------------------------------------
// 延迟统计
// ---------------------------------------------------------------------------

class LatencyStats {
 public:
  void add(std::chrono::microseconds sample) {
    samples_.push_back(static_cast<double>(sample.count()));
  }

  void print(const char* title) const {
    if (samples_.empty()) {
      return;
    }
    std::vector<double> sorted = samples_;
    std::sort(sorted.begin(), sorted.end());
    const double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
    const std::size_t last = sorted.size() - 1;

    std::printf("%s: n=%zu  min=%.0f  avg=%.0f  p50=%.0f  p99=%.0f  max=%.0f  (µs)\n",
                title,
                sorted.size(),
                sorted.front(),
                sum / static_cast<double>(sorted.size()),
                sorted[(last * 50) / 100],
                sorted[(last * 99) / 100],
                sorted.back());
  }

 private:
  std::vector<double> samples_;
};

constexpr milliseconds kTick{100};
constexpr std::chrono::seconds kConnectTimeout{5};

// 可选调优项失败时只告警，不影响主流程（std::expected 本身带 [[nodiscard]]，
// 必须显式消费返回值）
void warn_if_failed(const VoidResult& result, const char* what) {
  if (!result) {
    std::fprintf(stderr, "警告: %s 失败: %s\n", what, result.error().message().c_str());
  }
}

// ---------------------------------------------------------------------------
// TCP 客户端
// ---------------------------------------------------------------------------

class TcpClient final : public std::enable_shared_from_this<TcpClient> {
 public:
  TcpClient(Loop& loop, Options options)
      : loop_(loop),
        options_(std::move(options)),
        resolver_(loop),
        stream_(loop),
        tick_timer_(loop),
        send_timer_(loop),
        connect_timer_(loop) {}

  [[nodiscard]] bool ok() const { return ok_; }

  void start() {
    auto self = shared_from_this();

    // 100ms 轮询中断标志：把“信号上下文”和“循环线程”解耦
    warn_if_failed(tick_timer_.start(kTick, kTick, [self] { self->poll_interrupt(); }),
                   "启动中断轮询定时器");

    std::printf("解析 %s:%u …\n", options_.host.c_str(), static_cast<unsigned>(options_.port));
    resolver_.resolve(options_.host,
                      options_.port,
                      ResolveOptions{.socket_type = SocketType::Stream},
                      [self](Result<std::vector<SocketAddress>> addresses) {
                        if (!addresses) {
                          self->fail("域名解析失败: " + addresses.error().message());
                          return;
                        }
                        if (addresses->empty()) {
                          self->fail("域名解析结果为空");
                          return;
                        }
                        // localhost 这类名字常常同时给出 ::1 和 127.0.0.1，
                        // 生产代码必须按序回退，不能只试第一个
                        self->connect_all(*addresses);
                      });
  }

 private:
  void connect_all(const std::vector<SocketAddress>& addresses) {
    addresses_ = addresses;
    next_address_ = 0;
    connect_next();
  }

  void connect_next() {
    if (closing_) {
      return;
    }
    if (next_address_ >= addresses_.size()) {
      fail("所有地址都连接失败" +
           (last_error_.empty() ? std::string() : ": " + last_error_));
      return;
    }

    const SocketAddress address = addresses_[next_address_++];
    // 每次尝试都用全新句柄，避免复用失败状态的 socket
    stream_ = TcpStream(loop_);

    auto self = shared_from_this();
    warn_if_failed(connect_timer_.start(kConnectTimeout, [self] { self->fail("连接超时"); }),
                   "启动连接超时定时器");

    const auto submitted = stream_.connect(address, [self, address](VoidResult result) {
      self->connect_timer_.stop();
      if (!result) {
        // 在当前回调里直接重建句柄会销毁正在执行的实现对象，
        // 因此把下一次尝试投递到循环队列，等调用栈完全展开后再做
        self->last_error_ = result.error().message();
        std::fprintf(stderr,
                     "尝试 %s 失败: %s\n",
                     address.to_string().c_str(),
                     result.error().message().c_str());
        self->stream_.close();
        self->loop_.post([self] { self->connect_next(); });
        return;
      }
      self->on_connected(address);
    });

    if (!submitted) {
      connect_timer_.stop();
      last_error_ = submitted.error().message();
      stream_.close();
      loop_.post([self = shared_from_this()] { self->connect_next(); });
    }
  }

  void on_connected(const SocketAddress& address) {
    std::printf("已连接 %s\n", address.to_string().c_str());

    // 读窗口必须在第一次 start_read 之前配置。
    // slots 只在 Windows 生效（多 outstanding 读），Linux 忽略。
    if (const auto applied =
            stream_.set_read_options(ReadOptions{.slots = 4, .chunk_size = 32 * 1024});
        !applied) {
      fail("配置读窗口失败: " + applied.error().message());
      return;
    }
    // 小包交互场景关闭 Nagle，避免 40ms 延迟抖动
    warn_if_failed(stream_.set_nodelay(true), "设置 TCP_NODELAY");
    warn_if_failed(
        stream_.set_keepalive(KeepAliveOptions{.enable = true, .delay = std::chrono::seconds(30)}),
        "设置 keepalive");

    if (const auto local = stream_.local_address()) {
      std::printf("本地地址 %s（后端: %s）\n",
                  local->to_string().c_str(),
                  loop_.backend() == Backend::Iocp ? "IOCP" : "epoll");
    }

    start_reading();
    send_next();
  }

  void start_reading() {
    auto self = shared_from_this();

    // 低开销视图读：不构造 keepalive，数据只在回调期间有效。
    // 需要把数据转发出去时请改用 start_read(ReadCallback) 并转交 keepalive。
    const auto started = stream_.start_read([self](Result<std::span<const std::byte>> chunk) {
      if (!chunk) {
        if (chunk.error().code == ErrorCode::Canceled) {
          return;  // 我们自己 close 引起的，不需要报错
        }
        if (chunk.error().code == ErrorCode::EndOfFile) {
          self->fail("对端关闭连接");
          return;
        }
        self->fail("读取失败: " + chunk.error().message());
        return;
      }
      self->on_bytes(*chunk);
    });
    if (!started) {
      fail("启动读失败: " + started.error().message());
    }
  }

  void on_bytes(std::span<const std::byte> data) {
    // 一次读可能拿到半行或多行，这里按 '\n' 切分
    buffer_.append(reinterpret_cast<const char*>(data.data()), data.size());

    std::size_t newline = 0;
    while ((newline = buffer_.find('\n')) != std::string::npos) {
      std::string line = buffer_.substr(0, newline);
      buffer_.erase(0, newline + 1);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      on_line(line);
      if (closing_) {
        return;
      }
    }
  }

  void on_line(const std::string& line) {
    const auto rtt = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - sent_at_);
    stats_.add(rtt);
    ++completed_;
    std::printf("[%d/%d] 收到 \"%s\"  RTT %.0f µs\n",
                completed_,
                options_.count,
                line.c_str(),
                static_cast<double>(rtt.count()));

    if (completed_ >= options_.count) {
      finish();
      return;
    }

    // 间隔等待用一次性定时器，避免在回调里递归发下一包
    auto self = shared_from_this();
    warn_if_failed(send_timer_.start(milliseconds(options_.interval_ms),
                                     [self] { self->send_next(); }),
                   "启动发送间隔定时器");
  }

  void send_next() {
    if (closing_) {
      return;
    }

    auto payload = std::make_shared<std::string>("PING " + std::to_string(sequence_++) + "\n");
    sent_at_ = Clock::now();

    // 零拷贝写：库不复制数据，payload 的 shared_ptr 作为 keepalive，
    // 保证到写完成回调返回前字符串仍然存活。
    const auto bytes = std::as_bytes(std::span<const char>(payload->data(), payload->size()));
    auto self = shared_from_this();
    const auto submitted = stream_.write(WriteBuffer{bytes, payload}, [self](VoidResult result) {
      if (!result && result.error().code != ErrorCode::Canceled) {
        self->fail("写入失败: " + result.error().message());
      }
    });
    if (!submitted) {
      fail("发起写入失败: " + submitted.error().message());
    }
  }

  void finish() {
    if (closing_) {
      return;
    }
    closing_ = true;
    ok_ = true;

    stats_.print("RTT 统计");
    std::printf("握手完成后正常退出，正在优雅关闭…\n");

    auto self = shared_from_this();
    // 优雅关闭：先等已排队的写落地，再半关闭，最后释放句柄
    const auto submitted = stream_.shutdown([self](VoidResult) {
      self->stream_.close();
      self->stop_timers();
      self->loop_.stop();
    });
    if (!submitted) {
      stream_.close();
      stop_timers();
      loop_.stop();
    }
  }

  void poll_interrupt() {
    if (closing_ || !platform::g_stop_requested.load(std::memory_order_relaxed)) {
      return;
    }
    std::printf("\n收到中断信号，取消未完成操作并退出\n");
    abort_close();
  }

  void fail(const std::string& reason) {
    if (closing_) {
      return;
    }
    std::fprintf(stderr, "错误: %s\n", reason.c_str());
    abort_close();
  }

  void abort_close() {
    closing_ = true;
    ok_ = false;
    stream_.close();
    stop_timers();
    loop_.stop();
  }

  void stop_timers() {
    tick_timer_.stop();
    send_timer_.stop();
    connect_timer_.stop();
  }

  Loop& loop_;
  Options options_;
  Resolver resolver_;
  TcpStream stream_;
  Timer tick_timer_;
  Timer send_timer_;
  Timer connect_timer_;

  LatencyStats stats_;
  std::string buffer_;
  std::vector<SocketAddress> addresses_;
  std::size_t next_address_ = 0;
  std::string last_error_;
  Clock::time_point sent_at_{};
  std::uint64_t sequence_ = 0;
  int completed_ = 0;
  bool closing_ = false;
  bool ok_ = false;
};

// ---------------------------------------------------------------------------
// UDP 客户端
// ---------------------------------------------------------------------------

class UdpClient final : public std::enable_shared_from_this<UdpClient> {
 public:
  UdpClient(Loop& loop, Options options)
      : loop_(loop),
        options_(std::move(options)),
        resolver_(loop),
        tick_timer_(loop),
        send_timer_(loop) {}

  [[nodiscard]] bool ok() const { return ok_; }

  void start() {
    auto self = shared_from_this();
    warn_if_failed(tick_timer_.start(kTick, kTick, [self] { self->poll_interrupt(); }),
                   "启动中断轮询定时器");

    std::printf("解析 %s:%u …\n", options_.host.c_str(), static_cast<unsigned>(options_.port));
    resolver_.resolve(options_.host,
                      options_.port,
                      ResolveOptions{.socket_type = SocketType::Datagram},
                      [self](Result<std::vector<SocketAddress>> addresses) {
                        if (!addresses) {
                          self->fail("域名解析失败: " + addresses.error().message());
                          return;
                        }
                        if (addresses->empty()) {
                          self->fail("域名解析结果为空");
                          return;
                        }
                        self->open(addresses->front());
                      });
  }

 private:
  void open(const SocketAddress& target) {
    target_ = target;

    // UDP 必须按目标地址的协议族创建 socket：解析出 IPv6 就行走 IPv6
    socket_ = std::make_unique<UdpSocket>(loop_, target.family());
    if (const auto bound = socket_->bind(SocketAddress::any(target.family(), 0)); !bound) {
      fail("绑定本地端口失败: " + bound.error().message());
      return;
    }
    if (const auto applied =
            socket_->set_recv_options(UdpRecvOptions{.slots = 4, .chunk_size = 8 * 1024});
        !applied) {
      fail("配置收包窗口失败: " + applied.error().message());
      return;
    }

    auto self = shared_from_this();
    // 低开销视图收包：每个 datagram 单独回调，数据只在回调期间有效
    const auto started = socket_->start_recv([self](Result<DatagramView> datagram) {
      if (!datagram) {
        if (datagram.error().code == ErrorCode::Canceled) {
          return;
        }
        self->fail("接收失败: " + datagram.error().message());
        return;
      }
      self->on_datagram(*datagram);
    });
    if (!started) {
      fail("启动收包失败: " + started.error().message());
      return;
    }

    if (const auto local = socket_->local_address()) {
      std::printf("本地地址 %s，目标 %s（后端: %s）\n",
                  local->to_string().c_str(),
                  target_.to_string().c_str(),
                  loop_.backend() == Backend::Iocp ? "IOCP" : "epoll");
    }

    send_next();
  }

  void on_datagram(const DatagramView& datagram) {
    const auto rtt = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - sent_at_);
    stats_.add(rtt);
    ++completed_;
    std::printf("[%d/%d] <- %s \"%.*s\"  RTT %.0f µs\n",
                completed_,
                options_.count,
                datagram.source.to_string().c_str(),
                static_cast<int>(datagram.data.size()),
                reinterpret_cast<const char*>(datagram.data.data()),
                static_cast<double>(rtt.count()));

    if (completed_ >= options_.count) {
      finish();
      return;
    }

    auto self = shared_from_this();
    warn_if_failed(send_timer_.start(milliseconds(options_.interval_ms),
                                     [self] { self->send_next(); }),
                   "启动发送间隔定时器");
  }

  void send_next() {
    if (closing_) {
      return;
    }

    auto payload = std::make_shared<std::string>("PING " + std::to_string(sequence_++));
    sent_at_ = Clock::now();

    const auto bytes = std::as_bytes(std::span<const char>(payload->data(), payload->size()));
    auto self = shared_from_this();
    const auto submitted = socket_->send_to(target_,
                                            WriteBuffer{bytes, payload},
                                            [self](VoidResult result) {
                                              if (!result &&
                                                  result.error().code != ErrorCode::Canceled) {
                                                self->fail("发送失败: " +
                                                           result.error().message());
                                              }
                                            });
    if (!submitted) {
      fail("发起发送失败: " + submitted.error().message());
    }
  }

  void finish() {
    if (closing_) {
      return;
    }
    closing_ = true;
    ok_ = true;

    stats_.print("RTT 统计");
    std::printf("收发完成，正在关闭…\n");
    close_and_stop();
  }

  void poll_interrupt() {
    if (closing_ || !platform::g_stop_requested.load(std::memory_order_relaxed)) {
      return;
    }
    std::printf("\n收到中断信号，关闭并退出\n");
    abort_close();
  }

  void fail(const std::string& reason) {
    if (closing_) {
      return;
    }
    std::fprintf(stderr, "错误: %s\n", reason.c_str());
    abort_close();
  }

  void abort_close() {
    closing_ = true;
    ok_ = false;
    close_and_stop();
  }

  void close_and_stop() {
    if (socket_) {
      socket_->stop_recv();
      socket_->close();
    }
    tick_timer_.stop();
    send_timer_.stop();
    loop_.stop();
  }

  Loop& loop_;
  Options options_;
  Resolver resolver_;
  std::unique_ptr<UdpSocket> socket_;
  Timer tick_timer_;
  Timer send_timer_;

  SocketAddress target_;
  LatencyStats stats_;
  Clock::time_point sent_at_{};
  std::uint64_t sequence_ = 0;
  int completed_ = 0;
  bool closing_ = false;
  bool ok_ = false;
};

// ---------------------------------------------------------------------------
// 回显服务器（本地联调 / 演示零拷贝转发）
// ---------------------------------------------------------------------------

class EchoConnection final : public std::enable_shared_from_this<EchoConnection> {
 public:
  explicit EchoConnection(TcpStream stream) : stream_(std::move(stream)) {}

  void start() {
    warn_if_failed(stream_.set_read_options(ReadOptions{.slots = 4, .chunk_size = 32 * 1024}),
                   "配置读窗口");

    auto self = shared_from_this();
    // 零拷贝读 + 零拷贝写：把读块的 keepalive 直接交给写，
    // 数据在用户态一次都不复制。
    const auto started = stream_.start_read([self](Result<ReadChunk> chunk) {
      if (!chunk) {
        self->stream_.close();  // close 会清空回调，打破自引用环
        return;
      }
      const auto submitted = self->stream_.write(
          WriteBuffer{chunk->data, chunk->keepalive}, [self](VoidResult result) {
            if (!result && result.error().code != ErrorCode::Canceled) {
              self->stream_.close();
            }
          });
      if (!submitted) {
        self->stream_.close();
      }
    });
    if (!started) {
      stream_.close();
    }
  }

 private:
  TcpStream stream_;
};

class EchoServer final : public std::enable_shared_from_this<EchoServer> {
 public:
  EchoServer(Loop& loop, Options options)
      : loop_(loop),
        options_(std::move(options)),
        listener_(loop),
        tick_timer_(loop) {}

  void start() {
    auto self = shared_from_this();
    warn_if_failed(tick_timer_.start(kTick, kTick, [self] {
      if (!platform::g_stop_requested.load(std::memory_order_relaxed)) {
        return;
      }
      std::printf("\n正在退出…\n");
      self->listener_.close();
      if (self->socket_) {
        self->socket_->stop_recv();
        self->socket_->close();
      }
      self->tick_timer_.stop();
      self->loop_.stop();
    }),
                   "启动中断轮询定时器");

    if (options_.udp) {
      start_udp();
    } else {
      start_tcp();
    }
  }

 private:
  void start_tcp() {
    // reuse_address：重启服务器时不用等 TIME_WAIT 结束
    const auto bound = listener_.bind(SocketAddress::any(Family::IPv4, options_.port),
                                      BindOptions{.reuse_address = true});
    if (!bound) {
      std::fprintf(stderr, "监听失败: %s\n", bound.error().message().c_str());
      loop_.stop();
      return;
    }
    if (const auto address = listener_.local_address()) {
      std::printf("TCP 回显服务器监听 %s\n", address->to_string().c_str());
    }

    auto self = shared_from_this();
    const auto listening = listener_.listen(64, [self](Result<TcpStream> accepted) {
      if (!accepted) {
        if (accepted.error().code == ErrorCode::Canceled) {
          return;  // 监听器被主动关闭
        }
        std::fprintf(stderr, "accept 失败: %s\n", accepted.error().message().c_str());
        return;
      }
      if (const auto peer = accepted->peer_address()) {
        std::printf("接受连接 %s\n", peer->to_string().c_str());
      }
      auto connection = std::make_shared<EchoConnection>(std::move(*accepted));
      connection->start();
    });
    if (!listening) {
      std::fprintf(stderr, "listen 失败: %s\n", listening.error().message().c_str());
      listener_.close();
      loop_.stop();
    }
  }

  void start_udp() {
    socket_ = std::make_unique<UdpSocket>(loop_, Family::IPv4);
    if (const auto bound = socket_->bind(SocketAddress::any(Family::IPv4, options_.port));
        !bound) {
      std::fprintf(stderr, "监听失败: %s\n", bound.error().message().c_str());
      socket_.reset();
      loop_.stop();
      return;
    }
    if (const auto address = socket_->local_address()) {
      std::printf("UDP 回显服务器监听 %s\n", address->to_string().c_str());
    }

    auto self = shared_from_this();
    // 零拷贝收 + 零拷贝发：Datagram 的 keepalive 直接转交 send_to
    const auto started = socket_->start_recv([self](Result<Datagram> datagram) {
      if (!datagram) {
        if (datagram.error().code == ErrorCode::Canceled) {
          return;
        }
        std::fprintf(stderr, "接收失败: %s\n", datagram.error().message().c_str());
        return;
      }
      if (const auto submitted = self->socket_->send_to(
              datagram->source, WriteBuffer{datagram->data, datagram->keepalive});
          !submitted) {
        // 目标端口已关闭时发送会失败，UDP 语义下直接丢弃即可
      }
    });
    if (!started) {
      std::fprintf(stderr, "启动收包失败: %s\n", started.error().message().c_str());
      socket_->close();
      socket_.reset();
      loop_.stop();
    }
  }

  Loop& loop_;
  Options options_;
  TcpListener listener_;
  std::unique_ptr<UdpSocket> socket_;
  Timer tick_timer_;
};

} // namespace

// ---------------------------------------------------------------------------
// 入口
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
  platform::init_console();

  Options options;
  switch (parse_options(argc, argv, options)) {
    case ArgResult::Exit:
      return 0;
    case ArgResult::Error:
      return 2;
    case ArgResult::Run:
      break;
  }

  platform::install_stop_handler();

  Loop loop;
  std::printf("uvnet 示例 | 后端: %s | 协议: %s\n",
              loop.backend() == Backend::Iocp ? "IOCP (Windows)" : "epoll (Linux)",
              options.udp ? "UDP" : "TCP");

  if (options.server) {
    auto server = std::make_shared<EchoServer>(loop, options);
    server->start();
    loop.run();
    return 0;
  }

  if (options.udp) {
    auto client = std::make_shared<UdpClient>(loop, options);
    client->start();
    loop.run();
    return client->ok() ? 0 : 1;
  }

  auto client = std::make_shared<TcpClient>(loop, options);
  client->start();
  loop.run();
  return client->ok() ? 0 : 1;
}
