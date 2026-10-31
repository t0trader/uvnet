#include "app/session.hpp"

#include "app/logger.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <numeric>
#include <span>
#include <utility>
#include <vector>

namespace app {

namespace {

using Clock = std::chrono::steady_clock;

using uvnet::ErrorCode;
using uvnet::ResolveOptions;
using uvnet::Result;
using uvnet::SocketAddress;
using uvnet::SocketType;
using uvnet::UdpRecvOptions;
using uvnet::VoidResult;
using uvnet::WriteBuffer;

constexpr std::chrono::milliseconds kRetryBase{200};

std::string endpoint_text(const Config& config) {
  std::string text = config.host + ":" + std::to_string(config.port);
  text += config.protocol == Protocol::Udp ? " (UDP)" : " (TCP)";
  return text;
}

std::string latency_summary(const std::vector<double>& samples) {
  if (samples.empty()) {
    return "无样本";
  }

  std::vector<double> sorted = samples;
  std::sort(sorted.begin(), sorted.end());
  const double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
  const std::size_t last = sorted.size() - 1;

  char buffer[176];
  std::snprintf(buffer,
                sizeof(buffer),
                "n=%zu min=%.0f avg=%.0f p50=%.0f p99=%.0f max=%.0f (µs)",
                sorted.size(),
                sorted.front(),
                sum / static_cast<double>(sorted.size()),
                sorted[(last * 50) / 100],
                sorted[(last * 99) / 100],
                sorted.back());
  return buffer;
}

} // namespace

ClientSession::ClientSession(uvnet::Loop& loop, Config config)
    : loop_(loop),
      config_(std::move(config)),
      resolver_(loop),
      stream_(loop),
      connect_timer_(loop),
      request_timer_(loop),
      send_timer_(loop),
      backoff_timer_(loop) {}

ClientSession::~ClientSession() {
  stop_timers();
}

// ---------------------------------------------------------------------------
// 启动与连接
// ---------------------------------------------------------------------------

void ClientSession::start() {
  if (started_ || done_) {
    return;
  }
  started_ = true;
  log_info("连接 " + endpoint_text(config_) + "，共 " + std::to_string(config_.requests) + " 轮请求");
  resolve_and_connect(0);
}

void ClientSession::resolve_and_connect(int attempt) {
  if (done_) {
    return;
  }
  attempt_ = attempt;
  log_debug("开始解析 " + config_.host + "（尝试 " + std::to_string(attempt + 1) + "）");

  ResolveOptions options;
  options.socket_type =
      config_.protocol == Protocol::Udp ? SocketType::Datagram : SocketType::Stream;

  auto self = shared_from_this();
  resolver_.resolve(config_.host,
                    config_.port,
                    options,
                    [self](Result<std::vector<SocketAddress>> addresses) {
                      if (self->done_) {
                        return;
                      }
                      if (!addresses) {
                        self->schedule_retry("域名解析失败: " + addresses.error().message());
                        return;
                      }
                      if (addresses->empty()) {
                        self->schedule_retry("域名解析结果为空");
                        return;
                      }
                      self->addresses_ = std::move(*addresses);
                      self->next_address_ = 0;
                      self->try_next_address();
                    });
}

void ClientSession::try_next_address() {
  if (done_) {
    return;
  }
  if (next_address_ >= addresses_.size()) {
    schedule_retry("所有候选地址都连接失败");
    return;
  }

  const SocketAddress address = addresses_[next_address_++];
  if (config_.protocol == Protocol::Udp) {
    open_udp(address);
  } else {
    connect_tcp(address);
  }
}

void ClientSession::connect_tcp(const SocketAddress& address) {
  // 每次尝试都用新句柄：连接失败后的 socket 状态不可复用
  stream_ = uvnet::TcpStream(loop_);

  auto self = shared_from_this();
  if (const auto armed = connect_timer_.start(
          std::chrono::milliseconds(config_.connect_timeout_ms),
          [self, address] {
            if (self->done_) {
              return;
            }
            log_warn("连接 " + address.to_string() + " 超时");
            self->stream_.close();
            // 在定时器回调里重建句柄是安全的，但统一切到队列尾部更省心
            self->loop_.post([self] { self->try_next_address(); });
          });
      !armed) {
    log_warn("启动连接超时定时器失败: " + armed.error().message());
  }

  const auto submitted = stream_.connect(address, [self, address](VoidResult result) {
    self->connect_timer_.stop();
    if (self->done_) {
      return;
    }
    if (!result) {
      log_debug("连接 " + address.to_string() + " 失败: " + result.error().message());
      self->stream_.close();
      // 不能在连接回调里直接重建 stream_（会析构正在执行的实现对象）
      self->loop_.post([self] { self->try_next_address(); });
      return;
    }
    self->on_connected(address);
  });

  if (!submitted) {
    connect_timer_.stop();
    log_debug("发起连接失败: " + submitted.error().message());
    stream_.close();
    loop_.post([self] { self->try_next_address(); });
  }
}

void ClientSession::open_udp(const SocketAddress& address) {
  address_ = address;
  socket_ = std::make_unique<uvnet::UdpSocket>(loop_, address.family());

  if (const auto bound = socket_->bind(SocketAddress::any(address.family(), 0)); !bound) {
    fail("绑定本地端口失败: " + bound.error().message());
    return;
  }

  UdpRecvOptions options;
  options.slots = config_.read_slots;
  options.chunk_size = config_.read_chunk;
  if (const auto applied = socket_->set_recv_options(options); !applied) {
    fail("配置收包窗口失败: " + applied.error().message());
    return;
  }

  auto self = shared_from_this();
  const auto started = socket_->start_recv([self](Result<uvnet::DatagramView> datagram) {
    if (self->done_) {
      return;
    }
    if (!datagram) {
      if (datagram.error().code == ErrorCode::Canceled) {
        return;
      }
      const std::string reason = "接收失败: " + datagram.error().message();
      // socket 的停用推迟到队列尾部，避免在它自己的回调栈里析构实现对象
      self->loop_.post([self, reason] { self->connection_lost(reason); });
      return;
    }
    const std::string line(reinterpret_cast<const char*>(datagram->data.data()),
                           datagram->data.size());
    self->on_line(line);
  });
  if (!started) {
    fail("启动收包失败: " + started.error().message());
    return;
  }

  std::string local = "?";
  if (const auto local_address = socket_->local_address()) {
    local = local_address->to_string();
  }
  log_info("UDP 目标 " + address.to_string() + "（本地 " + local + "）");

  active_ = true;
  send_next();
}

void ClientSession::on_connected(const SocketAddress& address) {
  address_ = address;

  if (config_.protocol == Protocol::Udp) {
    open_udp(address);
    return;
  }

  uvnet::ReadOptions read_options;
  read_options.slots = config_.read_slots;
  read_options.chunk_size = config_.read_chunk;
  if (const auto applied = stream_.set_read_options(read_options); !applied) {
    fail("配置读窗口失败: " + applied.error().message());
    return;
  }
  if (const auto applied = stream_.set_nodelay(true); !applied) {
    log_debug("设置 TCP_NODELAY 失败: " + applied.error().message());
  }
  if (const auto applied = stream_.set_keepalive(
          uvnet::KeepAliveOptions{.enable = true, .delay = std::chrono::seconds(30)});
      !applied) {
    log_debug("设置 keepalive 失败: " + applied.error().message());
  }

  std::string local = "?";
  if (const auto local_address = stream_.local_address()) {
    local = local_address->to_string();
  }
  log_info("已连接 " + address.to_string() + "（本地 " + local + "）");

  active_ = true;
  start_tcp_read();
  send_next();
}

void ClientSession::start_tcp_read() {
  auto self = shared_from_this();
  const auto started =
      stream_.start_read([self](Result<std::span<const std::byte>> chunk) {
        if (self->done_) {
          return;
        }
        if (!chunk) {
          if (chunk.error().code == ErrorCode::Canceled) {
            return;
          }
          self->connection_lost(chunk.error().code == ErrorCode::EndOfFile
                                    ? "对端关闭连接"
                                    : "读取失败: " + chunk.error().message());
          return;
        }
        self->decoder_.feed(*chunk, [self](const std::string& line) { self->on_line(line); });
      });
  if (!started) {
    fail("启动读失败: " + started.error().message());
  }
}

// ---------------------------------------------------------------------------
// 收发
// ---------------------------------------------------------------------------

void ClientSession::send_next() {
  if (done_ || in_flight_) {
    return;
  }
  if (completed_ >= config_.requests) {
    return;
  }

  const std::uint64_t sequence = sequence_++;
  pending_sequence_ = sequence;
  in_flight_ = true;
  sent_at_ = Clock::now();

  auto payload = std::make_shared<std::string>(encode_message(MessageType::Ping, sequence));
  const auto bytes = std::as_bytes(std::span<const char>(payload->data(), payload->size()));

  auto self = shared_from_this();
  if (const auto armed = request_timer_.start(
          std::chrono::milliseconds(config_.request_timeout_ms),
          [self] { self->on_request_timeout(); });
      !armed) {
    log_warn("启动请求超时定时器失败: " + armed.error().message());
  }

  const auto on_write = [self](VoidResult result) {
    if (!result && result.error().code != ErrorCode::Canceled) {
      self->connection_lost("写入失败: " + result.error().message());
    }
  };

  if (config_.protocol == Protocol::Udp) {
    if (!socket_) {
      fail("UDP socket 未就绪");
      return;
    }
    // 零拷贝发送：payload 的 shared_ptr 作为 keepalive
    if (const auto submitted = socket_->send_to(address_, WriteBuffer{bytes, payload},
                                                on_write);
        !submitted) {
      connection_lost("发起发送失败: " + submitted.error().message());
    }
    return;
  }

  if (const auto submitted = stream_.write(WriteBuffer{bytes, payload}, on_write); !submitted) {
    connection_lost("发起写入失败: " + submitted.error().message());
  }
}

void ClientSession::on_line(const std::string& line) {
  if (done_) {
    return;
  }

  const auto message = decode_message(line);
  if (!message) {
    log_warn("忽略无法解析的服务端消息: " + message.error());
    return;
  }
  if (message->type != MessageType::Pong) {
    log_warn("忽略非 PONG 消息");
    return;
  }
  if (!in_flight_) {
    log_warn("收到未请求的 PONG " + std::to_string(message->sequence));
    return;
  }
  if (message->sequence != pending_sequence_) {
    log_warn("PONG 序号不匹配（期望 " + std::to_string(pending_sequence_) + "，收到 " +
             std::to_string(message->sequence) + "）");
    return;
  }

  on_response();
}

void ClientSession::on_response() {
  request_timer_.stop();
  in_flight_ = false;
  attempt_ = 0; // 有成功交互后清除重试计数

  const auto rtt =
      std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - sent_at_);
  rtt_samples_us_.push_back(static_cast<double>(rtt.count()));
  ++completed_;

  {
    char buffer[128];
    std::snprintf(buffer,
                  sizeof(buffer),
                  "第 %d/%d 轮响应 RTT %.0f µs",
                  completed_,
                  config_.requests,
                  static_cast<double>(rtt.count()));
    log_debug(buffer);
  }

  if (completed_ >= config_.requests) {
    finish_success();
    return;
  }

  auto self = shared_from_this();
  if (const auto armed = send_timer_.start(
          std::chrono::milliseconds(config_.interval_ms),
          [self] { self->send_next(); });
      !armed) {
    log_warn("启动发送间隔定时器失败: " + armed.error().message());
  }
}

void ClientSession::on_request_timeout() {
  if (done_ || !in_flight_) {
    return;
  }
  in_flight_ = false;
  connection_lost("第 " + std::to_string(pending_sequence_) + " 轮请求超时（" +
                  std::to_string(config_.request_timeout_ms) + "ms）");
}

// ---------------------------------------------------------------------------
// 失败处理与收尾
// ---------------------------------------------------------------------------

void ClientSession::connection_lost(const std::string& reason) {
  if (done_) {
    return;
  }
  log_warn(reason + "，连接不可用");

  stop_timers();
  active_ = false;
  in_flight_ = false;
  decoder_.reset();
  stream_.close();
  teardown_udp(true);
  schedule_retry(reason);
}

void ClientSession::schedule_retry(const std::string& reason) {
  if (done_) {
    return;
  }
  if (attempt_ >= config_.max_retries) {
    fail(reason + "（已重试 " + std::to_string(attempt_) + " 次）");
    return;
  }

  ++attempt_;
  const auto delay = std::chrono::milliseconds(kRetryBase.count() * attempt_);
  log_warn(reason + "，" + std::to_string(delay.count()) + "ms 后重试（" +
           std::to_string(attempt_) + "/" + std::to_string(config_.max_retries) + "）");

  auto self = shared_from_this();
  if (const auto armed =
          backoff_timer_.start(delay, [self] { self->resolve_and_connect(self->attempt_); });
      !armed) {
    log_warn("启动重试定时器失败: " + armed.error().message());
    fail("无法启动重试定时器");
  }
}

void ClientSession::fail(const std::string& reason) {
  if (done_) {
    return;
  }
  done_ = true;
  succeeded_ = false;

  stop_timers();
  stream_.close();
  teardown_udp(false);

  log_error("失败: " + reason + "（已完成 " + std::to_string(completed_) + "/" +
            std::to_string(config_.requests) + " 轮）");
  if (!rtt_samples_us_.empty()) {
    log_info("RTT " + latency_summary(rtt_samples_us_));
  }
  notify_done();
}

void ClientSession::finish_success() {
  if (done_) {
    return;
  }
  done_ = true;
  succeeded_ = true;

  stop_timers();
  log_info("完成 " + std::to_string(completed_) + " 轮请求，RTT " +
           latency_summary(rtt_samples_us_));

  if (config_.protocol == Protocol::Udp) {
    teardown_udp(false);
    notify_done();
    return;
  }

  // 优雅关闭：等已排队的写落地，再半关闭，最后释放句柄
  auto self = shared_from_this();
  const auto submitted = stream_.shutdown([self](VoidResult) {
    self->stream_.close();
    self->notify_done();
  });
  if (!submitted) {
    stream_.close();
    notify_done();
  }
}

void ClientSession::stop(const std::string& reason) {
  if (done_) {
    return;
  }
  done_ = true;
  succeeded_ = false;

  stop_timers();
  stream_.close();
  teardown_udp(false);

  log_warn("停止: " + reason);
  notify_done();
}

void ClientSession::teardown_udp(bool defer) {
  if (!socket_) {
    return;
  }

  // 用 shared_ptr 持有句柄：post 需要可拷贝的调用对象
  auto socket = std::shared_ptr<uvnet::UdpSocket>(std::move(socket_));
  if (defer) {
    // 在 UDP 回调栈里不能立刻析构实现对象，挪到队列尾部
    loop_.post([socket] {
      socket->stop_recv();
      socket->close();
    });
    return;
  }
  socket->stop_recv();
  socket->close();
}

void ClientSession::stop_timers() {
  connect_timer_.stop();
  request_timer_.stop();
  send_timer_.stop();
  backoff_timer_.stop();
}

void ClientSession::notify_done() {
  active_ = false;
  in_flight_ = false;
  loop_.stop();
}

} // namespace app
