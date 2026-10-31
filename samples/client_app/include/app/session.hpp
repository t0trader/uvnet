#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "app/config.hpp"
#include "app/protocol.hpp"
#include "uvnet/uvnet.hpp"

namespace app {

// 客户端会话：把一次"连上去问几轮再退出"的完整生命周期收敛到一个对象里。
//
//   async DNS -> 候选地址逐个回退连接 -> 请求/响应 -> 超时重连 -> 优雅关闭
//
// 线程模型：所有方法都必须在 uvnet::Loop 的线程上调用。
// 生命周期：调用方持有 shared_ptr，回调里通过 shared_from_this() 保活；
// 结束时 Loop::stop()，run() 返回后即可查询结果。
class ClientSession final : public std::enable_shared_from_this<ClientSession> {
 public:
  ClientSession(uvnet::Loop& loop, Config config);
  ~ClientSession();

  ClientSession(const ClientSession&) = delete;
  ClientSession& operator=(const ClientSession&) = delete;

  void start();

  // 主动中断（例如 Ctrl+C）：取消在途操作，不计为成功
  void stop(const std::string& reason = "被调用方中断");

  [[nodiscard]] bool active() const { return active_; }
  [[nodiscard]] bool done() const { return done_; }
  [[nodiscard]] bool succeeded() const { return succeeded_; }
  [[nodiscard]] int completed_requests() const { return completed_; }
  [[nodiscard]] const Config& config() const { return config_; }

 private:
  // 解析 + 建立连接
  void resolve_and_connect(int attempt);
  void try_next_address();
  void connect_tcp(const uvnet::SocketAddress& address);
  void open_udp(const uvnet::SocketAddress& address);
  void on_connected(const uvnet::SocketAddress& address);
  void start_tcp_read();

  // 收发
  void send_next();
  void on_line(const std::string& line);
  void on_response();
  void on_request_timeout();

  // 失败处理
  void connection_lost(const std::string& reason);
  void schedule_retry(const std::string& reason);
  void fail(const std::string& reason);
  void finish_success();
  void teardown_udp(bool defer);
  void stop_timers();
  void notify_done();

  uvnet::Loop& loop_;
  Config config_;

  uvnet::Resolver resolver_;
  uvnet::TcpStream stream_;                  // TCP 使用
  std::unique_ptr<uvnet::UdpSocket> socket_; // UDP 使用（按解析出的协议族创建）

  uvnet::Timer connect_timer_;
  uvnet::Timer request_timer_;
  uvnet::Timer send_timer_;
  uvnet::Timer backoff_timer_;

  LineDecoder decoder_;
  std::vector<uvnet::SocketAddress> addresses_;
  std::size_t next_address_ = 0;
  uvnet::SocketAddress address_{};

  std::vector<double> rtt_samples_us_;
  std::chrono::steady_clock::time_point sent_at_{};
  std::uint64_t sequence_ = 0;
  std::uint64_t pending_sequence_ = 0;
  int completed_ = 0;
  int attempt_ = 0;

  bool started_ = false;
  bool active_ = false;
  bool in_flight_ = false;
  bool done_ = false;
  bool succeeded_ = false;
};

} // namespace app
