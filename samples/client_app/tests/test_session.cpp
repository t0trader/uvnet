// 会话层端到端测试：同进程内起回显服务器 + 客户端会话

#include "app/echo_server.hpp"
#include "app/session.hpp"

#include <cstdint>
#include <memory>
#include <string>

#include "test_support.hpp"

namespace {

using namespace app;
using namespace uvnet;

struct Outcome {
  bool ok = false;
  int completed = 0;
  bool has_address = false;
};

Outcome run_case(Protocol protocol, const std::string& host, int requests) {
  Loop loop;
  apptest::Guard guard(loop, std::chrono::seconds(15));

  EchoServer server(loop, protocol, 0);
  std::string error;
  if (!server.start(error)) {
    std::fprintf(stderr, "服务器启动失败: %s\n", error.c_str());
    CHECK(false);
    return {};
  }

  const auto address = server.local_address();
  CHECK(address.has_value());
  if (!address) {
    return {};
  }

  Config config;
  config.host = host;
  config.port = address->port();
  config.protocol = protocol;
  config.requests = requests;
  config.interval_ms = 1;
  config.request_timeout_ms = 800;
  config.connect_timeout_ms = 800;
  config.max_retries = 1;
  config.log_level = LogLevel::Warn;

  auto session = std::make_shared<ClientSession>(loop, config);
  session->start();
  loop.run();

  Outcome outcome;
  outcome.ok = session->succeeded();
  outcome.completed = session->completed_requests();
  outcome.has_address = true;

  server.close();
  guard.cancel();
  return outcome;
}

void check_case(const char* name, Protocol protocol, const std::string& host, int requests) {
  const Outcome outcome = run_case(protocol, host, requests);
  if (!outcome.ok || outcome.completed != requests) {
    std::fprintf(stderr,
                 "%s: ok=%d completed=%d (期望 %d)\n",
                 name,
                 static_cast<int>(outcome.ok),
                 outcome.completed,
                 requests);
    CHECK(false);
    return;
  }
  CHECK(true);
}

} // namespace

int main() {
  check_case("TCP 回环", Protocol::Tcp, "127.0.0.1", 3);
  check_case("TCP 域名（多地址回退）", Protocol::Tcp, "localhost", 3);
  check_case("UDP 回环", Protocol::Udp, "127.0.0.1", 3);
  return apptest::finish("session");
}
