// 本地联调用的回显服务器入口：echo_server [--udp] [--port N] [--log-level LEVEL]

#include "app/echo_server.hpp"
#include "app/logger.hpp"
#include "app/platform.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

#include "uvnet/uvnet.hpp"

namespace {

void print_usage(const char* program) {
  std::printf(
      "用法: %s [选项]\n"
      "\n选项:\n"
      "  --udp                UDP 回显（默认 TCP）\n"
      "  --port N             监听端口（默认 9000，0 表示随机）\n"
      "  --log-level LEVEL    debug / info / warn / error（默认 info）\n"
      "  --help               显示本帮助\n",
      program);
}

} // namespace

int main(int argc, char** argv) {
  app::platform::init_console();

  app::Protocol protocol = app::Protocol::Tcp;
  std::uint16_t port = 9000;
  app::LogLevel level = app::LogLevel::Info;

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];

    if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      return 0;
    }
    if (arg == "--udp") {
      protocol = app::Protocol::Udp;
      continue;
    }
    if (arg == "--port") {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "--port 缺少参数\n");
        return 2;
      }
      const int value = std::atoi(argv[++i]);
      if (value < 0 || value > 65535) {
        std::fprintf(stderr, "端口无效: %s\n", argv[i]);
        return 2;
      }
      port = static_cast<std::uint16_t>(value);
      continue;
    }
    if (arg == "--log-level") {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "--log-level 缺少参数\n");
        return 2;
      }
      ++i;
      if (!app::parse_log_level(argv[i], level)) {
        std::fprintf(stderr, "未知日志级别: %s\n", argv[i]);
        return 2;
      }
      continue;
    }

    std::fprintf(stderr, "未知参数: %s\n", argv[i]);
    return 2;
  }

  app::set_log_level(level);
  app::platform::install_stop_handler();

  uvnet::Loop loop;
  app::EchoServer server(loop, protocol, port);

  std::string error;
  if (!server.start(error)) {
    app::log_error("启动失败: " + error);
    return 1;
  }

  if (const auto address = server.local_address()) {
    app::log_info(std::string("监听 ") + address->to_string() +
                  (protocol == app::Protocol::Udp ? " (UDP)" : " (TCP)"));
  }

  uvnet::Timer interrupt_timer(loop);
  if (const auto started = interrupt_timer.start(
          std::chrono::milliseconds(100),
          std::chrono::milliseconds(100),
          [&] {
            if (!app::platform::stop_requested()) {
              return;
            }
            app::log_info("正在退出");
            interrupt_timer.stop();
            server.close();
            loop.stop();
          });
      !started) {
    app::log_error("启动中断轮询定时器失败: " + started.error().message());
    return 1;
  }

  loop.run();
  return 0;
}
