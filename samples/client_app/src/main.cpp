#include "app/config.hpp"
#include "app/logger.hpp"
#include "app/platform.hpp"
#include "app/session.hpp"

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>

#include "uvnet/uvnet.hpp"

namespace {

constexpr std::chrono::milliseconds kTick{100};

uvnet::Timer* g_interrupt_timer = nullptr; // 仅用于在中断回调里停表，避免额外捕获

const char* backend_name(uvnet::Backend backend) {
  switch (backend) {
    case uvnet::Backend::Iocp:
      return "IOCP (Windows)";
    case uvnet::Backend::Readiness:
      return "Readiness (Windows, wepoll)";
    case uvnet::Backend::Epoll:
      return "epoll (Linux)";
    default:
      return "unknown";
  }
}

} // namespace

int main(int argc, char** argv) {
  app::platform::init_console();

  app::Config config;
  std::string error;

  switch (app::parse_args(argc, argv, config, error)) {
    case app::ArgOutcome::Exit:
      return 0;
    case app::ArgOutcome::Error:
      std::fprintf(stderr, "参数错误: %s\n", error.c_str());
      app::print_usage(argv[0]);
      return 2;
    case app::ArgOutcome::Run:
      break;
  }

  app::set_log_level(config.log_level);
  app::platform::install_stop_handler();

  uvnet::Loop loop;
  app::log_info(std::string("uvnet 后端: ") + backend_name(loop.backend()));

  auto session = std::make_shared<app::ClientSession>(loop, config);
  session->start();

  // 信号处理函数只置标志，这里在循环线程上轮询并执行收尾
  uvnet::Timer interrupt_timer(loop);
  g_interrupt_timer = &interrupt_timer;
  if (const auto started = interrupt_timer.start(kTick, kTick, [&loop, &session] {
        if (!app::platform::stop_requested()) {
          return;
        }
        if (g_interrupt_timer != nullptr) {
          g_interrupt_timer->stop();
        }
        app::log_warn("收到中断信号，正在退出");
        session->stop("收到中断信号");
        loop.stop();
      });
      !started) {
    app::log_error("启动中断轮询定时器失败: " + started.error().message());
    return 1;
  }

  loop.run();
  g_interrupt_timer = nullptr;

  return session->succeeded() ? 0 : 1;
}
