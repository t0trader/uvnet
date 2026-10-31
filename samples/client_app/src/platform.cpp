#include "app/platform.hpp"

#include <atomic>

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#endif

namespace app::platform {

namespace {

std::atomic<bool> g_stop_requested{false};

#ifdef _WIN32
BOOL WINAPI console_ctrl_handler(DWORD event) {
  if (event == CTRL_C_EVENT || event == CTRL_BREAK_EVENT || event == CTRL_CLOSE_EVENT) {
    g_stop_requested.store(true, std::memory_order_relaxed);
    return TRUE; // 已接管，阻止运行时默认的立即退出
  }
  return FALSE;
}
#else
// 信号处理函数里只能做 async-signal-safe 操作：这里只写一个原子标志，
// 收尾动作交给循环线程上的定时器。
extern "C" void posix_signal_handler(int) {
  g_stop_requested.store(true, std::memory_order_relaxed);
}
#endif

} // namespace

void init_console() {
#ifdef _WIN32
  ::SetConsoleOutputCP(CP_UTF8);
  ::SetConsoleCP(CP_UTF8);
#endif
}

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

bool stop_requested() {
  return g_stop_requested.load(std::memory_order_relaxed);
}

} // namespace app::platform
