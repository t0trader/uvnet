#pragma once

#include <chrono>
#include <cstdio>

#include "uvnet/uvnet.hpp"

namespace apptest {

inline int failures = 0;

inline void check(bool condition, const char* expression, const char* file, int line) {
  if (!condition) {
    std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", file, line, expression);
    ++failures;
  }
}

// 看门狗：用例卡住时强制退出循环
class Guard {
 public:
  explicit Guard(uvnet::Loop& loop,
                 std::chrono::milliseconds limit = std::chrono::seconds(15))
      : timer_(loop) {
    const auto started = timer_.start(limit, [&loop] { loop.stop(); });
    if (!started) {
      std::fprintf(stderr, "Guard 定时器启动失败: %s\n", started.error().message().c_str());
    }
  }

  void cancel() { timer_.stop(); }

 private:
  uvnet::Timer timer_;
};

inline int finish(const char* name) {
  if (failures == 0) {
    std::printf("[PASS] %s\n", name);
    return 0;
  }
  std::printf("[FAIL] %s (%d failures)\n", name, failures);
  return 1;
}

} // namespace apptest

#define CHECK(condition) ::apptest::check((condition), #condition, __FILE__, __LINE__)
