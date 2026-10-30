#pragma once

#include <chrono>
#include <cstdio>
#include <string>

#include "uvnet/uvnet.hpp"

namespace uvtest {

inline int failures = 0;

inline void check(bool condition, const char* expression, const char* file, int line) {
  if (!condition) {
    std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", file, line, expression);
    ++failures;
  }
}

inline bool expect_ok(const uvnet::VoidResult& result,
                      const char* expression,
                      const char* file,
                      int line) {
  if (!result) {
    std::fprintf(stderr,
                 "%s:%d: %s failed: %s (native=%d)\n",
                 file,
                 line,
                 expression,
                 result.error().message().c_str(),
                 static_cast<int>(result.error().native_code));
    ++failures;
    return false;
  }
  return true;
}

// 保护性定时器：测试卡住时强制退出循环；正常路径调用 cancel() 释放引用
class Guard {
 public:
  explicit Guard(uvnet::Loop& loop,
                 std::chrono::milliseconds limit = std::chrono::seconds(10))
      : timer_(loop) {
    (void)timer_.start(limit, [&loop] { loop.stop(); });
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

} // namespace uvtest

#define CHECK(condition) ::uvtest::check((condition), #condition, __FILE__, __LINE__)

#define CHECK_OK(expression)                                                       \
  do {                                                                            \
    if (!::uvtest::expect_ok((expression), #expression, __FILE__, __LINE__)) {     \
      return;                                                                      \
    }                                                                              \
  } while (false)
