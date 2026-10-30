#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include "uvnet/loop.hpp"

namespace uvnet {

namespace detail {
class TimerImpl;
} // namespace detail

class Timer {
 public:
  using Callback = std::function<void()>;

  explicit Timer(Loop& loop);
  ~Timer();

  Timer(const Timer&) = default;
  Timer& operator=(const Timer&) = default;
  Timer(Timer&&) noexcept = default;
  Timer& operator=(Timer&&) noexcept = default;

  // 一次性定时器
  VoidResult start(std::chrono::milliseconds timeout, Callback cb);
  // 重复定时器；repeat 为 0 时等价于一次性
  VoidResult start(std::chrono::milliseconds timeout,
                   std::chrono::milliseconds repeat,
                   Callback cb);

  void stop();

  [[nodiscard]] bool is_active() const;

 private:
  std::shared_ptr<detail::TimerImpl> impl_;
};

} // namespace uvnet
