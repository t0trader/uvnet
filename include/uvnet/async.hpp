#pragma once

#include <functional>
#include <memory>

#include "uvnet/loop.hpp"

namespace uvnet {

namespace detail {
class AsyncImpl;
} // namespace detail

// 跨线程唤醒器：send() 可在任意线程调用，回调在循环线程执行。
// 多次 send() 会被合并为一次回调。
class Async {
 public:
  explicit Async(Loop& loop, std::function<void()> cb);
  ~Async();

  Async(const Async&) = default;
  Async& operator=(const Async&) = default;
  Async(Async&&) noexcept = default;
  Async& operator=(Async&&) noexcept = default;

  void send();

  // 关闭后不再触发回调，并释放对事件循环的引用
  void close();

  [[nodiscard]] bool is_open() const;

 private:
  std::shared_ptr<detail::AsyncImpl> impl_;
};

} // namespace uvnet
