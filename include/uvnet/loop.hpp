#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "uvnet/error.hpp"

namespace uvnet {

namespace detail {
class LoopImpl;
} // namespace detail

enum class Backend : std::uint8_t {
  Unknown,
  // Linux：epoll（level-triggered）
  Epoll,
  // Windows：overlapped I/O + 完成端口，吞吐优先，TCP/UDP 均可用
  Iocp,
  // Windows：wepoll 就绪通知 + 非阻塞 recv/send，延迟优先，TCP/UDP 均可用
  Readiness,
};

// 单线程事件循环。所有句柄操作都必须在循环线程执行，
// 例外是 post() / Async::send()，它们是线程安全的。
class Loop {
 public:
  // 平台默认后端：Windows 为 Iocp，Linux 为 Epoll
  Loop();
  // 显式选择后端；Linux 上只支持 Epoll，传入其他值会被忽略
  explicit Loop(Backend backend);
  ~Loop();

  Loop(const Loop&) = delete;
  Loop& operator=(const Loop&) = delete;
  Loop(Loop&&) noexcept = default;
  Loop& operator=(Loop&&) noexcept = default;

  // 运行到 stop() 或没有任何活动句柄/请求
  void run();
  // 最多等待 max_wait，返回本轮是否处理了事件
  bool run_once(std::chrono::milliseconds max_wait);
  // 非阻塞处理一轮
  bool poll();

  void stop();

  // 是否还有活动句柄或未完成的异步请求
  [[nodiscard]] bool alive() const;
  // 当前缓存的单调时间（毫秒）
  [[nodiscard]] std::chrono::milliseconds now() const;
  void update_time();

  [[nodiscard]] Backend backend() const;

  // 线程安全：把 fn 投递到循环线程执行
  void post(std::function<void()> fn);

  // 内部使用，勿直接调用
  [[nodiscard]] detail::LoopImpl* native() const noexcept { return impl_.get(); }

 private:
  std::shared_ptr<detail::LoopImpl> impl_;
};

} // namespace uvnet
