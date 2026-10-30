#pragma once

#include <atomic>
#include <cstdint>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "uvnet/loop.hpp"

namespace uvnet::detail {

class LoopImpl;
class Poller;

// 定时器内部实现；由 LoopImpl 的最小堆持有裸指针，
// 生命周期由 public Timer 的 shared_ptr 以及触发时的临时 shared_ptr 保证。
class TimerImpl : public std::enable_shared_from_this<TimerImpl> {
 public:
  explicit TimerImpl(std::shared_ptr<LoopImpl> loop);
  ~TimerImpl();
  TimerImpl(const TimerImpl&) = delete;
  TimerImpl& operator=(const TimerImpl&) = delete;

  VoidResult start(std::chrono::milliseconds timeout,
                   std::chrono::milliseconds repeat,
                   std::function<void()> cb);
  void stop();

  [[nodiscard]] bool active() const noexcept { return active_; }

  // 以下字段仅供 LoopImpl 访问
  std::shared_ptr<LoopImpl> loop_;
  std::function<void()> callback_;
  std::chrono::milliseconds deadline_{0};
  std::chrono::milliseconds repeat_{0};
  std::uint64_t generation_ = 0;
  std::size_t heap_index_ = static_cast<std::size_t>(-1);
  bool active_ = false;
  bool holds_ref_ = false;

 private:
  friend class LoopImpl;
  void release_ref();
};

class Poller {
 public:
  virtual ~Poller() = default;

  virtual bool init(LoopImpl& loop) = 0;
  virtual void deinit() = 0;
  // timeout 为空表示无限等待
  virtual void poll(std::optional<std::chrono::milliseconds> timeout) = 0;
  virtual void wake() = 0;
  [[nodiscard]] virtual Backend backend() const = 0;
};

std::unique_ptr<Poller> create_poller();

class LoopImpl : public std::enable_shared_from_this<LoopImpl> {
 public:
  static std::shared_ptr<LoopImpl> create();

  ~LoopImpl();
  LoopImpl(const LoopImpl&) = delete;
  LoopImpl& operator=(const LoopImpl&) = delete;

  void run();
  bool run_once(std::chrono::milliseconds max_wait, bool no_wait);
  void stop();

  [[nodiscard]] bool alive() const;
  [[nodiscard]] std::chrono::milliseconds now() const noexcept { return now_ms_; }
  void update_time();
  [[nodiscard]] bool stopping() const noexcept { return stop_.load(std::memory_order_relaxed); }
  [[nodiscard]] std::thread::id thread_id() const noexcept { return thread_id_; }
  [[nodiscard]] Backend backend() const;
  [[nodiscard]] Poller& poller() const noexcept { return *poller_; }

  // 线程安全
  void post(std::function<void()> fn);
  void note_activity() noexcept { ++processed_; }

  // 活动计数：>0 时 run() 会继续等待
  void begin_work();
  void end_work();

  // 定时器堆操作（仅循环线程）
  void add_timer(TimerImpl* timer);
  void remove_timer(TimerImpl* timer);

 private:
  LoopImpl();
  bool init();

  void flush_pending();
  void run_timers();
  [[nodiscard]] std::optional<std::chrono::milliseconds> next_timeout(
      std::optional<std::chrono::milliseconds> max_wait) const;

  std::unique_ptr<Poller> poller_;
  std::deque<std::function<void()>> pending_;
  mutable std::mutex pending_mutex_;
  bool wake_pending_ = false;
  std::vector<TimerImpl*> timer_heap_;
  std::thread::id thread_id_;
  std::atomic<bool> stop_{false};
  std::chrono::milliseconds now_ms_{0};
  std::atomic<int> work_count_{0};
  bool running_ = false;
  int processed_ = 0;
};

} // namespace uvnet::detail
