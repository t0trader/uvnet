#include "uvnet/loop.hpp"
#include "uvnet/timer.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "detail/loop.hpp"
#include "detail/platform.hpp"

namespace uvnet::detail {

namespace {

std::chrono::milliseconds steady_now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch());
}

} // namespace

TimerImpl::TimerImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}

TimerImpl::~TimerImpl() {
  if (active_) {
    loop_->remove_timer(this);
    active_ = false;
  }
  release_ref();
}

VoidResult TimerImpl::start(std::chrono::milliseconds timeout,
                            std::chrono::milliseconds repeat,
                            std::function<void()> cb) {
  if (!cb) {
    return std::unexpected(make_error(ErrorCode::InvalidArgument));
  }
  if (timeout.count() < 0 || repeat.count() < 0) {
    return std::unexpected(make_error(ErrorCode::InvalidArgument));
  }

  if (active_) {
    loop_->remove_timer(this);
    active_ = false;
  }

  callback_ = std::move(cb);
  repeat_ = repeat;
  deadline_ = loop_->now() + timeout;
  ++generation_;
  active_ = true;

  if (!holds_ref_) {
    holds_ref_ = true;
    loop_->begin_work();
  }

  loop_->add_timer(this);
  return {};
}

void TimerImpl::stop() {
  if (active_) {
    loop_->remove_timer(this);
    active_ = false;
    ++generation_;
  }
  if (holds_ref_) {
    release_ref();
    ++generation_;
  }
}

void TimerImpl::release_ref() {
  if (!holds_ref_) {
    return;
  }
  holds_ref_ = false;
  loop_->end_work();
}

LoopImpl::LoopImpl() : thread_id_(std::this_thread::get_id()), now_ms_(steady_now_ms()) {}

LoopImpl::~LoopImpl() {
  if (poller_) {
    poller_->deinit();
  }
}

std::shared_ptr<LoopImpl> LoopImpl::create() {
  auto loop = std::shared_ptr<LoopImpl>(new LoopImpl());
  if (!loop->init()) {
    return nullptr;
  }
  return loop;
}

bool LoopImpl::init() {
  poller_ = create_poller();
  if (!poller_) {
    return false;
  }
  return poller_->init(*this);
}

void LoopImpl::update_time() {
  now_ms_ = steady_now_ms();
}

Backend LoopImpl::backend() const {
  return poller_ ? poller_->backend() : Backend::Unknown;
}

void LoopImpl::begin_work() {
  work_count_.fetch_add(1, std::memory_order_relaxed);
}

void LoopImpl::end_work() {
  work_count_.fetch_sub(1, std::memory_order_relaxed);
}

bool LoopImpl::alive() const {
  return work_count_.load(std::memory_order_relaxed) > 0;
}

void LoopImpl::post(std::function<void()> fn) {
  if (!fn) {
    return;
  }

  const bool on_loop_thread = std::this_thread::get_id() == thread_id_;
  bool need_wake = false;

  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_.push_back(std::move(fn));
    if (!on_loop_thread && !wake_pending_) {
      wake_pending_ = true;
      need_wake = true;
    }
  }

  if (need_wake) {
    poller_->wake();
  }
}

void LoopImpl::flush_pending() {
  std::deque<std::function<void()>> local;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    local.swap(pending_);
    wake_pending_ = false;
  }

  if (local.empty()) {
    return;
  }

  processed_ += static_cast<int>(local.size());
  for (auto& fn : local) {
    fn();
  }
}

void LoopImpl::stop() {
  stop_.store(true, std::memory_order_relaxed);
  if (poller_) {
    poller_->wake();
  }
}
void LoopImpl::add_timer(TimerImpl* timer) {
  timer->heap_index_ = timer_heap_.size();
  timer_heap_.push_back(timer);

  std::size_t index = timer->heap_index_;
  while (index > 0) {
    const std::size_t parent = (index - 1) / 2;
    if (timer_heap_[parent]->deadline_ <= timer_heap_[index]->deadline_) {
      break;
    }
    std::swap(timer_heap_[parent], timer_heap_[index]);
    timer_heap_[parent]->heap_index_ = parent;
    timer_heap_[index]->heap_index_ = index;
    index = parent;
  }
}

void LoopImpl::remove_timer(TimerImpl* timer) {
  const std::size_t index = timer->heap_index_;
  if (index == static_cast<std::size_t>(-1)) {
    return;
  }

  TimerImpl* last = timer_heap_.back();
  timer_heap_.pop_back();
  timer->heap_index_ = static_cast<std::size_t>(-1);

  if (last == timer || index >= timer_heap_.size()) {
    return;
  }

  timer_heap_[index] = last;
  last->heap_index_ = index;

  // 先上浮再下沉，保持堆序
  std::size_t current = index;
  while (current > 0) {
    const std::size_t parent = (current - 1) / 2;
    if (timer_heap_[parent]->deadline_ <= timer_heap_[current]->deadline_) {
      break;
    }
    std::swap(timer_heap_[parent], timer_heap_[current]);
    timer_heap_[parent]->heap_index_ = parent;
    timer_heap_[current]->heap_index_ = current;
    current = parent;
  }
  if (current != index) {
    return;
  }

  while (true) {
    const std::size_t left = current * 2 + 1;
    if (left >= timer_heap_.size()) {
      break;
    }
    const std::size_t right = left + 1;
    std::size_t smallest = left;
    if (right < timer_heap_.size() &&
        timer_heap_[right]->deadline_ < timer_heap_[left]->deadline_) {
      smallest = right;
    }
    if (timer_heap_[current]->deadline_ <= timer_heap_[smallest]->deadline_) {
      break;
    }
    std::swap(timer_heap_[current], timer_heap_[smallest]);
    timer_heap_[current]->heap_index_ = current;
    timer_heap_[smallest]->heap_index_ = smallest;
    current = smallest;
  }
}

void LoopImpl::run_timers() {
  while (!timer_heap_.empty() && timer_heap_.front()->deadline_ <= now_ms_) {
    TimerImpl* timer = timer_heap_.front();
    remove_timer(timer);

    if (!timer->active_) {
      continue;
    }

    auto self = timer->shared_from_this();
    timer->active_ = false;
    ++timer->generation_;
    const std::uint64_t generation = timer->generation_;

    auto callback = timer->callback_;
    ++processed_;
    if (callback) {
      callback();
    }

    if (timer->generation_ != generation) {
      // 回调内重启或停止了定时器，由回调自身的 start/stop 负责状态
      continue;
    }

    if (timer->repeat_.count() > 0) {
      timer->deadline_ = now_ms_ + timer->repeat_;
      timer->active_ = true;
      ++timer->generation_;
      add_timer(timer);
    } else {
      timer->release_ref();
    }
  }
}

std::optional<std::chrono::milliseconds> LoopImpl::next_timeout(
    std::optional<std::chrono::milliseconds> max_wait) const {
  std::optional<std::chrono::milliseconds> timeout;

  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    if (!pending_.empty()) {
      timeout = std::chrono::milliseconds(0);
    } else if (!timer_heap_.empty()) {
      const auto delta = timer_heap_.front()->deadline_ - now_ms_;
      timeout = delta.count() > 0 ? delta : std::chrono::milliseconds(0);
    } else if (work_count_.load(std::memory_order_relaxed) > 0) {
      timeout = std::nullopt;
    } else {
      timeout = std::chrono::milliseconds(0);
    }
  }

  if (max_wait) {
    timeout = timeout ? std::min(*timeout, *max_wait) : *max_wait;
  }
  return timeout;
}

void LoopImpl::run() {
  if (running_) {
    return;
  }

  running_ = true;
  stop_.store(false, std::memory_order_relaxed);

  while (!stop_.load(std::memory_order_relaxed)) {
    update_time();
    run_timers();
    flush_pending();

    if (stop_.load(std::memory_order_relaxed)) {
      break;
    }

    {
      std::lock_guard<std::mutex> lock(pending_mutex_);
      if (pending_.empty() && work_count_.load(std::memory_order_relaxed) == 0) {
        break;
      }
    }

    poller_->poll(next_timeout(std::nullopt));
  }

  running_ = false;
}

bool LoopImpl::run_once(std::chrono::milliseconds max_wait, bool no_wait) {
  const int before = processed_;

  update_time();
  run_timers();
  flush_pending();

  if (!stop_.load(std::memory_order_relaxed)) {
    std::optional<std::chrono::milliseconds> limit =
        no_wait ? std::optional<std::chrono::milliseconds>(std::chrono::milliseconds(0))
                : std::optional<std::chrono::milliseconds>(max_wait);
    poller_->poll(next_timeout(limit));
    update_time();
    run_timers();
    flush_pending();
  }

  return processed_ != before;
}

} // namespace uvnet::detail

namespace uvnet {

Loop::Loop() {
  impl_ = detail::LoopImpl::create();
  if (!impl_) {
    throw std::runtime_error("uvnet: failed to initialize event loop");
  }
}

Loop::~Loop() = default;

void Loop::run() {
  impl_->run();
}

bool Loop::run_once(std::chrono::milliseconds max_wait) {
  return impl_->run_once(max_wait, false);
}

bool Loop::poll() {
  return impl_->run_once(std::chrono::milliseconds(0), true);
}

void Loop::stop() {
  impl_->stop();
}

bool Loop::alive() const {
  return impl_->alive();
}

std::chrono::milliseconds Loop::now() const {
  return impl_->now();
}

void Loop::update_time() {
  impl_->update_time();
}

Backend Loop::backend() const {
  return impl_->backend();
}

void Loop::post(std::function<void()> fn) {
  impl_->post(std::move(fn));
}

Timer::Timer(Loop& loop) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::TimerImpl>(impl->shared_from_this());
}

Timer::~Timer() = default;

VoidResult Timer::start(std::chrono::milliseconds timeout, Callback cb) {
  return impl_->start(timeout, std::chrono::milliseconds(0), std::move(cb));
}

VoidResult Timer::start(std::chrono::milliseconds timeout,
                        std::chrono::milliseconds repeat,
                        Callback cb) {
  return impl_->start(timeout, repeat, std::move(cb));
}

void Timer::stop() {
  if (impl_) {
    impl_->stop();
  }
}

bool Timer::is_active() const {
  return impl_ && impl_->active();
}

} // namespace uvnet
