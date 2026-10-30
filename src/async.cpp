#include "uvnet/async.hpp"

#include <atomic>
#include <stdexcept>
#include <utility>

#include "detail/loop.hpp"

namespace uvnet::detail {

class AsyncImpl : public std::enable_shared_from_this<AsyncImpl> {
 public:
  AsyncImpl(std::shared_ptr<LoopImpl> loop, std::function<void()> cb)
      : loop_(std::move(loop)), cb_(std::move(cb)) {
    loop_->begin_work();
  }

  ~AsyncImpl() { close(); }

  void send() {
    bool expected = false;
    if (!pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return;
    }

    auto self = shared_from_this();
    loop_->post([self] {
      self->pending_.store(false, std::memory_order_release);
      if (!self->open_.load(std::memory_order_acquire)) {
        return;
      }
      auto callback = self->cb_;
      if (callback) {
        callback();
      }
    });
  }

  void close() {
    if (!open_.exchange(false, std::memory_order_acq_rel)) {
      return;
    }

    if (std::this_thread::get_id() == loop_->thread_id()) {
      cb_ = nullptr;
      loop_->end_work();
    } else {
      auto loop = loop_;
      loop->post([loop] { loop->end_work(); });
    }
  }

  [[nodiscard]] bool is_open() const {
    return open_.load(std::memory_order_acquire);
  }

 private:
  std::shared_ptr<LoopImpl> loop_;
  std::function<void()> cb_;
  std::atomic<bool> pending_{false};
  std::atomic<bool> open_{true};
};

} // namespace uvnet::detail

namespace uvnet {

Async::Async(Loop& loop, std::function<void()> cb) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::AsyncImpl>(impl->shared_from_this(), std::move(cb));
}

Async::~Async() = default;

void Async::send() {
  if (impl_) {
    impl_->send();
  }
}

void Async::close() {
  if (impl_) {
    impl_->close();
  }
}

bool Async::is_open() const {
  return impl_ && impl_->is_open();
}

} // namespace uvnet
