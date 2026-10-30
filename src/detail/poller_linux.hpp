#pragma once

#if !defined(__linux__)
#error "poller_linux.hpp 仅用于 Linux"
#endif

#include <cstdint>
#include <unordered_set>

#include "detail/loop.hpp"

namespace uvnet::detail {

// epoll 注册项。on_io 中必须自行持有 shared_ptr，防止回调内析构。
class IoWatcher {
 public:
  virtual ~IoWatcher() = default;
  virtual void on_io(std::uint32_t events) = 0;

  int io_fd = -1;
};

class EpollPoller final : public Poller {
 public:
  ~EpollPoller() override;

  bool init(LoopImpl& loop) override;
  void deinit() override;
  void poll(std::optional<std::chrono::milliseconds> timeout) override;
  void wake() override;
  [[nodiscard]] Backend backend() const override { return Backend::Epoll; }

  bool add(IoWatcher* watcher, int fd, std::uint32_t events);
  bool mod(IoWatcher* watcher, std::uint32_t events);
  void del(IoWatcher* watcher);

 private:
  int epfd_ = -1;
  int wakefd_ = -1;
  LoopImpl* loop_ = nullptr;
  std::unordered_set<IoWatcher*> watched_;
};

inline EpollPoller& linux_poller(LoopImpl& loop) {
  return static_cast<EpollPoller&>(loop.poller());
}

} // namespace uvnet::detail
