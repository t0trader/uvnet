#include "detail/poller_linux.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace uvnet::detail {

EpollPoller::~EpollPoller() {
  deinit();
}

bool EpollPoller::init(LoopImpl& loop) {
  loop_ = &loop;
  epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epfd_ < 0) {
    return false;
  }

  wakefd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakefd_ < 0) {
    return false;
  }

  epoll_event event{};
  event.events = EPOLLIN;
  event.data.ptr = nullptr;
  if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, wakefd_, &event) != 0) {
    return false;
  }
  return true;
}

void EpollPoller::deinit() {
  if (wakefd_ >= 0) {
    ::close(wakefd_);
    wakefd_ = -1;
  }
  if (epfd_ >= 0) {
    ::close(epfd_);
    epfd_ = -1;
  }
}

void EpollPoller::wake() {
  if (wakefd_ < 0) {
    return;
  }
  const std::uint64_t value = 1;
  const ssize_t ignored = ::write(wakefd_, &value, sizeof(value));
  (void)ignored;
}

bool EpollPoller::add(IoWatcher* watcher, int fd, std::uint32_t events) {
  if (epfd_ < 0 || watcher == nullptr || fd < 0) {
    return false;
  }

  epoll_event event{};
  event.events = events;
  event.data.ptr = watcher;
  if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &event) != 0) {
    return false;
  }
  watcher->io_fd = fd;
  watched_.insert(watcher);
  return true;
}

bool EpollPoller::mod(IoWatcher* watcher, std::uint32_t events) {
  if (epfd_ < 0 || watcher == nullptr || watcher->io_fd < 0) {
    return false;
  }

  epoll_event event{};
  event.events = events;
  event.data.ptr = watcher;
  return ::epoll_ctl(epfd_, EPOLL_CTL_MOD, watcher->io_fd, &event) == 0;
}

void EpollPoller::del(IoWatcher* watcher) {
  if (epfd_ < 0 || watcher == nullptr || watcher->io_fd < 0) {
    return;
  }
  (void)::epoll_ctl(epfd_, EPOLL_CTL_DEL, watcher->io_fd, nullptr);
  watcher->io_fd = -1;
  watched_.erase(watcher);
}

void EpollPoller::poll(std::optional<std::chrono::milliseconds> timeout) {
  if (epfd_ < 0) {
    return;
  }

  const int timeout_ms =
      timeout ? static_cast<int>(std::min<std::int64_t>(timeout->count(), INT_MAX)) : -1;

  constexpr int kMaxEvents = 64;
  epoll_event events[kMaxEvents];

  int count = 0;
  while (true) {
    count = ::epoll_wait(epfd_, events, kMaxEvents, timeout_ms);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    break;
  }

  if (count <= 0) {
    return;
  }

  LoopImpl& loop = *loop_;
  for (int i = 0; i < count; ++i) {
    if (events[i].data.ptr == nullptr) {
      // 唤醒 fd：清空计数即可
      std::uint64_t value = 0;
      while (::read(wakefd_, &value, sizeof(value)) > 0) {
      }
      continue;
    }

    auto* watcher = static_cast<IoWatcher*>(events[i].data.ptr);
    if (watched_.find(watcher) == watched_.end()) {
      // 该 watcher 已在本轮更早的回调中被关闭
      continue;
    }
    loop.note_activity();
    watcher->on_io(events[i].events);
  }
}

std::unique_ptr<Poller> create_poller() {
  return std::make_unique<EpollPoller>();
}

} // namespace uvnet::detail
