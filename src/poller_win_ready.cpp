#include "detail/ready_poller.hpp"

#include <algorithm>
#include <cstdint>

#include "detail/platform.hpp"
#include "detail/win_socket.hpp"
#include "detail/wepoll/wepoll.h"

namespace uvnet::detail {

ReadyPoller::~ReadyPoller() {
  deinit();
}

bool ReadyPoller::init(LoopImpl& loop) {
  loop_ = &loop;
  ensure_winsock();

  epfd_ = epoll_create1(0);
  if (epfd_ == nullptr) {
    return false;
  }

  if (!win_create_socket_pair(wake_reader_, wake_writer_)) {
    deinit();
    return false;
  }

  epoll_event event{};
  event.events = EPOLLIN;
  event.data.ptr = nullptr;
  if (epoll_ctl(epfd_, EPOLL_CTL_ADD, wake_reader_, &event) != 0) {
    deinit();
    return false;
  }
  return true;
}

void ReadyPoller::deinit() {
  if (wake_reader_ != INVALID_SOCKET) {
    (void)::closesocket(wake_reader_);
    wake_reader_ = INVALID_SOCKET;
  }
  if (wake_writer_ != INVALID_SOCKET) {
    (void)::closesocket(wake_writer_);
    wake_writer_ = INVALID_SOCKET;
  }
  if (epfd_ != nullptr) {
    (void)epoll_close(epfd_);
    epfd_ = nullptr;
  }
}

bool ReadyPoller::attach(SOCKET socket, ReadyHandler* handler, std::uint32_t events) {
  if (epfd_ == nullptr || socket == INVALID_SOCKET) {
    return false;
  }
  epoll_event event{};
  event.events = events;
  event.data.ptr = handler;
  return epoll_ctl(epfd_, EPOLL_CTL_ADD, socket, &event) == 0;
}

bool ReadyPoller::modify(SOCKET socket, ReadyHandler* handler, std::uint32_t events) {
  if (epfd_ == nullptr || socket == INVALID_SOCKET) {
    return false;
  }
  epoll_event event{};
  event.events = events;
  // wepoll 的 MOD 会整体覆盖注册时的 data，必须重新带上 handler
  event.data.ptr = handler;
  return epoll_ctl(epfd_, EPOLL_CTL_MOD, socket, &event) == 0;
}

bool ReadyPoller::detach(SOCKET socket) {
  if (epfd_ == nullptr || socket == INVALID_SOCKET) {
    return false;
  }
  epoll_event event{};
  event.events = 0;
  event.data.ptr = nullptr;
  return epoll_ctl(epfd_, EPOLL_CTL_DEL, socket, &event) == 0;
}

void ReadyPoller::wake() {
  if (wake_writer_ == INVALID_SOCKET) {
    return;
  }
  const char byte = 1;
  (void)::send(wake_writer_, &byte, 1, 0);
}

void ReadyPoller::drain_wakeup() {
  char buffer[64];
  for (;;) {
    const int n = ::recv(wake_reader_, buffer, static_cast<int>(sizeof(buffer)), 0);
    if (n > 0) {
      continue;
    }
    if (n == 0) {
      return;
    }
    if (WSAGetLastError() == WSAEWOULDBLOCK) {
      return;
    }
    return;
  }
}

void ReadyPoller::poll(std::optional<std::chrono::milliseconds> timeout) {
  if (epfd_ == nullptr) {
    return;
  }

  int wait_ms = -1;
  if (timeout) {
    const auto count = timeout->count();
    wait_ms = count <= 0 ? 0 : static_cast<int>(std::min<std::int64_t>(count, 0x7FFFFFFF));
  }

  epoll_event events[kReadyMaxEvents];
  const int count = epoll_wait(epfd_, events, kReadyMaxEvents, wait_ms);
  if (count <= 0) {
    return;
  }

  for (int i = 0; i < count; ++i) {
    ReadyHandler* handler = static_cast<ReadyHandler*>(events[i].data.ptr);
    if (handler == nullptr) {
      drain_wakeup();
      continue;
    }
    loop_->note_activity();
    handler->on_ready(events[i].events);
  }
}

} // namespace uvnet::detail
