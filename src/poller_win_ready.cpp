#include "detail/ready_poller.hpp"

#include <algorithm>
#include <cstdint>

#include "detail/platform.hpp"
#include "detail/win_socket.hpp"
#include "detail/wepoll/wepoll.h"

namespace uvnet::detail {

namespace {

// 唤醒对：wepoll 没有 eventfd，用一个环回 socketpair 把 wake() 变成可读事件
bool create_socket_pair(SOCKET& reader, SOCKET& writer) {
  reader = INVALID_SOCKET;
  writer = INVALID_SOCKET;

  const SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == INVALID_SOCKET) {
    return false;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0 ||
      ::listen(listener, 1) != 0) {
    (void)::closesocket(listener);
    return false;
  }

  sockaddr_in local{};
  int length = static_cast<int>(sizeof(local));
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&local), &length) != 0) {
    (void)::closesocket(listener);
    return false;
  }

  SOCKET client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (client == INVALID_SOCKET) {
    (void)::closesocket(listener);
    return false;
  }
  if (::connect(client, reinterpret_cast<const sockaddr*>(&local), sizeof(local)) != 0) {
    (void)::closesocket(client);
    (void)::closesocket(listener);
    return false;
  }

  SOCKET server = ::accept(listener, nullptr, nullptr);
  (void)::closesocket(listener);
  if (server == INVALID_SOCKET) {
    (void)::closesocket(client);
    return false;
  }

  (void)win_set_nonblocking(client);
  (void)win_set_nonblocking(server);
  reader = server;
  writer = client;
  return true;
}

} // namespace

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

  if (!create_socket_pair(wake_reader_, wake_writer_)) {
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
