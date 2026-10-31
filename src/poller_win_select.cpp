#include "detail/ready_poller.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <utility>

#include "detail/platform.hpp"
#include "detail/win_socket.hpp"
#include "detail/wepoll/wepoll.h"

namespace uvnet::detail {

SelectPoller::~SelectPoller() {
  deinit();
}

bool SelectPoller::init(LoopImpl& loop) {
  loop_ = &loop;
  ensure_winsock();
  if (!win_create_socket_pair(wake_reader_, wake_writer_)) {
    deinit();
    return false;
  }
  return true;
}

void SelectPoller::deinit() {
  entries_.clear();
  if (wake_reader_ != INVALID_SOCKET) {
    (void)::closesocket(wake_reader_);
    wake_reader_ = INVALID_SOCKET;
  }
  if (wake_writer_ != INVALID_SOCKET) {
    (void)::closesocket(wake_writer_);
    wake_writer_ = INVALID_SOCKET;
  }
  loop_ = nullptr;
}

bool SelectPoller::attach(SOCKET socket, ReadyHandler* handler, std::uint32_t events) {
  if (wake_reader_ == INVALID_SOCKET || socket == INVALID_SOCKET) {
    return false;
  }
  if (entries_.find(socket) == entries_.end() && entries_.size() >= kSelectMaxSockets) {
    // 超过 FD_SETSIZE 时拒绝注册，避免 select 静默截断
    return false;
  }
  entries_[socket] = Entry{handler, events};
  return true;
}

bool SelectPoller::modify(SOCKET socket, ReadyHandler* handler, std::uint32_t events) {
  const auto it = entries_.find(socket);
  if (it == entries_.end()) {
    return false;
  }
  it->second = Entry{handler, events};
  return true;
}

bool SelectPoller::detach(SOCKET socket) {
  return entries_.erase(socket) != 0;
}

void SelectPoller::wake() {
  if (wake_writer_ == INVALID_SOCKET) {
    return;
  }
  const char byte = 1;
  (void)::send(wake_writer_, &byte, 1, 0);
}

void SelectPoller::drain_wakeup() {
  char buffer[64];
  for (;;) {
    const int n = ::recv(wake_reader_, buffer, static_cast<int>(sizeof(buffer)), 0);
    if (n > 0) {
      continue;
    }
    // 0 或 WSAEWOULDBLOCK 都表示已排空
    return;
  }
}

void SelectPoller::poll(std::optional<std::chrono::milliseconds> timeout) {
  if (wake_reader_ == INVALID_SOCKET) {
    return;
  }

  // 句柄数很少，每次现场重建 fd_set；注册表变化时不需要额外记账
  fd_set reads;
  fd_set writes;
  fd_set exceptions;
  FD_ZERO(&reads);
  FD_ZERO(&writes);
  FD_ZERO(&exceptions);

  for (const auto& [socket, entry] : entries_) {
    if ((entry.events & EPOLLIN) != 0) {
      FD_SET(socket, &reads);
    }
    if ((entry.events & EPOLLOUT) != 0) {
      FD_SET(socket, &writes);
    }
    // 失败的非阻塞 connect 在 Winsock 上只出现在 exceptfds，
    // 必须统一监听，否则连接被拒时收不到任何事件
    FD_SET(socket, &exceptions);
  }
  FD_SET(wake_reader_, &reads);

  timeval tv{};
  timeval* timeout_ptr = nullptr;
  if (timeout) {
    const auto count = std::max<std::int64_t>(0, timeout->count());
    tv.tv_sec = static_cast<long>(count / 1000);
    tv.tv_usec = static_cast<long>((count % 1000) * 1000);
    timeout_ptr = &tv;
  }

  // Winsock 忽略 nfds 参数
  const int ready_count = ::select(0, &reads, &writes, &exceptions, timeout_ptr);
  if (ready_count <= 0) {
    return;
  }

  if (FD_ISSET(wake_reader_, &reads)) {
    drain_wakeup();
  }

  // 回调里可能 attach/detach，先快照再分发；容量固定，避免每次 poll 堆分配
  std::array<std::pair<ReadyHandler*, std::uint32_t>, kSelectMaxSockets> ready{};
  std::size_t ready_size = 0;
  for (const auto& [socket, entry] : entries_) {
    std::uint32_t events = 0;
    if (FD_ISSET(socket, &reads)) {
      events |= EPOLLIN;
    }
    if (FD_ISSET(socket, &writes)) {
      events |= EPOLLOUT;
    }
    if (FD_ISSET(socket, &exceptions)) {
      events |= EPOLLERR;
    }
    if (events != 0) {
      ready[ready_size++] = {entry.handler, events};
    }
  }

  for (std::size_t i = 0; i < ready_size; ++i) {
    ReadyHandler* handler = ready[i].first;
    if (handler == nullptr) {
      continue;
    }
    loop_->note_activity();
    handler->on_ready(ready[i].second);
  }
}

} // namespace uvnet::detail
