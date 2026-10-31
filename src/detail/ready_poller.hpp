#pragma once

#if !defined(_WIN32)
#error "ready_poller.hpp 仅用于 Windows"
#endif

#include <winsock2.h>
#include <windows.h>

#include <cstdint>
#include <optional>

#include "detail/loop.hpp"

namespace uvnet::detail {

// 就绪式后端的事件处理器：每次 epoll_wait 返回后由 ReadyPoller 回调
class ReadyHandler {
 public:
  virtual ~ReadyHandler() = default;
  virtual void on_ready(std::uint32_t events) = 0;
};

// read 就绪个数即 epoll 事件数上限
inline constexpr int kReadyMaxEvents = 64;

// wepoll 封装：就绪通知 + 非阻塞 recv/send。
// 与 IocpPoller 互斥：一个 Loop 只用其中一种 poller。
class ReadyPoller final : public Poller {
 public:
  ~ReadyPoller() override;

  bool init(LoopImpl& loop) override;
  void deinit() override;
  void poll(std::optional<std::chrono::milliseconds> timeout) override;
  void wake() override;
  [[nodiscard]] Backend backend() const override { return Backend::Readiness; }

  bool attach(SOCKET socket, ReadyHandler* handler, std::uint32_t events);
  bool modify(SOCKET socket, ReadyHandler* handler, std::uint32_t events);
  bool detach(SOCKET socket);

 private:
  void drain_wakeup();

  HANDLE epfd_ = nullptr;
  LoopImpl* loop_ = nullptr;
  SOCKET wake_reader_ = INVALID_SOCKET;
  SOCKET wake_writer_ = INVALID_SOCKET;
};

inline ReadyPoller& ready_poller(LoopImpl& loop) {
  return static_cast<ReadyPoller&>(loop.poller());
}

} // namespace uvnet::detail
