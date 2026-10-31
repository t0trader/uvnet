#pragma once

#if !defined(_WIN32)
#error "ready_poller.hpp 仅用于 Windows"
#endif

#include <winsock2.h>
#include <windows.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "detail/loop.hpp"

namespace uvnet::detail {

// 就绪式后端的事件处理器：每次 poll 返回后由 poller 回调
class ReadyHandler {
 public:
  virtual ~ReadyHandler() = default;
  virtual void on_ready(std::uint32_t events) = 0;
};

// read 就绪个数即事件数上限
inline constexpr int kReadyMaxEvents = 64;

// 就绪式 poller 的公共注册接口；wepoll 与 select 两种实现共用
class ReadyPollerBase {
 public:
  virtual ~ReadyPollerBase() = default;
  virtual bool attach(SOCKET socket, ReadyHandler* handler, std::uint32_t events) = 0;
  virtual bool modify(SOCKET socket, ReadyHandler* handler, std::uint32_t events) = 0;
  virtual bool detach(SOCKET socket) = 0;
};

// wepoll 封装：就绪通知 + 非阻塞 recv/send。
// 与 IocpPoller 互斥：一个 Loop 只用其中一种 poller。
class ReadyPoller final : public Poller, public ReadyPollerBase {
 public:
  ~ReadyPoller() override;

  bool init(LoopImpl& loop) override;
  void deinit() override;
  void poll(std::optional<std::chrono::milliseconds> timeout) override;
  void wake() override;
  [[nodiscard]] Backend backend() const override { return Backend::Readiness; }

  bool attach(SOCKET socket, ReadyHandler* handler, std::uint32_t events) override;
  bool modify(SOCKET socket, ReadyHandler* handler, std::uint32_t events) override;
  bool detach(SOCKET socket) override;

 private:
  void drain_wakeup();

  HANDLE epfd_ = nullptr;
  LoopImpl* loop_ = nullptr;
  SOCKET wake_reader_ = INVALID_SOCKET;
  SOCKET wake_writer_ = INVALID_SOCKET;
};

// FD_SETSIZE(64) 减去唤醒套接字
inline constexpr std::size_t kSelectMaxSockets = static_cast<std::size_t>(FD_SETSIZE) - 1;

// select 封装：与 ReadyPoller 相同的就绪语义，但每次 poll 现场构建 fd_set。
// 句柄数受 FD_SETSIZE 限制，定位是单连接/少量句柄的低延迟客户端。
class SelectPoller final : public Poller, public ReadyPollerBase {
 public:
  ~SelectPoller() override;

  bool init(LoopImpl& loop) override;
  void deinit() override;
  void poll(std::optional<std::chrono::milliseconds> timeout) override;
  void wake() override;
  [[nodiscard]] Backend backend() const override { return Backend::Select; }

  bool attach(SOCKET socket, ReadyHandler* handler, std::uint32_t events) override;
  bool modify(SOCKET socket, ReadyHandler* handler, std::uint32_t events) override;
  bool detach(SOCKET socket) override;

 private:
  void drain_wakeup();

  struct Entry {
    ReadyHandler* handler = nullptr;
    std::uint32_t events = 0;
  };

  std::unordered_map<SOCKET, Entry> entries_;
  LoopImpl* loop_ = nullptr;
  SOCKET wake_reader_ = INVALID_SOCKET;
  SOCKET wake_writer_ = INVALID_SOCKET;
};

inline ReadyPollerBase& ready_poller(LoopImpl& loop) {
  // 只有这两种后端使用就绪接口，按 backend() 精确分派，避免 RTTI
  if (loop.backend() == Backend::Select) {
    return static_cast<SelectPoller&>(loop.poller());
  }
  return static_cast<ReadyPoller&>(loop.poller());
}

} // namespace uvnet::detail
