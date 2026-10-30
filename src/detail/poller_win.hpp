#pragma once

#if !defined(_WIN32)
#error "poller_win.hpp 仅用于 Windows"
#endif

#include <winsock2.h>
#include <windows.h>

#include <cstddef>

#include "detail/loop.hpp"

namespace uvnet::detail {

// IOCP 完成项基类。hEvent 必须保持为 nullptr，否则部分 Winsock 扩展函数
// 会返回 WSA_INVALID_HANDLE；这里用 offsetof 从 OVERLAPPED 反查所属对象。
struct WinOperation {
  virtual ~WinOperation() = default;

  // error 为 0 表示成功；字节数从 ov.InternalHigh 读取
  virtual void on_complete(LoopImpl& loop, DWORD error) = 0;

  OVERLAPPED ov{};
  HANDLE file = nullptr;
  // false 表示该操作对象由句柄长期持有（例如可复用的 UDP 收包槽位）
  bool delete_after_completion = true;
  // FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 开启时，OVERLAPPED I/O 同步完成不会投递
  // 到 IOCP。提交点若已走同步完成路径，需置位此标记；poller 收到包会跳过，避免双回调
  bool completed_synchronically = false;
};

// SetFileCompletionNotificationModes 仅 Vista+ 存在
#ifndef FILE_SKIP_COMPLETION_PORT_ON_SUCCESS
#define FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 0x1
#endif
#ifndef FILE_SKIP_SET_EVENT_ON_HANDLE
#define FILE_SKIP_SET_EVENT_ON_HANDLE 0x2
#endif

class IocpPoller final : public Poller {
 public:
  ~IocpPoller() override;

  bool init(LoopImpl& loop) override;
  void deinit() override;
  void poll(std::optional<std::chrono::milliseconds> timeout) override;
  void wake() override;
  [[nodiscard]] Backend backend() const override { return Backend::Iocp; }

  [[nodiscard]] HANDLE iocp() const noexcept { return iocp_; }
  // 将 socket 关联到完成端口并开启 FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 快速模式
  bool associate(SOCKET socket) const;

 private:
  HANDLE iocp_ = nullptr;
  LoopImpl* loop_ = nullptr;
};

inline IocpPoller& win_poller(LoopImpl& loop) {
  return static_cast<IocpPoller&>(loop.poller());
}

inline WinOperation* operation_from(OVERLAPPED* overlapped) noexcept {
  return reinterpret_cast<WinOperation*>(
      reinterpret_cast<char*>(overlapped) - offsetof(WinOperation, ov));
}

// 用于 PostQueuedCompletionStatus 的唤醒完成键
inline constexpr ULONG_PTR kAsyncWakeKey = static_cast<ULONG_PTR>(0x55474E57u);

} // namespace uvnet::detail
