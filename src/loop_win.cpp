#include "detail/poller_win.hpp"

#include <algorithm>
#include <cstdint>
#include <mutex>

namespace uvnet::detail {

namespace {

std::once_flag g_wsa_once;
bool g_wsa_ready = false;

} // namespace

void ensure_winsock() {
  std::call_once(g_wsa_once, [] {
    WSADATA data{};
    g_wsa_ready = ::WSAStartup(MAKEWORD(2, 2), &data) == 0;
  });
}

IocpPoller::~IocpPoller() {
  deinit();
}

bool IocpPoller::init(LoopImpl& loop) {
  loop_ = &loop;
  ensure_winsock();
  iocp_ = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);
  return iocp_ != nullptr;
}

void IocpPoller::deinit() {
  if (iocp_ != nullptr) {
    ::CloseHandle(iocp_);
    iocp_ = nullptr;
  }
}

bool IocpPoller::associate(SOCKET socket) const {
  if (iocp_ == nullptr || socket == INVALID_SOCKET) {
    return false;
  }
  HANDLE handle = reinterpret_cast<HANDLE>(socket);
  if (::CreateIoCompletionPort(handle, iocp_, 0, 0) == nullptr) {
    return false;
  }
  // 开启快速模式：OVERLAPPED I/O 同步成功时跳过完成端口投递，省一次内核/用户态切换
  // 同步完成的 op 通过 post 走完成路径，避免递归 submit→drain→submit 爆栈
  {
    static const DWORD kSkipModes =
        FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE;
    using SetModesFn = BOOL(WINAPI*)(HANDLE, UCHAR);
    static auto* set_modes = reinterpret_cast<SetModesFn>(
        ::GetProcAddress(::GetModuleHandleW(L"kernel32.dll"),
                          "SetFileCompletionNotificationModes"));
    if (set_modes != nullptr) {
      (void)set_modes(handle, static_cast<UCHAR>(kSkipModes));
    }
  }
  return true;
}

void IocpPoller::wake() {
  if (iocp_ != nullptr) {
    (void)::PostQueuedCompletionStatus(iocp_, 0, kAsyncWakeKey, nullptr);
  }
}

void IocpPoller::poll(std::optional<std::chrono::milliseconds> timeout) {
  if (iocp_ == nullptr) {
    return;
  }

  DWORD wait_ms = INFINITE;
  if (timeout) {
    const auto count = timeout->count();
    wait_ms = count <= 0 ? 0 : static_cast<DWORD>(std::min<std::int64_t>(count, 0x7FFFFFFF));
  }

  constexpr ULONG kMaxEntries = 128;
  OVERLAPPED_ENTRY entries[kMaxEntries]{};
  ULONG count = 0;

  if (!::GetQueuedCompletionStatusEx(iocp_, entries, kMaxEntries, &count, wait_ms, FALSE)) {
    const DWORD error = ::GetLastError();
    if (error == WAIT_TIMEOUT || error == ERROR_ABANDONED_WAIT_0) {
      return;
    }
    return;
  }

  LoopImpl& loop = *loop_;
  for (ULONG i = 0; i < count; ++i) {
    OVERLAPPED* overlapped = entries[i].lpOverlapped;
    if (overlapped == nullptr) {
      // 唤醒包，交给下一轮 flush_pending 处理
      continue;
    }

    WinOperation* operation = operation_from(overlapped);

    // 若提交点已走同步完成路径（FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 未生效时
    // 仍可能收到一个多余的完成包），直接丢弃，避免重复回调
    if (operation->completed_synchronically) {
      continue;
    }

    loop.note_activity();

    DWORD error = 0;
    if (entries[i].Internal != 0) {
      DWORD transferred = 0;
      if (!::GetOverlappedResult(operation->file, &operation->ov, &transferred, FALSE)) {
        error = ::GetLastError();
      } else {
        // 兜底：NTSTATUS 0xC0000120 表示取消
        error = entries[i].Internal == 0xC0000120L
                    ? ERROR_OPERATION_ABORTED
                    : static_cast<DWORD>(entries[i].Internal);
      }
    }

    const bool destroy_operation = operation->delete_after_completion;
    operation->on_complete(loop, error);
    if (destroy_operation) {
      delete operation;
    }
  }
}

std::unique_ptr<Poller> create_poller() {
  return std::make_unique<IocpPoller>();
}

} // namespace uvnet::detail
