#pragma once

#include <cstdint>
#include <expected>
#include <string>

namespace uvnet {

// 跨平台归一化错误码，屏蔽 errno / WSA / getaddrinfo 的差异
enum class ErrorCode : std::int16_t {
  None = 0,
  InvalidArgument,
  OutOfMemory,
  OutOfRange,
  PermissionDenied,
  AddressInUse,
  AddressNotAvailable,
  AddressFamilyNotSupported,
  AlreadyExists,
  AlreadyConnected,
  BadFileDescriptor,
  BrokenPipe,
  ConnectionAborted,
  ConnectionRefused,
  ConnectionReset,
  ConnectionTimedOut,
  EndOfFile,
  HostNotFound,
  HostUnreachable,
  NetworkDown,
  NetworkUnreachable,
  Interrupted,
  InProgress,
  MessageTooBig,
  NoBufferSpace,
  NotConnected,
  NotSocket,
  OperationNotSupported,
  ServiceNotSupported,
  TimedOut,
  TryAgain,
  WouldBlock,
  Canceled,
  DnsFailure,
  NoData,
  NotFound,
  Unknown,
};

struct Error {
  ErrorCode code = ErrorCode::Unknown;
  // 平台原生错误码：POSIX errno / Winsock WSA / getaddrinfo EAI
  std::int32_t native_code = 0;

  [[nodiscard]] std::string message() const;

  constexpr explicit operator bool() const noexcept { return code != ErrorCode::None; }

  friend bool operator==(const Error& lhs, const Error& rhs) noexcept {
    return lhs.code == rhs.code && lhs.native_code == rhs.native_code;
  }
  friend bool operator!=(const Error& lhs, const Error& rhs) noexcept { return !(lhs == rhs); }
};

template <class T>
using Result = std::expected<T, Error>;

using VoidResult = std::expected<void, Error>;

} // namespace uvnet
