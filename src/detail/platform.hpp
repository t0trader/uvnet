#pragma once

#include <string>

#include "uvnet/address.hpp"
#include "uvnet/error.hpp"

#if defined(_WIN32)
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601  // Win7+，暴露 WSARecvMsg/WSASendMsg
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace uvnet::detail {

// 平台相关的 socket 初始化（Windows 上是 WSAStartup 的一次性守卫）
#if defined(_WIN32)
void ensure_winsock();
#else
inline void ensure_winsock() noexcept {}
#endif

[[nodiscard]] int last_socket_error() noexcept;
// 把 POSIX errno 或 Winsock WSA 错误转换为归一化错误
[[nodiscard]] Error from_socket_error(int code) noexcept;
// 把 getaddrinfo / getnameinfo 错误转换为归一化错误
[[nodiscard]] Error from_gai_error(int code) noexcept;
[[nodiscard]] Error make_error(ErrorCode code, int native_code = 0) noexcept;

// SocketAddress 与平台 sockaddr 之间的转换
struct AddressCodec {
  static bool to_native(const SocketAddress& address, sockaddr_storage& storage, int& length);
  static SocketAddress from_native(const sockaddr* address, int length);
  [[nodiscard]] static int native_family(Family family) noexcept;
  [[nodiscard]] static Family family_from_native(int family) noexcept;
  [[nodiscard]] static std::string ip_from_native(const sockaddr* address);
  [[nodiscard]] static int native_length(Family family) noexcept;
};

} // namespace uvnet::detail
