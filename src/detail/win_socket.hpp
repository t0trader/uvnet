#pragma once

#if !defined(_WIN32)
#error "win_socket.hpp 仅用于 Windows"
#endif

#include "uvnet/tcp.hpp"
#include "detail/platform.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>

namespace uvnet::detail {

inline int win_socket_setopt(SOCKET socket, int level, int name, const void* value, int length) {
  return ::setsockopt(socket, level, name, reinterpret_cast<const char*>(value), length);
}

inline int win_socket_getopt(SOCKET socket, int level, int name, void* value, int* length) {
  return ::getsockopt(socket, level, name, reinterpret_cast<char*>(value), length);
}

inline bool win_set_nonblocking(SOCKET socket) {
  u_long mode = 1;
  return ::ioctlsocket(socket, FIONBIO, &mode) == 0;
}

// 唤醒对：就绪式 poller 没有 eventfd，用一个环回 socketpair 把 wake() 变成可读事件
inline bool win_create_socket_pair(SOCKET& reader, SOCKET& writer) {
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

// 避免引入 mstcpip.h（部分 SDK 组合下会与 WIN32_LEAN_AND_MEAN 冲突）
struct WinKeepAliveSetting {
  ULONG onoff;
  ULONG keepalivetime;
  ULONG keepaliveinterval;
};

#ifndef SIO_KEEPALIVE_VALS
#define SIO_KEEPALIVE_VALS _WSAIOW(IOC_VENDOR, 4)
#endif

inline VoidResult win_apply_bind_options(SOCKET socket,
                                         Family family,
                                         const BindOptions& options) {
  if (options.reuse_port) {
    return std::unexpected(make_error(ErrorCode::OperationNotSupported, WSAGetLastError()));
  }

  const int one = 1;
  if (options.reuse_address &&
      win_socket_setopt(socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
    return std::unexpected(from_socket_error(WSAGetLastError()));
  }

  if (family == Family::IPv6) {
    const int v6only = options.ipv6_only ? 1 : 0;
    if (win_socket_setopt(socket, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) != 0) {
      return std::unexpected(from_socket_error(WSAGetLastError()));
    }
  }

  return {};
}

inline VoidResult win_apply_keepalive(SOCKET socket, const KeepAliveOptions& options) {
  const int enable = options.enable ? 1 : 0;
  if (win_socket_setopt(socket, SOL_SOCKET, SO_KEEPALIVE, &enable, sizeof(enable)) != 0) {
    return std::unexpected(from_socket_error(WSAGetLastError()));
  }
  if (!options.enable) {
    return {};
  }

  WinKeepAliveSetting setting{};
  setting.onoff = 1;
  setting.keepalivetime = static_cast<ULONG>(std::max<std::int64_t>(
      1, std::chrono::duration_cast<std::chrono::milliseconds>(options.delay).count()));
  setting.keepaliveinterval = setting.keepalivetime;

  DWORD bytes = 0;
  if (::WSAIoctl(socket,
                 SIO_KEEPALIVE_VALS,
                 &setting,
                 sizeof(setting),
                 nullptr,
                 0,
                 &bytes,
                 nullptr,
                 nullptr) != 0) {
    return std::unexpected(from_socket_error(WSAGetLastError()));
  }
  return {};
}

inline Result<SocketAddress> win_socket_local_address(SOCKET socket) {
  sockaddr_storage storage{};
  int length = static_cast<int>(sizeof(storage));
  if (::getsockname(socket, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
    return std::unexpected(from_socket_error(WSAGetLastError()));
  }
  return AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage), length);
}

inline Result<SocketAddress> win_socket_peer_address(SOCKET socket) {
  sockaddr_storage storage{};
  int length = static_cast<int>(sizeof(storage));
  if (::getpeername(socket, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
    return std::unexpected(from_socket_error(WSAGetLastError()));
  }
  return AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage), length);
}

} // namespace uvnet::detail
