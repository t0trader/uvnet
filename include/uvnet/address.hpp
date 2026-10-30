#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>

#include "uvnet/error.hpp"

namespace uvnet {

enum class Family : std::uint8_t {
  Unspecified = 0,
  IPv4 = 4,
  IPv6 = 6,
};

namespace detail {
struct AddressCodec;
class UdpSocketImpl;
} // namespace detail

// 不依赖任何平台头文件的地址表示：IPv4 存前 4 字节，IPv6 存全部 16 字节
class SocketAddress {
 public:
  SocketAddress() = default;

  // 解析纯 IP 字面量（不支持主机名，主机名请使用 Resolver）
  static Result<SocketAddress> parse(std::string_view ip, std::uint16_t port);
  // 解析 "1.2.3.4:80" 或 "[::1]:80"；无端口时端口为 0
  static Result<SocketAddress> parse(std::string_view ip_port);

  static SocketAddress any(Family family, std::uint16_t port);
  static SocketAddress loopback(Family family, std::uint16_t port);

  [[nodiscard]] Family family() const noexcept { return family_; }
  [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
  [[nodiscard]] std::uint32_t scope_id() const noexcept { return scope_id_; }
  void set_scope_id(std::uint32_t id) noexcept { scope_id_ = id; }

  // 纯 IP 字符串（IPv6 带 %scope）
  [[nodiscard]] std::string ip() const;
  // 带端口字符串：[::1]:8080
  [[nodiscard]] std::string to_string() const;

  [[nodiscard]] bool is_loopback() const;
  [[nodiscard]] bool is_unspecified() const;

  friend bool operator==(const SocketAddress& lhs, const SocketAddress& rhs) noexcept;
  friend bool operator!=(const SocketAddress& lhs, const SocketAddress& rhs) noexcept {
    return !(lhs == rhs);
  }

 private:
  friend struct detail::AddressCodec;
  friend class detail::UdpSocketImpl;

  Family family_ = Family::Unspecified;
  std::array<std::byte, 16> bytes_{};
  std::uint16_t port_ = 0;
  std::uint32_t scope_id_ = 0;
};

} // namespace uvnet
