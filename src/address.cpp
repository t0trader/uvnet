#include "uvnet/address.hpp"

#include <charconv>
#include <cstdlib>
#include <cstring>

#include "detail/platform.hpp"

namespace uvnet {
namespace {

#if defined(_WIN32)
int pton(int family, const char* src, void* dst) { return InetPtonA(family, src, dst); }
int ntop(int family, const void* src, char* dst, std::size_t size) {
  return InetNtopA(family, src, dst, static_cast<DWORD>(size)) != nullptr ? 0 : -1;
}
#else
int pton(int family, const char* src, void* dst) { return inet_pton(family, src, dst); }
int ntop(int family, const void* src, char* dst, std::size_t size) {
  return inet_ntop(family, src, dst, static_cast<int>(size)) != nullptr ? 0 : -1;
}
#endif

bool parse_scope(std::string_view ip, std::string_view& base, std::uint32_t& scope_id) {
  const std::size_t percent = ip.find('%');
  if (percent == std::string_view::npos) {
    base = ip;
    scope_id = 0;
    return true;
  }
  base = ip.substr(0, percent);
  const std::string_view tail = ip.substr(percent + 1);
  if (tail.empty()) {
    return false;
  }
  std::uint32_t value = 0;
  const char* begin = tail.data();
  const char* end = tail.data() + tail.size();
  const auto result = std::from_chars(begin, end, value);
  if (result.ec != std::errc{} || result.ptr != end) {
    return false;
  }
  scope_id = value;
  return true;
}

Result<std::uint16_t> parse_port(std::string_view text) {
  if (text.empty()) {
    return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
  }
  std::uint32_t value = 0;
  const char* begin = text.data();
  const char* end = text.data() + text.size();
  const auto result = std::from_chars(begin, end, value);
  if (result.ec != std::errc{} || result.ptr != end || value > 65535) {
    return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
  }
  return static_cast<std::uint16_t>(value);
}

} // namespace

Result<SocketAddress> SocketAddress::parse(std::string_view ip, std::uint16_t port) {
  detail::ensure_winsock();

  if (ip.empty()) {
    return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
  }

  std::string_view base;
  std::uint32_t scope_id = 0;
  if (!parse_scope(ip, base, scope_id)) {
    return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
  }

  SocketAddress address;
  std::string text(base);

  in_addr v4{};
  if (pton(AF_INET, text.c_str(), &v4) == 1) {
    address.family_ = Family::IPv4;
    std::memcpy(address.bytes_.data(), &v4, 4);
    address.port_ = port;
    address.scope_id_ = 0;
    return address;
  }

  in6_addr v6{};
  if (pton(AF_INET6, text.c_str(), &v6) == 1) {
    address.family_ = Family::IPv6;
    std::memcpy(address.bytes_.data(), &v6, 16);
    address.port_ = port;
    address.scope_id_ = scope_id;
    return address;
  }

  return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
}

Result<SocketAddress> SocketAddress::parse(std::string_view ip_port) {
  if (ip_port.empty()) {
    return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
  }

  if (ip_port.front() == '[') {
    const std::size_t closing = ip_port.find(']');
    if (closing == std::string_view::npos) {
      return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
    }
    const std::string_view ip = ip_port.substr(1, closing - 1);
    if (closing + 1 == ip_port.size()) {
      return parse(ip, 0);
    }
    if (ip_port[closing + 1] != ':') {
      return std::unexpected(detail::make_error(ErrorCode::InvalidArgument));
    }
    const auto port = parse_port(ip_port.substr(closing + 2));
    if (!port) {
      return std::unexpected(port.error());
    }
    return parse(ip, *port);
  }

  const std::size_t first_colon = ip_port.find(':');
  if (first_colon == std::string_view::npos || ip_port.find(':', first_colon + 1) != std::string_view::npos) {
    // 不含端口，或裸 IPv6 字面量（多个冒号）
    return parse(ip_port, 0);
  }

  const auto port = parse_port(ip_port.substr(first_colon + 1));
  if (!port) {
    return std::unexpected(port.error());
  }
  return parse(ip_port.substr(0, first_colon), *port);
}

SocketAddress SocketAddress::any(Family family, std::uint16_t port) {
  SocketAddress address;
  address.family_ = family;
  address.bytes_.fill(std::byte{0});
  address.port_ = port;
  return address;
}

SocketAddress SocketAddress::loopback(Family family, std::uint16_t port) {
  SocketAddress address;
  address.family_ = family;
  address.bytes_.fill(std::byte{0});
  if (family == Family::IPv4) {
    address.bytes_[0] = static_cast<std::byte>(127);
    address.bytes_[3] = static_cast<std::byte>(1);
  } else if (family == Family::IPv6) {
    address.bytes_[15] = static_cast<std::byte>(1);
  }
  address.port_ = port;
  return address;
}

std::string SocketAddress::ip() const {
  if (family_ == Family::Unspecified) {
    return {};
  }

  in_addr v4{};
  in6_addr v6{};
  char buffer[64] = {};

  if (family_ == Family::IPv4) {
    std::memcpy(&v4, bytes_.data(), 4);
    if (ntop(AF_INET, &v4, buffer, sizeof(buffer)) != 0) {
      return {};
    }
    return std::string(buffer);
  }

  std::memcpy(&v6, bytes_.data(), 16);
  if (ntop(AF_INET6, &v6, buffer, sizeof(buffer)) != 0) {
    return {};
  }
  std::string text(buffer);
  if (scope_id_ != 0) {
    text.push_back('%');
    text += std::to_string(scope_id_);
  }
  return text;
}

std::string SocketAddress::to_string() const {
  const std::string text = ip();
  if (family_ == Family::IPv6) {
    return "[" + text + "]:" + std::to_string(port_);
  }
  return text + ":" + std::to_string(port_);
}

bool SocketAddress::is_loopback() const {
  if (family_ == Family::IPv4) {
    return bytes_[0] == static_cast<std::byte>(127);
  }
  if (family_ == Family::IPv6) {
    for (std::size_t i = 0; i < 15; ++i) {
      if (bytes_[i] != std::byte{0}) {
        return false;
      }
    }
    return bytes_[15] == std::byte{1};
  }
  return false;
}

bool SocketAddress::is_unspecified() const {
  if (family_ == Family::Unspecified) {
    return true;
  }
  for (const auto value : bytes_) {
    if (value != std::byte{0}) {
      return false;
    }
  }
  return true;
}

bool operator==(const SocketAddress& lhs, const SocketAddress& rhs) noexcept {
  return lhs.family_ == rhs.family_ && lhs.bytes_ == rhs.bytes_ &&
         lhs.port_ == rhs.port_ && lhs.scope_id_ == rhs.scope_id_;
}

namespace detail {

bool AddressCodec::to_native(const SocketAddress& address,
                             sockaddr_storage& storage,
                             int& length) {
  std::memset(&storage, 0, sizeof(storage));

  if (address.family() == Family::IPv4) {
    auto* v4 = reinterpret_cast<sockaddr_in*>(&storage);
    v4->sin_family = AF_INET;
    v4->sin_port = htons(address.port());
    std::memcpy(&v4->sin_addr, address.bytes_.data(), 4);
    length = static_cast<int>(sizeof(sockaddr_in));
    return true;
  }

  if (address.family() == Family::IPv6) {
    auto* v6 = reinterpret_cast<sockaddr_in6*>(&storage);
    v6->sin6_family = AF_INET6;
    v6->sin6_port = htons(address.port());
    std::memcpy(&v6->sin6_addr, address.bytes_.data(), 16);
    v6->sin6_scope_id = address.scope_id();
    length = static_cast<int>(sizeof(sockaddr_in6));
    return true;
  }

  length = 0;
  return false;
}

SocketAddress AddressCodec::from_native(const sockaddr* address, int length) {
  SocketAddress result;
  if (address == nullptr) {
    return result;
  }

  if (address->sa_family == AF_INET && length >= static_cast<int>(sizeof(sockaddr_in))) {
    const auto* v4 = reinterpret_cast<const sockaddr_in*>(address);
    result.family_ = Family::IPv4;
    std::memcpy(result.bytes_.data(), &v4->sin_addr, 4);
    result.port_ = ntohs(v4->sin_port);
    return result;
  }

  if (address->sa_family == AF_INET6 && length >= static_cast<int>(sizeof(sockaddr_in6))) {
    const auto* v6 = reinterpret_cast<const sockaddr_in6*>(address);
    result.family_ = Family::IPv6;
    std::memcpy(result.bytes_.data(), &v6->sin6_addr, 16);
    result.port_ = ntohs(v6->sin6_port);
    result.scope_id_ = v6->sin6_scope_id;
    return result;
  }

  return result;
}

int AddressCodec::native_family(Family family) noexcept {
  switch (family) {
    case Family::IPv4: return AF_INET;
    case Family::IPv6: return AF_INET6;
    default: return AF_UNSPEC;
  }
}

Family AddressCodec::family_from_native(int family) noexcept {
  switch (family) {
    case AF_INET: return Family::IPv4;
    case AF_INET6: return Family::IPv6;
    default: return Family::Unspecified;
  }
}

std::string AddressCodec::ip_from_native(const sockaddr* address) {
  if (address == nullptr) {
    return {};
  }
  char buffer[64] = {};
  if (address->sa_family == AF_INET) {
    const auto* v4 = reinterpret_cast<const sockaddr_in*>(address);
    if (ntop(AF_INET, &v4->sin_addr, buffer, sizeof(buffer)) != 0) {
      return {};
    }
    return std::string(buffer);
  }
  if (address->sa_family == AF_INET6) {
    const auto* v6 = reinterpret_cast<const sockaddr_in6*>(address);
    if (ntop(AF_INET6, &v6->sin6_addr, buffer, sizeof(buffer)) != 0) {
      return {};
    }
    std::string text(buffer);
    if (v6->sin6_scope_id != 0) {
      text.push_back('%');
      text += std::to_string(v6->sin6_scope_id);
    }
    return text;
  }
  return {};
}

int AddressCodec::native_length(Family family) noexcept {
  switch (family) {
    case Family::IPv4: return static_cast<int>(sizeof(sockaddr_in));
    case Family::IPv6: return static_cast<int>(sizeof(sockaddr_in6));
    default: return 0;
  }
}

} // namespace detail
} // namespace uvnet
