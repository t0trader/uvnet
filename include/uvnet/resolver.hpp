#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "uvnet/address.hpp"
#include "uvnet/loop.hpp"

namespace uvnet {

namespace detail {
class ResolverImpl;
} // namespace detail

enum class SocketType : std::uint8_t {
  Any,
  Stream,
  Datagram,
};

struct ResolveOptions {
  Family family = Family::Unspecified;
  SocketType socket_type = SocketType::Any;
  // true 时允许 host 为空，返回通配地址（用于监听）
  bool passive = false;
  // true 时 host 必须是数字地址，不发起 DNS 查询
  bool numeric_host = false;
};

// 基于线程池 + getaddrinfo 的异步 DNS 解析器。
class Resolver {
 public:
  using ResolveCallback = std::function<void(Result<std::vector<SocketAddress>>)>;
  using ReverseCallback = std::function<void(Result<std::string>)>;

  explicit Resolver(Loop& loop);
  ~Resolver();

  Resolver(const Resolver&) = default;
  Resolver& operator=(const Resolver&) = default;
  Resolver(Resolver&&) noexcept = default;
  Resolver& operator=(Resolver&&) noexcept = default;

  // host 可为 IPv4/IPv6 字面量或域名；回调在循环线程执行
  void resolve(std::string host,
               std::uint16_t port,
               ResolveOptions options,
               ResolveCallback cb);
  // 反向解析 IP，返回主机名（失败时可能返回数字地址字符串）
  void reverse(const SocketAddress& address, ReverseCallback cb);

 private:
  std::shared_ptr<detail::ResolverImpl> impl_;
};

} // namespace uvnet
