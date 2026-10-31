#include <string>
#include <vector>

#include "test_support.hpp"

namespace {

using namespace uvnet;

void test_resolve_localhost(Resolver& resolver, int& done, uvtest::Guard& guard) {
  ResolveOptions options;
  resolver.resolve("localhost", 8080, options, [&done, &guard](Result<std::vector<SocketAddress>> result) {
    CHECK(result.has_value());
    if (result) {
      CHECK(!result->empty());
      bool has_loopback = false;
      for (const auto& address : *result) {
        CHECK(address.family() == Family::IPv4 || address.family() == Family::IPv6);
        has_loopback = has_loopback || address.is_loopback();
      }
      CHECK(has_loopback);
    }
    if (++done == 5) {
      guard.cancel();
    }
  });
}

void test_numeric(Backend backend) {
  Loop loop(backend);
  uvtest::Guard guard(loop, std::chrono::seconds(15));
  Resolver resolver(loop);

  int done = 0;

  ResolveOptions numeric_ipv4;
  numeric_ipv4.numeric_host = true;
  resolver.resolve("127.0.0.1", 53, numeric_ipv4,
                   [&done, &guard](Result<std::vector<SocketAddress>> result) {
                     CHECK(result.has_value());
                     if (result) {
                       CHECK(!result->empty());
                       if (!result->empty()) {
                         CHECK(result->front().family() == Family::IPv4);
                         CHECK(result->front().is_loopback());
                       }
                     }
                     if (++done == 5) guard.cancel();
                   });

  ResolveOptions numeric_ipv6;
  numeric_ipv6.numeric_host = true;
  numeric_ipv6.family = Family::IPv6;
  resolver.resolve("::1", 53, numeric_ipv6,
                   [&done, &guard](Result<std::vector<SocketAddress>> result) {
                     CHECK(result.has_value());
                     if (result) {
                       CHECK(!result->empty());
                       if (!result->empty()) {
                         CHECK(result->front().family() == Family::IPv6);
                         CHECK(result->front().is_loopback());
                       }
                     }
                     if (++done == 5) guard.cancel();
                   });

  ResolveOptions invalid;
  invalid.numeric_host = true;
  resolver.resolve("not-an-ip", 53, invalid,
                   [&done, &guard](Result<std::vector<SocketAddress>> result) {
                     CHECK(!result.has_value());
                     if (!result) {
                       CHECK(result.error().code == ErrorCode::HostNotFound ||
                             result.error().code == ErrorCode::InvalidArgument);
                     }
                     if (++done == 5) guard.cancel();
                   });

  resolver.reverse(SocketAddress::loopback(Family::IPv4, 0),
                   [&done, &guard](Result<std::string> name) {
                     CHECK(name.has_value());
                     if (name) {
                       CHECK(!name->empty());
                     }
                     if (++done == 5) guard.cancel();
                   });

  test_resolve_localhost(resolver, done, guard);

  loop.run();
  CHECK(done == 5);
}

} // namespace

int main() {
#if defined(_WIN32)
  const Backend backends[] = {Backend::Iocp, Backend::Readiness};
#else
  const Backend backends[] = {Backend::Epoll};
#endif
  for (const Backend backend : backends) {
    test_numeric(backend);
  }
  return uvtest::finish("dns");
}
