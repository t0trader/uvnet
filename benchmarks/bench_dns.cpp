#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "bench_support.hpp"

namespace {

using namespace uvnet;

void run_resolve(const char* label,
                 std::string host,
                 ResolveOptions options,
                 std::size_t count) {
  Loop loop;
  bench::Guard guard(loop, std::chrono::seconds(60));
  Resolver resolver(loop);

  bool failed = false;
  bool completed = false;
  std::size_t index = 0;
  std::uint64_t start_ns = 0;
  std::vector<std::uint64_t> samples;
  samples.reserve(count);

  std::function<void()> next;
  next = [&] {
    if (index >= count) {
      completed = true;
      guard.cancel();
      return;
    }

    start_ns = bench::now_ns();
    resolver.resolve(host, 80, options, [&](Result<std::vector<SocketAddress>> result) {
      if (!result) {
        failed = true;
        guard.cancel();
        return;
      }
      samples.push_back(bench::now_ns() - start_ns);
      ++index;
      next();
    });
  };

  next();
  loop.run();

  if (failed || !completed) {
    std::printf("%s: 失败或超时 (%zu/%zu)\n", label, index, count);
    return;
  }

  bench::print_latency(label, std::move(samples));
}

} // namespace

int main() {
  std::printf("== DNS ==\n");

  ResolveOptions hostname;
  run_resolve("dns localhost", "localhost", hostname, 2000);

  ResolveOptions numeric;
  numeric.numeric_host = true;
  run_resolve("dns numeric 127.0.0.1", "127.0.0.1", numeric, 2000);

  return 0;
}
