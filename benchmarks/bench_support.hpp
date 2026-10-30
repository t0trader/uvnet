#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "uvnet/uvnet.hpp"

namespace bench {

using Clock = std::chrono::steady_clock;

inline std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
          .count());
}

// 防止基准因丢包/卡死而无限等待
class Guard {
 public:
  explicit Guard(uvnet::Loop& loop, std::chrono::seconds limit = std::chrono::seconds(60))
      : timer_(loop) {
    (void)timer_.start(std::chrono::duration_cast<std::chrono::milliseconds>(limit),
                       [&loop] { loop.stop(); });
  }

  void cancel() { timer_.stop(); }

 private:
  uvnet::Timer timer_;
};

inline void print_latency(const char* name, std::vector<std::uint64_t> samples) {
  if (samples.empty()) {
    std::printf("%-36s FAILED (无样本)\n", name);
    return;
  }

  std::sort(samples.begin(), samples.end());
  const double scale = 1.0 / 1000.0;  // ns -> us
  const auto p = [&](double q) {
    const std::size_t index = static_cast<std::size_t>(q * (samples.size() - 1));
    return static_cast<double>(samples[index]) * scale;
  };
  double sum = 0.0;
  for (auto value : samples) {
    sum += static_cast<double>(value) * scale;
  }
  const double avg = sum / static_cast<double>(samples.size());

  std::printf("%-36s avg=%8.1f us  p50=%8.1f us  p99=%8.1f us  max=%9.1f us  n=%zu\n",
              name,
              avg,
              p(0.50),
              p(0.99),
              static_cast<double>(samples.back()) * scale,
              samples.size());
}

inline void print_throughput(const char* name,
                             std::uint64_t bytes,
                             std::uint64_t elapsed_ns,
                             std::uint64_t operations = 0) {
  if (elapsed_ns == 0) {
    std::printf("%-36s FAILED (无耗时)\n", name);
    return;
  }
  const double seconds = static_cast<double>(elapsed_ns) / 1e9;
  const double mib = static_cast<double>(bytes) / (1024.0 * 1024.0);
  if (operations != 0) {
    std::printf("%-36s %9.1f MiB/s  %9.1f kpps  (%8.3f ms)\n",
                name,
                mib / seconds,
                static_cast<double>(operations) / seconds / 1000.0,
                seconds * 1000.0);
  } else {
    std::printf("%-36s %9.1f MiB/s  (%8.3f ms, %.1f MiB)\n",
                name,
                mib / seconds,
                seconds * 1000.0,
                mib);
  }
}

} // namespace bench
