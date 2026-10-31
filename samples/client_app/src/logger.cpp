#include "app/logger.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string>

namespace app {

namespace {

std::mutex g_mutex;
LogLevel g_level = LogLevel::Info;

const char* level_name(LogLevel level) {
  switch (level) {
    case LogLevel::Debug:
      return "debug";
    case LogLevel::Info:
      return "info";
    case LogLevel::Warn:
      return "warn";
    case LogLevel::Error:
      return "error";
  }
  return "info";
}

} // namespace

void set_log_level(LogLevel level) {
  const std::lock_guard<std::mutex> lock(g_mutex);
  g_level = level;
}

LogLevel log_level() {
  const std::lock_guard<std::mutex> lock(g_mutex);
  return g_level;
}

bool parse_log_level(std::string_view name, LogLevel& out) {
  std::string lowered(name);
  for (char& c : lowered) {
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
  }

  if (lowered == "debug") {
    out = LogLevel::Debug;
  } else if (lowered == "info") {
    out = LogLevel::Info;
  } else if (lowered == "warn") {
    out = LogLevel::Warn;
  } else if (lowered == "error") {
    out = LogLevel::Error;
  } else {
    return false;
  }
  return true;
}

void log(LogLevel level, std::string_view message) {
  const std::lock_guard<std::mutex> lock(g_mutex);
  if (static_cast<int>(level) < static_cast<int>(g_level)) {
    return;
  }

  const auto now = std::chrono::system_clock::now();
  const auto since_epoch = now.time_since_epoch();
  const auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch) % 1000;
  const std::time_t seconds = std::chrono::system_clock::to_time_t(now);

  std::tm local{};
#ifdef _WIN32
  localtime_s(&local, &seconds);
#else
  localtime_r(&seconds, &local);
#endif

  std::printf("%02d:%02d:%02d.%03d [%s] %.*s\n",
              local.tm_hour,
              local.tm_min,
              local.tm_sec,
              static_cast<int>(millis.count()),
              level_name(level),
              static_cast<int>(message.size()),
              message.data());
  std::fflush(stdout);
}

} // namespace app
