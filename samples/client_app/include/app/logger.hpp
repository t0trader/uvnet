#pragma once

#include <string>
#include <string_view>

namespace app {

enum class LogLevel {
  Debug = 0,
  Info,
  Warn,
  Error,
};

// 全局日志级别；默认 Info
void set_log_level(LogLevel level);
[[nodiscard]] LogLevel log_level();

// "debug" / "info" / "warn" / "error"，不区分大小写
bool parse_log_level(std::string_view name, LogLevel& out);

// 线程安全：循环线程和 post 回调里都可以直接用
void log(LogLevel level, std::string_view message);

inline void log_debug(std::string_view message) { log(LogLevel::Debug, message); }
inline void log_info(std::string_view message) { log(LogLevel::Info, message); }
inline void log_warn(std::string_view message) { log(LogLevel::Warn, message); }
inline void log_error(std::string_view message) { log(LogLevel::Error, message); }

} // namespace app
