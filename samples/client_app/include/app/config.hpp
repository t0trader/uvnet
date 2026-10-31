#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "app/logger.hpp"

namespace app {

enum class Protocol {
  Tcp,
  Udp,
};

// 客户端运行参数，命令行解析后不可变
struct Config {
  std::string host = "127.0.0.1";
  std::uint16_t port = 9000;
  Protocol protocol = Protocol::Tcp;

  int requests = 5;               // 请求/响应轮数
  int interval_ms = 20;           // 两轮之间的间隔
  int request_timeout_ms = 3000;  // 单轮响应超时，超时视为连接失效
  int connect_timeout_ms = 3000;  // 单个候选地址的连接超时
  int max_retries = 2;            // 连接失败/超时后的重试次数

  std::size_t read_slots = 4;  // Windows 多 outstanding 读槽数量（Linux 忽略）
  std::size_t read_chunk = 32 * 1024;

  LogLevel log_level = LogLevel::Info;
};

enum class ArgOutcome {
  Run,
  Exit,   // 打印了帮助，正常退出
  Error,  // 参数错误
};

ArgOutcome parse_args(int argc, char** argv, Config& config, std::string& error);
void print_usage(const char* program);

} // namespace app
