#include "app/config.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

namespace app {

namespace {

bool read_int(int argc,
              char** argv,
              int& index,
              int minimum,
              const char* name,
              int& out,
              std::string& error) {
  if (index + 1 >= argc) {
    error = std::string(name) + " 缺少参数";
    return false;
  }
  ++index;
  const int value = std::atoi(argv[index]);
  if (value < minimum) {
    error = std::string(name) + " 需要 >= " + std::to_string(minimum);
    return false;
  }
  out = value;
  return true;
}

} // namespace

void print_usage(const char* program) {
  std::printf(
      "用法: %s [选项] <host> <port>\n"
      "\n选项:\n"
      "  --udp                 使用 UDP（默认 TCP）\n"
      "  --count N             请求/响应轮数（默认 5）\n"
      "  --interval MS         两轮之间的间隔，毫秒（默认 20）\n"
      "  --timeout MS          单轮响应超时，毫秒（默认 3000）\n"
      "  --connect-timeout MS  单个地址的连接超时，毫秒（默认 3000）\n"
      "  --retries N           失败后的重试次数（默认 2）\n"
      "  --slots N             读/收包槽位数量，Windows IOCP 生效（默认 4）\n"
      "  --chunk BYTES         单次读缓冲字节数（默认 32768）\n"
      "  --log-level LEVEL     debug / info / warn / error（默认 info）\n"
      "  --help                显示本帮助\n",
      program);
}

ArgOutcome parse_args(int argc, char** argv, Config& config, std::string& error) {
  std::vector<std::string> positional;

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];

    if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      return ArgOutcome::Exit;
    }
    if (arg == "--udp") {
      config.protocol = Protocol::Udp;
      continue;
    }
    if (arg == "--count") {
      if (!read_int(argc, argv, i, 1, "--count", config.requests, error)) {
        return ArgOutcome::Error;
      }
      continue;
    }
    if (arg == "--interval") {
      if (!read_int(argc, argv, i, 0, "--interval", config.interval_ms, error)) {
        return ArgOutcome::Error;
      }
      continue;
    }
    if (arg == "--timeout") {
      if (!read_int(argc, argv, i, 1, "--timeout", config.request_timeout_ms, error)) {
        return ArgOutcome::Error;
      }
      continue;
    }
    if (arg == "--connect-timeout") {
      if (!read_int(argc, argv, i, 1, "--connect-timeout", config.connect_timeout_ms, error)) {
        return ArgOutcome::Error;
      }
      continue;
    }
    if (arg == "--retries") {
      if (!read_int(argc, argv, i, 0, "--retries", config.max_retries, error)) {
        return ArgOutcome::Error;
      }
      continue;
    }
    if (arg == "--slots") {
      int slots = static_cast<int>(config.read_slots);
      if (!read_int(argc, argv, i, 1, "--slots", slots, error)) {
        return ArgOutcome::Error;
      }
      if (slots > 64) {
        error = "--slots 上限为 64";
        return ArgOutcome::Error;
      }
      config.read_slots = static_cast<std::size_t>(slots);
      continue;
    }
    if (arg == "--chunk") {
      int chunk = static_cast<int>(config.read_chunk);
      if (!read_int(argc, argv, i, 1024, "--chunk", chunk, error)) {
        return ArgOutcome::Error;
      }
      config.read_chunk = static_cast<std::size_t>(chunk);
      continue;
    }
    if (arg == "--log-level") {
      if (i + 1 >= argc) {
        error = "--log-level 缺少参数";
        return ArgOutcome::Error;
      }
      ++i;
      if (!parse_log_level(argv[i], config.log_level)) {
        error = std::string("未知日志级别: ") + argv[i];
        return ArgOutcome::Error;
      }
      continue;
    }
    if (arg.size() > 2 && arg.substr(0, 2) == "--") {
      error = "未知选项: " + std::string(arg);
      return ArgOutcome::Error;
    }
    positional.emplace_back(arg);
  }

  if (positional.size() < 2) {
    error = "缺少 host 或 port";
    return ArgOutcome::Error;
  }
  config.host = positional[0];

  const int port = std::atoi(positional[1].c_str());
  if (port <= 0 || port > 65535) {
    error = "端口无效: " + positional[1];
    return ArgOutcome::Error;
  }
  config.port = static_cast<std::uint16_t>(port);

  return ArgOutcome::Run;
}

} // namespace app
