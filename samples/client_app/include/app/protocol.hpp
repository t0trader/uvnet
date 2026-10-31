#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>

namespace app {

enum class MessageType {
  Ping,
  Pong,
};

struct Message {
  MessageType type = MessageType::Ping;
  std::uint64_t sequence = 0;
};

// 线格式：一行一条，"PING <seq>\n" / "PONG <seq>\n"
std::string encode_message(MessageType type, std::uint64_t sequence);

// 解码单行；容忍尾部 \r\n 和空白，序号必须是十进制数字
std::expected<Message, std::string> decode_message(std::string_view line);

// 增量行解码器：处理"一次读只拿到半行"或"一次拿到多行"的情况。
// handler 收到的是完整行（已去掉换行）；调用 handler 前缓冲已经弹出，
// 因此 handler 内部即便重置解码器也不会悬垂。
class LineDecoder {
 public:
  template <typename Handler>
  void feed(std::span<const std::byte> data, Handler&& handler) {
    buffer_.append(reinterpret_cast<const char*>(data.data()), data.size());

    std::size_t newline = 0;
    while ((newline = buffer_.find('\n')) != std::string::npos) {
      std::string line = buffer_.substr(0, newline);
      buffer_.erase(0, newline + 1);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      handler(line);
    }
  }

  void reset() { buffer_.clear(); }
  [[nodiscard]] std::size_t buffered() const { return buffer_.size(); }

 private:
  std::string buffer_;
};

} // namespace app
