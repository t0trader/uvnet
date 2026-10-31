#include "app/protocol.hpp"

#include <cstdint>
#include <string>

namespace app {

std::string encode_message(MessageType type, std::uint64_t sequence) {
  std::string out = type == MessageType::Ping ? "PING " : "PONG ";
  out += std::to_string(sequence);
  out += '\n';
  return out;
}

std::expected<Message, std::string> decode_message(std::string_view line) {
  // 容忍尾部空白：UDP 报文没有换行，也可能带 \r
  while (!line.empty() &&
         (line.back() == '\r' || line.back() == '\n' || line.back() == ' ' ||
          line.back() == '\t')) {
    line.remove_suffix(1);
  }

  const std::size_t space = line.find(' ');
  if (space == std::string_view::npos) {
    return std::unexpected("缺少字段: " + std::string(line));
  }

  const std::string_view name = line.substr(0, space);
  const std::string_view sequence_text = line.substr(space + 1);

  MessageType type{};
  if (name == "PING") {
    type = MessageType::Ping;
  } else if (name == "PONG") {
    type = MessageType::Pong;
  } else {
    return std::unexpected("未知消息类型: " + std::string(name));
  }

  if (sequence_text.empty()) {
    return std::unexpected("缺少序号");
  }

  std::uint64_t sequence = 0;
  for (const char c : sequence_text) {
    if (c < '0' || c > '9') {
      return std::unexpected("序号不是十进制数字: " + std::string(sequence_text));
    }
    sequence = sequence * 10 + static_cast<std::uint64_t>(c - '0');
  }

  return Message{type, sequence};
}

} // namespace app
