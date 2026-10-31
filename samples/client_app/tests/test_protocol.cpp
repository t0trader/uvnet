// 协议层单元测试：不碰网络，只验证编解码和增量拆行

#include "app/protocol.hpp"

#include <cstddef>
#include <span>
#include <string>
#include <vector>

#include "test_support.hpp"

namespace {

using namespace app;

void test_encode() {
  CHECK(encode_message(MessageType::Ping, 0) == "PING 0\n");
  CHECK(encode_message(MessageType::Ping, 42) == "PING 42\n");
  CHECK(encode_message(MessageType::Pong, 1234567) == "PONG 1234567\n");
}

void test_decode() {
  const auto ping = decode_message("PING 7");
  CHECK(ping.has_value());
  if (ping) {
    CHECK(ping->type == MessageType::Ping);
    CHECK(ping->sequence == 7);
  }

  const auto pong = decode_message("PONG 12\n");
  CHECK(pong.has_value());
  if (pong) {
    CHECK(pong->type == MessageType::Pong);
    CHECK(pong->sequence == 12);
  }

  // 容忍 CRLF 和尾部空白
  const auto crlf = decode_message("PONG 9\r\n");
  CHECK(crlf.has_value());
  if (crlf) {
    CHECK(crlf->sequence == 9);
  }

  CHECK(!decode_message("").has_value());
  CHECK(!decode_message("PING").has_value());
  CHECK(!decode_message("PING ").has_value());
  CHECK(!decode_message("HELLO 3").has_value());
  CHECK(!decode_message("PING abc").has_value());
  CHECK(!decode_message("PING -1").has_value());
  CHECK(!decode_message("PING 1x").has_value());
}

void test_line_decoder() {
  LineDecoder decoder;
  std::vector<std::string> lines;
  const auto collect = [&lines](const std::string& line) { lines.push_back(line); };

  // 半行：先喂 "PI"，再喂 "NG 1\n"
  const std::string first = "PI";
  const std::string second = "NG 1\n";
  decoder.feed(std::span<const std::byte>(reinterpret_cast<const std::byte*>(first.data()),
                                          first.size()),
               collect);
  CHECK(lines.empty());
  CHECK(decoder.buffered() == 2);

  decoder.feed(std::span<const std::byte>(reinterpret_cast<const std::byte*>(second.data()),
                                          second.size()),
               collect);
  CHECK(lines.size() == 1);
  CHECK(lines[0] == "PING 1");
  CHECK(decoder.buffered() == 0);

  // 一次多行 + CRLF
  const std::string multi = "PING 2\r\nPONG 3\n";
  decoder.feed(std::span<const std::byte>(reinterpret_cast<const std::byte*>(multi.data()),
                                          multi.size()),
               collect);
  CHECK(lines.size() == 3);
  CHECK(lines[1] == "PING 2");
  CHECK(lines[2] == "PONG 3");

  decoder.reset();
  CHECK(decoder.buffered() == 0);
}

} // namespace

int main() {
  test_encode();
  test_decode();
  test_line_decoder();
  return apptest::finish("protocol");
}
