#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "app/config.hpp"
#include "uvnet/uvnet.hpp"

namespace app {

// 本地联调用的回显服务器：收到 "PING <n>" 回复 "PONG <n>"，TCP / UDP 各一套。
//
// 与 ClientSession 共用同一份协议解码器，保证两端线格式一致。
class EchoServer final {
 public:
  EchoServer(uvnet::Loop& loop, Protocol protocol, std::uint16_t port);
  ~EchoServer();

  EchoServer(const EchoServer&) = delete;
  EchoServer& operator=(const EchoServer&) = delete;

  // 绑定 + 开始监听/收包；失败时 error 带原因
  bool start(std::string& error);

  // 监听端口为 0 时，用它拿到实际端口
  [[nodiscard]] uvnet::Result<uvnet::SocketAddress> local_address() const;

  void close();

 private:
  void start_tcp(std::string& error);
  void start_udp(std::string& error);

  uvnet::Loop& loop_;
  Protocol protocol_;
  std::uint16_t port_;
  uvnet::TcpListener listener_;
  std::unique_ptr<uvnet::UdpSocket> socket_;
};

} // namespace app
