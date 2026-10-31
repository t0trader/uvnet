#include "app/echo_server.hpp"

#include "app/logger.hpp"
#include "app/protocol.hpp"

#include <memory>
#include <span>
#include <string>
#include <utility>

namespace app {

namespace {

using uvnet::ErrorCode;
using uvnet::Family;
using uvnet::ReadOptions;
using uvnet::Result;
using uvnet::SocketAddress;
using uvnet::SpanReadCallback;
using uvnet::TcpStream;
using uvnet::UdpRecvOptions;
using uvnet::WriteBuffer;

constexpr std::size_t kReadSlots = 4;
constexpr std::size_t kChunkSize = 32 * 1024;

std::string encode_pong(std::uint64_t sequence) {
  return encode_message(MessageType::Pong, sequence);
}

// 一个 TCP 连接：按行解析 PING，回复 PONG
class EchoConnection final : public std::enable_shared_from_this<EchoConnection> {
 public:
  explicit EchoConnection(TcpStream stream) : stream_(std::move(stream)) {}

  void start() {
    ReadOptions options;
    options.slots = kReadSlots;
    options.chunk_size = kChunkSize;
    if (const auto applied = stream_.set_read_options(options); !applied) {
      log_warn("配置读窗口失败: " + applied.error().message());
    }
    if (const auto applied = stream_.set_nodelay(true); !applied) {
      log_debug("设置 TCP_NODELAY 失败: " + applied.error().message());
    }

    auto self = shared_from_this();
    const auto started =
        stream_.start_read(SpanReadCallback([self](Result<std::span<const std::byte>> chunk) {
          if (!chunk) {
            if (chunk.error().code == ErrorCode::Canceled) {
              return;
            }
            self->stream_.close();
            return;
          }
          self->decoder_.feed(*chunk, [self](const std::string& line) { self->on_line(line); });
        }));
    if (!started) {
      stream_.close();
    }
  }

 private:
  void on_line(const std::string& line) {
    const auto message = decode_message(line);
    if (!message || message->type != MessageType::Ping) {
      log_debug("忽略非 PING 消息");
      return;
    }

    auto response = std::make_shared<std::string>(encode_pong(message->sequence));
    const auto bytes = std::as_bytes(std::span<const char>(response->data(), response->size()));
    // 零拷贝写：response 的 shared_ptr 作为 keepalive
    if (const auto submitted = stream_.write(WriteBuffer{bytes, response}); !submitted) {
      stream_.close();
    }
  }

  TcpStream stream_;
  LineDecoder decoder_;
};

} // namespace

EchoServer::EchoServer(uvnet::Loop& loop, Protocol protocol, std::uint16_t port)
    : loop_(loop), protocol_(protocol), port_(port), listener_(loop) {}

EchoServer::~EchoServer() {
  close();
}

bool EchoServer::start(std::string& error) {
  if (protocol_ == Protocol::Udp) {
    start_udp(error);
  } else {
    start_tcp(error);
  }
  return error.empty();
}

void EchoServer::start_tcp(std::string& error) {
  const auto bound =
      listener_.bind(SocketAddress::any(Family::IPv4, port_), uvnet::BindOptions{.reuse_address = true});
  if (!bound) {
    error = "绑定失败: " + bound.error().message();
    return;
  }

  // accept 回调不捕获 this：EchoServer 被销毁后监听器也随之关闭，不需要状态
  const auto listening = listener_.listen(64, [](Result<TcpStream> accepted) {
    if (!accepted) {
      if (accepted.error().code == ErrorCode::Canceled) {
        return;
      }
      log_warn("accept 失败: " + accepted.error().message());
      return;
    }

    std::string peer = "?";
    if (const auto address = accepted->peer_address()) {
      peer = address->to_string();
    }
    log_debug("接受连接 " + peer);

    auto connection = std::make_shared<EchoConnection>(std::move(*accepted));
    connection->start();
  });
  if (!listening) {
    error = "监听失败: " + listening.error().message();
    listener_.close();
  }
}

void EchoServer::start_udp(std::string& error) {
  socket_ = std::make_unique<uvnet::UdpSocket>(loop_, Family::IPv4);
  if (const auto bound = socket_->bind(SocketAddress::any(Family::IPv4, port_)); !bound) {
    error = "绑定失败: " + bound.error().message();
    socket_.reset();
    return;
  }

  UdpRecvOptions options;
  options.slots = kReadSlots;
  options.chunk_size = kChunkSize;
  if (const auto applied = socket_->set_recv_options(options); !applied) {
    log_warn("配置收包窗口失败: " + applied.error().message());
  }

  // 收包回调不捕获 this：这里拷一份句柄，它和 socket_ 共享同一个实现，
  // 即使 EchoServer 被销毁，句柄副本仍然可用
  uvnet::UdpSocket handle = *socket_;
  const auto started =
      socket_->start_recv([handle](Result<uvnet::DatagramView> datagram) mutable {
        if (!datagram) {
          if (datagram.error().code == ErrorCode::Canceled) {
            return;
          }
          log_warn("接收失败: " + datagram.error().message());
          return;
        }

        const std::string line(reinterpret_cast<const char*>(datagram->data.data()),
                               datagram->data.size());
        const auto message = decode_message(line);
        if (!message || message->type != MessageType::Ping) {
          return;
        }

        auto response = std::make_shared<std::string>(encode_pong(message->sequence));
        const auto bytes =
            std::as_bytes(std::span<const char>(response->data(), response->size()));
        if (const auto submitted =
                handle.send_to(datagram->source, WriteBuffer{bytes, response});
            !submitted) {
          log_debug("回复失败: " + submitted.error().message());
        }
      });
  if (!started) {
    error = "启动收包失败: " + started.error().message();
    socket_->close();
    socket_.reset();
  }
}

Result<SocketAddress> EchoServer::local_address() const {
  if (protocol_ == Protocol::Udp) {
    if (!socket_) {
      return std::unexpected(uvnet::Error{ErrorCode::BadFileDescriptor, 0});
    }
    return socket_->local_address();
  }
  return listener_.local_address();
}

void EchoServer::close() {
  listener_.close();
  if (socket_) {
    socket_->stop_recv();
    socket_->close();
    socket_.reset();
  }
}

} // namespace app
