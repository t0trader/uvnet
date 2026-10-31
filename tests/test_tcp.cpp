#include <memory>
#include <span>
#include <thread>
#include <vector>

#include "test_support.hpp"

namespace {

using namespace uvnet;

constexpr std::size_t kChunkSize = 32 * 1024;
constexpr std::size_t kChunkCount = 8;
constexpr std::size_t kTotalSize = kChunkSize * kChunkCount;

std::vector<std::byte> make_payload() {
  std::vector<std::byte> payload(kTotalSize);
  for (std::size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<std::byte>((i * 7 + 11) & 0xFF);
  }
  return payload;
}

void test_echo(Backend backend, Family family) {
  Loop loop(backend);
  uvtest::Guard guard(loop, std::chrono::seconds(10));
  CHECK(loop.backend() == backend);

  TcpListener listener(loop);
  const auto bound = listener.bind(SocketAddress::loopback(family, 0));
  if (!bound) {
    if (family == Family::IPv6) {
      std::printf("[skip] IPv6 不可用: %s\n", bound.error().message().c_str());
      return;
    }
    CHECK(false);
    return;
  }

  const auto address = listener.local_address();
  if (!address) {
    CHECK(false);
    return;
  }

  auto payload_owner = std::make_shared<std::vector<std::byte>>(make_payload());
  const std::vector<std::byte>& payload = *payload_owner;
  std::size_t server_received = 0;
  std::size_t client_received = 0;
  bool server_closed = false;
  bool client_closed = false;

  auto maybe_finish = [&] {
    if (server_closed && client_closed) {
      listener.close();
      guard.cancel();
    }
  };

  CHECK_OK(listener.listen(16, [&](Result<TcpStream> accepted) {
    if (!accepted) {
      CHECK(false);
      guard.cancel();
      return;
    }

    auto server = std::make_shared<TcpStream>(std::move(*accepted));
    CHECK_OK(server->set_read_options(ReadOptions{.slots = 2, .chunk_size = 32 * 1024}));
    CHECK_OK(server->start_read([server, &server_received, &server_closed, &maybe_finish](
                                    Result<ReadChunk> chunk) {
      if (!chunk) {
        if (chunk.error().code == ErrorCode::EndOfFile) {
          server->close();
          server_closed = true;
          maybe_finish();
        } else {
          CHECK(false);
          server->close();
          server_closed = true;
          maybe_finish();
        }
        return;
      }
      server_received += chunk->data.size();
      if (!server->write(WriteBuffer{chunk->data, chunk->keepalive})) {
        CHECK(false);
      }
    }));
  }));

  TcpStream client(loop);
  const auto connected = client.connect(*address, [&](VoidResult result) {
    if (!result) {
      CHECK(false);
      guard.cancel();
      return;
    }

    CHECK_OK(client.set_read_options(ReadOptions{.slots = 2, .chunk_size = 32 * 1024}));
    CHECK_OK(client.start_read([&](Result<ReadChunk> chunk) {
      if (!chunk) {
        if (chunk.error().code == ErrorCode::EndOfFile) {
          CHECK(client_received == kTotalSize);
          client.close();
          client_closed = true;
          maybe_finish();
        } else {
          CHECK(false);
          client.close();
          client_closed = true;
          maybe_finish();
        }
        return;
      }

      CHECK(client_received + chunk->data.size() <= kTotalSize);
      const std::size_t base = client_received;
      for (std::size_t i = 0; i < chunk->data.size(); ++i) {
        if (chunk->data[i] != payload[base + i]) {
          CHECK(false);
          break;
        }
      }
      client_received += chunk->data.size();

      if (client_received == kTotalSize) {
        client.stop_read();
        CHECK_OK(client.shutdown([&](VoidResult shutdown_result) {
          CHECK(shutdown_result.has_value());
          client.close();
          client_closed = true;
          maybe_finish();
        }));
      }
    }));

    for (std::size_t i = 0; i < kChunkCount; ++i) {
      const auto offset = i * kChunkSize;
      std::span<const std::byte> chunk(payload.data() + offset, kChunkSize);
      if (!client.write(WriteBuffer{chunk, payload_owner})) {
        CHECK(false);
        guard.cancel();
        return;
      }
    }
  });

  if (!connected) {
    CHECK(false);
    guard.cancel();
  }

  loop.run();

  CHECK(server_received == kTotalSize);
  CHECK(client_received == kTotalSize);
}

void test_connection_refused(Backend backend) {
  Loop loop(backend);
  uvtest::Guard guard(loop, std::chrono::seconds(5));

  TcpListener listener(loop);
  CHECK_OK(listener.bind(SocketAddress::loopback(Family::IPv4, 0)));
  const auto address = listener.local_address();
  if (!address) {
    CHECK(false);
    return;
  }
  listener.close();

  TcpStream client(loop);
  bool got_result = false;

  auto handle_result = [&](VoidResult result) {
    got_result = true;
    CHECK(!result);
    if (!result) {
      CHECK(result.error().code == ErrorCode::ConnectionRefused);
    }
    client.close();
    guard.cancel();
  };

  const auto started = client.connect(*address, handle_result);
  if (!started) {
    handle_result(started);
  }

  loop.run();
  CHECK(got_result);
}

// 视图读（无 keepalive）+ 拷贝写路径，覆盖 SpanReadCallback
void test_span_echo(Backend backend) {
  Loop loop(backend);
  uvtest::Guard guard(loop, std::chrono::seconds(10));
  CHECK(loop.backend() == backend);

  TcpListener listener(loop);
  CHECK_OK(listener.bind(SocketAddress::loopback(Family::IPv4, 0)));
  const auto address = listener.local_address();
  if (!address) {
    CHECK(false);
    return;
  }

  const auto payload = make_payload();
  std::size_t server_received = 0;
  std::size_t client_received = 0;

  CHECK_OK(listener.listen(16, [&](Result<TcpStream> accepted) {
    if (!accepted) {
      CHECK(false);
      guard.cancel();
      return;
    }
    auto server = std::make_shared<TcpStream>(std::move(*accepted));
    CHECK_OK(server->start_read(
        [server, &server_received](Result<std::span<const std::byte>> chunk) {
          if (!chunk) {
            server->close();
            return;
          }
          server_received += chunk->size();
          if (!server->write(*chunk)) {
            CHECK(false);
          }
        }));
  }));

  TcpStream client(loop);
  const auto connected = client.connect(*address, [&](VoidResult result) {
    if (!result) {
      CHECK(false);
      guard.cancel();
      return;
    }

    CHECK_OK(client.start_read([&](Result<std::span<const std::byte>> chunk) {
      if (!chunk) {
        CHECK(false);
        client.close();
        guard.cancel();
        return;
      }
      client_received += chunk->size();
      if (client_received >= kTotalSize) {
        client.stop_read();
        CHECK_OK(client.shutdown([&](VoidResult shutdown_result) {
          CHECK(shutdown_result.has_value());
          client.close();
          listener.close();
          guard.cancel();
        }));
      }
    }));

    for (std::size_t i = 0; i < kChunkCount; ++i) {
      std::span<const std::byte> chunk(payload.data() + i * kChunkSize, kChunkSize);
      if (!client.write(chunk)) {
        CHECK(false);
        guard.cancel();
        return;
      }
    }
  });

  if (!connected) {
    CHECK(false);
    guard.cancel();
  }

  loop.run();

  CHECK(server_received == kTotalSize);
  CHECK(client_received == kTotalSize);
}

#if defined(_WIN32)
// 就绪式 Loop 没有 IOCP，跨线程 post 依赖唤醒对；验证能唤醒阻塞中的 epoll_wait
void test_readiness_cross_thread_post() {
  Loop loop(Backend::Readiness);
  // 没有活动句柄时 run() 会立即返回，用保护定时器把循环挂住
  uvtest::Guard guard(loop, std::chrono::seconds(5));
  bool ran = false;

  std::thread worker([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    loop.post([&] {
      ran = true;
      loop.stop();
    });
  });

  loop.run();
  worker.join();
  CHECK(ran);
}
#endif

} // namespace

int main() {
#if defined(_WIN32)
  const Backend backends[] = {Backend::Iocp, Backend::Readiness, Backend::Select};
#else
  const Backend backends[] = {Backend::Epoll};
#endif
  for (const Backend backend : backends) {
    test_echo(backend, Family::IPv4);
    test_echo(backend, Family::IPv6);
    test_connection_refused(backend);
    test_span_echo(backend);
  }
#if defined(_WIN32)
  test_readiness_cross_thread_post();
#endif
  return uvtest::finish("tcp");
}
