#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <vector>

#include "bench_support.hpp"

namespace {

using namespace uvnet;

// 默认后端；命令行传 readiness / iocp / epoll 可指定
Backend g_backend = Backend::Unknown;

const char* backend_name(Backend backend) {
  switch (backend) {
    case Backend::Iocp:
      return "iocp";
    case Backend::Readiness:
      return "readiness";
    case Backend::Epoll:
      return "epoll";
    default:
      return "default";
  }
}

std::vector<std::byte> make_payload(std::size_t size) {
  std::vector<std::byte> payload(size);
  for (std::size_t i = 0; i < size; ++i) {
    payload[i] = static_cast<std::byte>((i * 7 + 3) & 0xFF);
  }
  return payload;
}

SocketAddress bind_loopback(TcpListener& listener) {
  const auto bound = listener.bind(SocketAddress::loopback(Family::IPv4, 0));
  if (!bound) {
    return {};
  }
  const auto address = listener.local_address();
  return address ? *address : SocketAddress{};
}

void run_throughput(std::size_t chunk_size, std::size_t total_bytes) {
  Loop loop(g_backend);
  bench::Guard guard(loop, std::chrono::seconds(60));

  bool failed = false;
  bool completed = false;
  std::size_t sent = 0;
  std::size_t received = 0;
  std::size_t in_flight = 0;
  std::uint64_t elapsed_ns = 0;
  std::uint64_t start_ns = 0;
  const auto payload = make_payload(chunk_size);

  TcpListener listener(loop);
  const SocketAddress address = bind_loopback(listener);
  if (address.family() == Family::Unspecified) {
    std::printf("tcp throughput %zu B: bind 失败\n", chunk_size);
    return;
  }

  if (!listener.listen(16, [&](Result<TcpStream> accepted) {
        if (!accepted) {
          failed = true;
          guard.cancel();
          return;
        }
        auto server = std::make_shared<TcpStream>(std::move(*accepted));
        const bool started = bool(server->start_read(
            [server](Result<std::span<const std::byte>> chunk) {
              if (!chunk) {
                server->close();
                return;
              }
              (void)server->write(*chunk);
            }));
        if (!started) {
          failed = true;
          guard.cancel();
        }
      })) {
    std::printf("tcp throughput %zu B: listen 失败\n", chunk_size);
    return;
  }

  TcpStream client(loop);
  std::function<void()> pump;

  pump = [&] {
    while (!failed && in_flight < 16 && sent < total_bytes) {
      const std::size_t size = std::min(chunk_size, total_bytes - sent);
      sent += size;
      ++in_flight;
      const bool queued = bool(client.write(
          std::span<const std::byte>(payload.data(), size), [&](VoidResult result) {
            if (!result) {
              failed = true;
              guard.cancel();
              return;
            }
            --in_flight;
            if (sent < total_bytes) {
              pump();
            }
          }));
      if (!queued) {
        failed = true;
        guard.cancel();
        break;
      }
    }
  };

  const bool connected = bool(client.connect(address, [&](VoidResult result) {
    if (!result) {
      failed = true;
      guard.cancel();
      return;
    }

    const bool reading = bool(client.start_read([&](Result<std::span<const std::byte>> chunk) {
      if (!chunk) {
        if (chunk.error().code != ErrorCode::EndOfFile) {
          failed = true;
        }
        guard.cancel();
        return;
      }

      received += chunk->size();
      if (received >= total_bytes) {
        completed = true;
        elapsed_ns = bench::now_ns() - start_ns;
        client.stop_read();
        client.close();
        listener.close();
        guard.cancel();
      }
    }));
    if (!reading) {
      failed = true;
      guard.cancel();
      return;
    }

    start_ns = bench::now_ns();
    pump();
  }));

  if (!connected) {
    std::printf("tcp throughput %zu B: connect 失败\n", chunk_size);
    return;
  }

  loop.run();

  if (failed || !completed) {
    std::printf("tcp throughput %zu B: 失败或超时\n", chunk_size);
    return;
  }

  char name[64] = {};
  std::snprintf(name, sizeof(name), "tcp throughput %zu B chunk", chunk_size);
  bench::print_throughput(name, total_bytes, elapsed_ns, total_bytes / chunk_size);
}

void run_pingpong(std::size_t iterations, std::size_t message_size) {
  Loop loop(g_backend);
  bench::Guard guard(loop, std::chrono::seconds(60));

  bool failed = false;
  bool completed = false;
  std::size_t echoes = 0;
  const std::size_t warmup = 200;
  std::uint64_t send_ns = 0;
  std::vector<std::uint64_t> samples;
  samples.reserve(iterations);
  const auto payload = make_payload(message_size);

  TcpListener listener(loop);
  const SocketAddress address = bind_loopback(listener);
  if (address.family() == Family::Unspecified) {
    std::printf("tcp pingpong: bind 失败\n");
    return;
  }

  if (!listener.listen(16, [&](Result<TcpStream> accepted) {
        if (!accepted) {
          failed = true;
          guard.cancel();
          return;
        }
        auto server = std::make_shared<TcpStream>(std::move(*accepted));
        const bool started = bool(server->start_read(
            [server](Result<std::span<const std::byte>> chunk) {
              if (!chunk) {
                server->close();
                return;
              }
              (void)server->write(*chunk);
            }));
        if (!started) {
          failed = true;
          guard.cancel();
        }
      })) {
    std::printf("tcp pingpong: listen 失败\n");
    return;
  }

  TcpStream client(loop);
  std::function<void()> send_next;

  send_next = [&] {
    send_ns = bench::now_ns();
    const bool queued = bool(client.write(
        std::span<const std::byte>(payload.data(), payload.size()), [&](VoidResult result) {
          if (!result) {
            failed = true;
            guard.cancel();
          }
        }));
    if (!queued) {
      failed = true;
      guard.cancel();
    }
  };

  const bool connected = bool(client.connect(address, [&](VoidResult result) {
    if (!result) {
      failed = true;
      guard.cancel();
      return;
    }

    const bool reading = bool(client.start_read([&](Result<std::span<const std::byte>> chunk) {
      if (!chunk) {
        if (chunk.error().code != ErrorCode::EndOfFile) {
          failed = true;
        }
        guard.cancel();
        return;
      }

      ++echoes;
      if (echoes > warmup) {
        samples.push_back(bench::now_ns() - send_ns);
      }

      if (samples.size() >= iterations) {
        completed = true;
        client.stop_read();
        client.close();
        listener.close();
        guard.cancel();
        return;
      }

      send_next();
    }));
    if (!reading) {
      failed = true;
      guard.cancel();
      return;
    }

    send_next();
  }));

  if (!connected) {
    std::printf("tcp pingpong: connect 失败\n");
    return;
  }

  loop.run();

  if (failed || !completed) {
    std::printf("tcp pingpong %zu B: 失败或超时\n", message_size);
    return;
  }

  char name[64] = {};
  std::snprintf(name, sizeof(name), "tcp pingpong %zu B RTT", message_size);
  bench::print_latency(name, std::move(samples));
}

void run_send_throughput(std::size_t chunk_size, std::size_t total_bytes, bool zero_copy) {
  Loop loop(g_backend);
  bench::Guard guard(loop, std::chrono::seconds(60));

  bool failed = false;
  bool completed = false;
  bool pumping = false;
  std::size_t sent = 0;
  std::size_t in_flight = 0;
  std::uint64_t start_ns = 0;
  std::uint64_t elapsed_ns = 0;

  auto payload_owner =
      std::make_shared<std::vector<std::byte>>(make_payload(chunk_size));
  const std::vector<std::byte>& payload = *payload_owner;

  TcpListener listener(loop);
  const SocketAddress address = bind_loopback(listener);
  if (address.family() == Family::Unspecified) {
    std::printf("tcp send: bind 失败\n");
    return;
  }

  if (!listener.listen(16, [&](Result<TcpStream> accepted) {
        if (!accepted) {
          failed = true;
          guard.cancel();
          return;
        }
        auto server = std::make_shared<TcpStream>(std::move(*accepted));
        if (!server->start_read([server](Result<std::span<const std::byte>> chunk) {
              if (!chunk) {
                server->close();
              }
            })) {
          failed = true;
          guard.cancel();
        }
      })) {
    std::printf("tcp send: listen 失败\n");
    return;
  }

  TcpStream client(loop);

  const auto finish = [&] {
    if (completed) {
      return;
    }
    completed = true;
    elapsed_ns = bench::now_ns() - start_ns;
    client.close();
    listener.close();
    guard.cancel();
  };

  std::function<void()> pump;
  pump = [&] {
    if (pumping) {
      return;
    }
    pumping = true;

    while (!failed && !completed && in_flight < 16 && sent < total_bytes) {
      const std::size_t size = std::min(chunk_size, total_bytes - sent);
      sent += size;
      ++in_flight;

      auto callback = [&](VoidResult result) {
        if (!result) {
          failed = true;
          guard.cancel();
          return;
        }
        --in_flight;
        if (sent < total_bytes) {
          pump();
        } else if (in_flight == 0) {
          finish();
        }
      };

      const bool queued =
          zero_copy
              ? bool(client.write(WriteBuffer{std::span<const std::byte>(payload.data(), size),
                                              payload_owner},
                                  callback))
              : bool(client.write(std::span<const std::byte>(payload.data(), size), callback));

      if (!queued) {
        failed = true;
        guard.cancel();
        break;
      }
    }

    pumping = false;

    if (!failed && !completed && sent >= total_bytes && in_flight == 0) {
      finish();
    }
  };

  const bool connected = bool(client.connect(address, [&](VoidResult result) {
    if (!result) {
      failed = true;
      guard.cancel();
      return;
    }
    start_ns = bench::now_ns();
    pump();
  }));

  if (!connected) {
    std::printf("tcp send: connect 失败\n");
    return;
  }

  loop.run();

  if (failed || !completed) {
    std::printf("tcp send %zu B: 失败或超时\n", chunk_size);
    return;
  }

  char name[80] = {};
  std::snprintf(name,
                sizeof(name),
                "tcp send %s %zu B",
                zero_copy ? "zero-copy" : "copy",
                chunk_size);
  bench::print_throughput(name, total_bytes, elapsed_ns, total_bytes / chunk_size);
}

} // namespace

int main(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "readiness") == 0) {
      g_backend = Backend::Readiness;
    } else if (std::strcmp(argv[i], "iocp") == 0) {
      g_backend = Backend::Iocp;
    } else if (std::strcmp(argv[i], "epoll") == 0) {
      g_backend = Backend::Epoll;
    }
  }

  std::printf("== TCP (%s) ==\n", backend_name(g_backend));
  run_throughput(16 * 1024, 32u * 1024u * 1024u);
  run_throughput(64 * 1024, 32u * 1024u * 1024u);
  run_pingpong(20000, 64);
  run_send_throughput(16 * 1024, 32u * 1024u * 1024u, false);
  run_send_throughput(16 * 1024, 32u * 1024u * 1024u, true);
  run_send_throughput(64 * 1024, 32u * 1024u * 1024u, false);
  run_send_throughput(64 * 1024, 32u * 1024u * 1024u, true);
  run_send_throughput(256 * 1024, 64u * 1024u * 1024u, false);
  run_send_throughput(256 * 1024, 64u * 1024u * 1024u, true);
  return 0;
}
