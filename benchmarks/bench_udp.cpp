#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
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
    payload[i] = static_cast<std::byte>((i + 5) & 0xFF);
  }
  return payload;
}

void run_throughput(std::size_t datagram_size, std::size_t count) {
  Loop loop(g_backend);
  bench::Guard guard(loop, std::chrono::seconds(60));

  bool failed = false;
  bool completed = false;
  bool tail_armed = false;
  bool pumping = false;
  std::size_t sent = 0;
  std::size_t in_flight = 0;
  std::size_t received = 0;
  std::uint64_t start_ns = 0;
  std::uint64_t elapsed_ns = 0;
  std::uint64_t send_done_ns = 0;
  const auto payload = make_payload(datagram_size);

  Timer tail_timer(loop);

  UdpSocket server(loop, Family::IPv4);
  const auto server_bound = server.bind(SocketAddress::loopback(Family::IPv4, 0));
  if (!server_bound) {
    std::printf("udp throughput %zu B: server bind 失败\n", datagram_size);
    return;
  }
  const auto address = server.local_address();
  if (!address) {
    std::printf("udp throughput %zu B: 地址获取失败\n", datagram_size);
    return;
  }

  UdpSocket client(loop, Family::IPv4);
  if (!client.bind(SocketAddress::loopback(Family::IPv4, 0)) ||
      !client.connect(*address)) {
    std::printf("udp throughput %zu B: client 初始化失败\n", datagram_size);
    return;
  }

  (void)server.start_recv([&](Result<DatagramView> datagram) {
    if (!datagram) {
      failed = true;
      guard.cancel();
      return;
    }
    if (!server.send_to(datagram->source, datagram->data)) {
      failed = true;
      guard.cancel();
    }
  });

  std::function<void()> pump;

  (void)client.start_recv([&](Result<DatagramView> datagram) {
    if (!datagram) {
      if (!completed) {
        failed = true;
        guard.cancel();
      }
      return;
    }
    if (datagram->data.size() != datagram_size) {
      failed = true;
      guard.cancel();
      return;
    }

    ++received;
    if (received >= count) {
      completed = true;
      elapsed_ns = bench::now_ns() - start_ns;
      client.stop_recv();
      server.stop_recv();
      client.close();
      server.close();
      guard.cancel();
      return;
    }
    pump();
  });

  const auto finalize = [&] {
    if (completed) {
      return;
    }
    completed = true;
    elapsed_ns = (send_done_ns != 0 ? send_done_ns : bench::now_ns()) - start_ns;
    client.stop_recv();
    server.stop_recv();
    client.close();
    server.close();
    guard.cancel();
  };

  pump = [&] {
    if (pumping) {
      return;
    }
    pumping = true;
    // 发送完成即时回（就绪式）时 in_flight 始终为 0，用"已发未回"再兜一层流控
    while (!failed && !completed && in_flight < 64 && sent < count &&
           sent - received < 64) {
      ++sent;
      ++in_flight;
      const bool queued = bool(client.send(
          payload, [&](VoidResult result) {
            if (!result) {
              if (!completed) {
                failed = true;
                guard.cancel();
              }
              return;
            }
            --in_flight;
            if (!completed && sent < count) {
              pump();
            } else if (!completed && !tail_armed && sent >= count && in_flight == 0) {
              // 全部发出后给内核/回声端 300ms 收尾，容忍 UDP 的少量丢包
              tail_armed = true;
              send_done_ns = bench::now_ns();
              (void)tail_timer.start(std::chrono::milliseconds(300), finalize);
            }
          }));
      if (!queued) {
        failed = true;
        guard.cancel();
        break;
      }
    }
    pumping = false;
  };

  start_ns = bench::now_ns();
  pump();
  loop.run();

  if (failed || !completed) {
    std::printf("udp throughput %zu B: 失败或超时 (received=%zu/%zu)\n",
                datagram_size,
                received,
                count);
    return;
  }

  char name[64] = {};
  if (received == count) {
    std::snprintf(name, sizeof(name), "udp throughput %zu B datagram", datagram_size);
  } else {
    std::snprintf(name, sizeof(name), "udp throughput %zu B (drop %zu)", datagram_size,
                  count - received);
  }
  bench::print_throughput(name,
                          static_cast<std::uint64_t>(datagram_size) * received,
                          elapsed_ns,
                          received);
}

void run_pingpong(std::size_t iterations, std::size_t datagram_size) {
  Loop loop(g_backend);
  bench::Guard guard(loop, std::chrono::seconds(60));

  bool failed = false;
  bool completed = false;
  std::size_t echoes = 0;
  const std::size_t warmup = 100;
  std::uint64_t send_ns = 0;
  std::vector<std::uint64_t> samples;
  samples.reserve(iterations);
  const auto payload = make_payload(datagram_size);

  UdpSocket server(loop, Family::IPv4);
  if (!server.bind(SocketAddress::loopback(Family::IPv4, 0))) {
    std::printf("udp pingpong: server bind 失败\n");
    return;
  }
  const auto address = server.local_address();
  if (!address) {
    std::printf("udp pingpong: 地址获取失败\n");
    return;
  }

  UdpSocket client(loop, Family::IPv4);
  if (!client.bind(SocketAddress::loopback(Family::IPv4, 0)) ||
      !client.connect(*address)) {
    std::printf("udp pingpong: client 初始化失败\n");
    return;
  }

  (void)server.start_recv([&](Result<DatagramView> datagram) {
    if (!datagram) {
      failed = true;
      guard.cancel();
      return;
    }
    if (!server.send_to(datagram->source, datagram->data)) {
      failed = true;
      guard.cancel();
    }
  });

  std::function<void()> send_next;

  (void)client.start_recv([&](Result<DatagramView> datagram) {
    if (!datagram) {
      if (!completed) {
        failed = true;
        guard.cancel();
      }
      return;
    }

    ++echoes;
    if (echoes > warmup) {
      samples.push_back(bench::now_ns() - send_ns);
    }

    if (samples.size() >= iterations) {
      completed = true;
      client.stop_recv();
      server.stop_recv();
      client.close();
      server.close();
      guard.cancel();
      return;
    }

    send_next();
  });

  send_next = [&] {
    send_ns = bench::now_ns();
    if (!client.send(payload, [&](VoidResult result) {
          if (!result && !completed) {
            failed = true;
            guard.cancel();
          }
        })) {
      failed = true;
      guard.cancel();
    }
  };

  send_next();
  loop.run();

  if (failed || !completed) {
    std::printf("udp pingpong %zu B: 失败或超时\n", datagram_size);
    return;
  }

  char name[64] = {};
  std::snprintf(name, sizeof(name), "udp pingpong %zu B RTT", datagram_size);
  bench::print_latency(name, std::move(samples));
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

  std::printf("== UDP (%s) ==\n", backend_name(g_backend));
  run_throughput(64, 10000);
  run_throughput(1024, 10000);
  run_pingpong(5000, 64);
  return 0;
}
