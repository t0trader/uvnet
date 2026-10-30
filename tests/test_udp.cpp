#include <functional>
#include <vector>

#include "test_support.hpp"

namespace {

using namespace uvnet;

constexpr int kDatagramCount = 32;

void test_echo(Family family) {
  Loop loop;
  uvtest::Guard guard(loop, std::chrono::seconds(10));

  UdpSocket server(loop, family);
  const auto server_bound = server.bind(SocketAddress::loopback(family, 0));
  if (!server_bound) {
    if (family == Family::IPv6) {
      std::printf("[skip] IPv6 UDP 不可用: %s\n", server_bound.error().message().c_str());
      return;
    }
    CHECK(false);
    return;
  }

  const auto server_address = server.local_address();
  if (!server_address) {
    CHECK(false);
    return;
  }

  UdpSocket client(loop, family);
  CHECK_OK(client.bind(SocketAddress::loopback(family, 0)));

  int received = 0;

  CHECK_OK(server.set_recv_options(UdpRecvOptions{.slots = 2, .chunk_size = 32 * 1024}));
  CHECK_OK(server.start_recv([&](Result<Datagram> datagram) {
    if (!datagram) {
      CHECK(false);
      return;
    }
    if (!server.send_to(datagram->source,
                        WriteBuffer{datagram->data, datagram->keepalive})) {
      CHECK(false);
    }
  }));

  std::function<void(int)> send_one;
  send_one = [&](int index) {
    const std::size_t payload_size = 16 + static_cast<std::size_t>(index) * 4;
    auto payload = std::make_shared<std::vector<std::byte>>(payload_size + 2);
    (*payload)[0] = static_cast<std::byte>(index);
    (*payload)[1] = static_cast<std::byte>(payload_size);
    for (std::size_t j = 0; j < payload_size; ++j) {
      (*payload)[j + 2] = static_cast<std::byte>((index + static_cast<int>(j)) & 0xFF);
    }

    if (!client.send_to(*server_address, WriteBuffer{*payload, payload},
                        [](VoidResult result) { CHECK(result.has_value()); })) {
      CHECK(false);
      guard.cancel();
    }
  };

  // 逐个发送、逐个等待回显，避免 loopback 下的突发丢包影响断言
  CHECK_OK(client.set_recv_options(UdpRecvOptions{.slots = 2, .chunk_size = 32 * 1024}));
  CHECK_OK(client.start_recv([&](Result<Datagram> datagram) {
    if (!datagram) {
      CHECK(false);
      guard.cancel();
      return;
    }
    CHECK(datagram->data.size() >= 2);
    if (datagram->data.size() < 2) {
      return;
    }
    const auto tag = static_cast<int>(datagram->data[0]);
    const auto length = static_cast<std::size_t>(datagram->data[1]);
    CHECK(tag == received);
    CHECK(length == datagram->data.size() - 2);
    ++received;

    if (received == kDatagramCount) {
      client.stop_recv();
      server.stop_recv();
      server.close();
      client.close();
      guard.cancel();
      return;
    }

    send_one(received);
  }));

  send_one(0);
  loop.run();

  CHECK(received == kDatagramCount);
}

void test_connected_send(Family family) {
  Loop loop;
  uvtest::Guard guard(loop, std::chrono::seconds(5));

  UdpSocket server(loop, family);
  const auto bound = server.bind(SocketAddress::loopback(family, 0));
  if (!bound) {
    if (family == Family::IPv6) {
      return;
    }
    CHECK(false);
    return;
  }
  const auto address = server.local_address();
  if (!address) {
    CHECK(false);
    return;
  }

  UdpSocket client(loop, family);
  CHECK_OK(client.connect(*address));

  bool server_got = false;
  bool client_got = false;

  CHECK_OK(server.start_recv([&](Result<Datagram> datagram) {
    CHECK(datagram.has_value());
    if (datagram) {
      server_got = true;
      CHECK_OK(server.send_to(datagram->source, datagram->data));
    }
  }));

  CHECK_OK(client.start_recv([&](Result<Datagram> datagram) {
    CHECK(datagram.has_value());
    if (datagram) {
      client_got = true;
      client.stop_recv();
      server.stop_recv();
      server.close();
      client.close();
      guard.cancel();
    }
  }));

  const std::vector<std::byte> payload{std::byte{0x55}, std::byte{0xAA}};
  CHECK_OK(client.send(payload, [](VoidResult result) { CHECK(result.has_value()); }));

  loop.run();

  CHECK(server_got);
  CHECK(client_got);
}

} // namespace

int main() {
  test_echo(Family::IPv4);
  test_echo(Family::IPv6);
  test_connected_send(Family::IPv4);
  return uvtest::finish("udp");
}
