#define _GNU_SOURCE  // 暴露 recvmmsg/sendmmsg
#include "uvnet/udp.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "detail/buffer_pool.hpp"
#include "detail/platform.hpp"
#include "detail/poller_linux.hpp"

namespace uvnet {
namespace {

constexpr std::size_t kMaxDatagramSize = 65536;

struct SendItem {
  std::vector<std::byte> owned;
  const std::byte* data = nullptr;
  std::size_t size = 0;
  SocketAddress destination;
  WriteCallback callback;
  bool has_destination = false;
  std::shared_ptr<const void> keepalive;
};

VoidResult apply_udp_options(int fd, Family family, const UdpOptions& options) {
  const int one = 1;

  if (options.reuse_address &&
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }

  if (options.reuse_port &&
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0) {
    return std::unexpected(detail::from_socket_error(errno));
  }

  if (family == Family::IPv6) {
    const int v6only = options.ipv6_only ? 1 : 0;
    if (::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) != 0) {
      return std::unexpected(detail::from_socket_error(errno));
    }
  }

  // 默认 UDP 收发缓冲 256KB：真实网卡 + 高 PPS 场景显著降低丢包
  // loopback 场景几乎无影响；失败仅 warn，不阻塞 socket 创建
  constexpr int kDefaultUdpBuf = 256 * 1024;
  (void)::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &kDefaultUdpBuf, sizeof(kDefaultUdpBuf));
  (void)::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &kDefaultUdpBuf, sizeof(kDefaultUdpBuf));

  return {};
}

int create_udp_socket(Family family) {
  return ::socket(detail::AddressCodec::native_family(family),
                  SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                  IPPROTO_UDP);
}

} // namespace

namespace detail {

class UdpSocketImpl final : public IoWatcher,
                            public std::enable_shared_from_this<UdpSocketImpl> {
 public:
  UdpSocketImpl(std::shared_ptr<LoopImpl> loop, Family family, UdpOptions options)
      : loop_(std::move(loop)), family_(family), options_(options) {}

  ~UdpSocketImpl() override { destroy(); }

  VoidResult bind(const SocketAddress& local) {
    if (local.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(local, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    if (::bind(fd_, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult connect(const SocketAddress& remote) {
    if (remote.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    sockaddr_storage storage{};
    int length = 0;
    if (!AddressCodec::to_native(remote, storage, length)) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&storage), static_cast<socklen_t>(length)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }

    connected_ = true;
    connected_peer_ = remote;
    return {};
  }

  VoidResult disconnect() {
    if (fd_ < 0 || !connected_) {
      return {};
    }

    sockaddr_storage storage{};
    std::memset(&storage, 0, sizeof(storage));
    storage.ss_family = AF_UNSPEC;
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&storage), sizeof(sockaddr)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    connected_ = false;
    return {};
  }

  VoidResult send(std::span<const std::byte> data, WriteCallback callback) {
    if (!connected_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.owned.assign(data.begin(), data.end());
    item.data = item.owned.data();
    item.size = item.owned.size();
    item.callback = std::move(callback);
    item.has_destination = false;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult send(WriteBuffer buffer, WriteCallback callback) {
    if (!connected_) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.data = buffer.data.data();
    item.size = buffer.data.size();
    item.keepalive = std::move(buffer.keepalive);
    item.callback = std::move(callback);
    item.has_destination = false;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult send_to(const SocketAddress& remote,
                     std::span<const std::byte> data,
                     WriteCallback callback) {
    if (remote.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.owned.assign(data.begin(), data.end());
    item.data = item.owned.data();
    item.size = item.owned.size();
    item.destination = remote;
    item.callback = std::move(callback);
    item.has_destination = true;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult send_to(const SocketAddress& remote,
                     WriteBuffer buffer,
                     WriteCallback callback) {
    if (remote.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    SendItem item;
    item.data = buffer.data.data();
    item.size = buffer.data.size();
    item.destination = remote;
    item.keepalive = std::move(buffer.keepalive);
    item.callback = std::move(callback);
    item.has_destination = true;
    sends_.push_back(std::move(item));

    flush_sends();
    return {};
  }

  VoidResult set_recv_options(const UdpRecvOptions& options) {
    if (recv_started_) {
      return std::unexpected(make_error(ErrorCode::InProgress));
    }
    if (options.slots == 0 || options.slots > 64 || options.chunk_size < 1024 ||
        options.chunk_size > 1024 * 1024) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    recv_chunk_size_ = options.chunk_size;
    return {};
  }

  VoidResult start_recv(UdpRecvCallback callback) {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    keepalive_reads_ = true;
    view_callback_ = nullptr;
    recv_callback_ = std::move(callback);
    receiving_ = true;
    recv_started_ = true;
    update_interest();
    return {};
  }

  VoidResult start_recv(UdpRecvViewCallback callback) {
    if (!callback) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    keepalive_reads_ = false;
    recv_callback_ = nullptr;
    view_callback_ = std::move(callback);
    receiving_ = true;
    recv_started_ = true;
    update_interest();
    return {};
  }

  void stop_recv() {
    receiving_ = false;
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    update_interest();
  }

  VoidResult set_broadcast(bool enable) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    const int value = enable ? 1 : 0;
    if (::setsockopt(fd_, SOL_SOCKET, SO_BROADCAST, &value, sizeof(value)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult set_ttl(int ttl) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_UNICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    } else {
      if (::setsockopt(fd_, IPPROTO_IP, IP_TTL, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    }
    return {};
  }

  VoidResult set_multicast_ttl(int ttl) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &ttl, sizeof(ttl)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    } else {
      unsigned char value = static_cast<unsigned char>(ttl);
      if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    }
    return {};
  }

  VoidResult set_multicast_loop(bool enable) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (family_ == Family::IPv6) {
      const int value = enable ? 1 : 0;
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    } else {
      const unsigned char value = enable ? 1 : 0;
      if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &value, sizeof(value)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
    }
    return {};
  }

  VoidResult set_multicast_interface(const SocketAddress& iface) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    if (family_ == Family::IPv6) {
      const unsigned int index = iface.scope_id();
      if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_MULTICAST_IF, &index, sizeof(index)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
      return {};
    }

    if (iface.family() != Family::IPv4) {
      return std::unexpected(make_error(ErrorCode::InvalidArgument));
    }

    in_addr address{};
    std::memcpy(&address, iface.bytes_.data(), 4);
    if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  VoidResult set_membership(const SocketAddress& group,
                            const SocketAddress& iface,
                            bool join) {
    if (group.family() != family_) {
      return std::unexpected(make_error(ErrorCode::AddressFamilyNotSupported));
    }
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }

    if (family_ == Family::IPv6) {
      ipv6_mreq request{};
      std::memcpy(&request.ipv6mr_multiaddr, group.bytes_.data(), 16);
      request.ipv6mr_interface = iface.scope_id();
      const int option = join ? IPV6_JOIN_GROUP : IPV6_LEAVE_GROUP;
      if (::setsockopt(fd_, IPPROTO_IPV6, option, &request, sizeof(request)) != 0) {
        return std::unexpected(from_socket_error(errno));
      }
      return {};
    }

    ip_mreq request{};
    std::memcpy(&request.imr_multiaddr, group.bytes_.data(), 4);
    if (iface.family() == Family::IPv4) {
      std::memcpy(&request.imr_interface, iface.bytes_.data(), 4);
    } else {
      request.imr_interface.s_addr = htonl(INADDR_ANY);
    }
    const int option = join ? IP_ADD_MEMBERSHIP : IP_DROP_MEMBERSHIP;
    if (::setsockopt(fd_, IPPROTO_IP, option, &request, sizeof(request)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  Result<SocketAddress> local_address() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return AddressCodec::from_native(reinterpret_cast<sockaddr*>(&storage),
                                     static_cast<int>(length));
  }

  Result<int> recv_buffer_size() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    socklen_t length = sizeof(value);
    if (::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return value;
  }

  VoidResult set_recv_buffer_size(int bytes) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  Result<int> send_buffer_size() const {
    if (fd_ < 0) {
      return std::unexpected(make_error(ErrorCode::BadFileDescriptor));
    }
    int value = 0;
    socklen_t length = sizeof(value);
    if (::getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &value, &length) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return value;
  }

  VoidResult set_send_buffer_size(int bytes) {
    auto ensured = ensure_socket();
    if (!ensured) {
      return ensured;
    }
    if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
      return std::unexpected(from_socket_error(errno));
    }
    return {};
  }

  [[nodiscard]] bool is_open() const noexcept { return fd_ >= 0; }
  [[nodiscard]] bool is_connected() const noexcept { return connected_; }

  void close() { destroy_with_callbacks(); }

  void on_io(std::uint32_t events) override {
    auto self = shared_from_this();
    if (fd_ < 0) {
      return;
    }
    if ((events & (EPOLLIN | EPOLLERR | EPOLLHUP)) != 0) {
      do_recv();
    }
    if ((events & (EPOLLOUT | EPOLLERR)) != 0) {
      flush_sends();
    }
  }

 private:
  VoidResult ensure_socket() {
    if (fd_ >= 0) {
      return {};
    }

    const int fd = create_udp_socket(family_);
    if (fd < 0) {
      return std::unexpected(from_socket_error(errno));
    }

    const auto applied = apply_udp_options(fd, family_, options_);
    if (!applied) {
      (void)::close(fd);
      return applied;
    }

    fd_ = fd;
    loop_->begin_work();
    work_ref_ = true;
    return {};
  }

  void update_interest() {
    if (fd_ < 0) {
      return;
    }

    std::uint32_t wanted = 0;
    if (receiving_) {
      wanted |= EPOLLIN;
    }
    if (!sends_.empty()) {
      wanted |= EPOLLOUT;
    }

    EpollPoller& poller = linux_poller(*loop_);
    if (wanted == 0) {
      if (watching_) {
        poller.del(this);
        watching_ = false;
        interest_ = 0;
      }
      return;
    }

    if (!watching_) {
      watching_ = poller.add(this, fd_, wanted);
      interest_ = wanted;
    } else if (wanted != interest_) {
      (void)poller.mod(this, wanted);
      interest_ = wanted;
    }
  }

  void deliver_recv_error(Error error) {
    if (keepalive_reads_) {
      auto callback = std::move(recv_callback_);
      recv_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    } else {
      auto callback = std::move(view_callback_);
      view_callback_ = nullptr;
      if (callback) {
        callback(std::unexpected(error));
      }
    }
  }

  // 一次 recvmmsg 最多收多少个 datagram — syscall 数降 1/kRecvBatch
  static constexpr unsigned int kRecvBatch = 32;

  void do_recv() {
    if (!receiving_ || fd_ < 0) {
      return;
    }

    if (keepalive_reads_ && !recv_pool_) {
      recv_pool_ = BufferPool::create(recv_chunk_size_, 32);
    }
    // view 模式也用 BufferPool，让 recvmmsg 批量可以同时处理多个独立缓冲
    if (!keepalive_reads_ && !recv_pool_) {
      recv_pool_ = BufferPool::create(recv_chunk_size_, 32);
    }

    auto self = shared_from_this();
    while (receiving_ && fd_ >= 0) {
      // 预填 batch：每个 entry 独立 BufferPool 缓冲 + 独立 source sockaddr
      std::array<BufferPool::Handle, kRecvBatch> handles{};
      std::array<sockaddr_storage, kRecvBatch> sources{};
      std::array<iovec, kRecvBatch> iovecs{};
      std::array<mmsghdr, kRecvBatch> mmsghdrs{};

      for (unsigned int i = 0; i < kRecvBatch; ++i) {
        handles[i] = recv_pool_->acquire();
        iovecs[i].iov_base = handles[i].data.data();
        iovecs[i].iov_len = handles[i].data.size();

        mmsghdrs[i].msg_hdr.msg_name = &sources[i];
        mmsghdrs[i].msg_hdr.msg_namelen = sizeof(sockaddr_storage);
        mmsghdrs[i].msg_hdr.msg_iov = &iovecs[i];
        mmsghdrs[i].msg_hdr.msg_iovlen = 1;
        mmsghdrs[i].msg_hdr.msg_control = nullptr;
        mmsghdrs[i].msg_hdr.msg_controllen = 0;
        mmsghdrs[i].msg_hdr.msg_flags = 0;
        mmsghdrs[i].msg_len = 0;
      }

      const int received = ::recvmmsg(fd_, mmsghdrs.data(), kRecvBatch, 0, nullptr);

      if (received > 0) {
        for (int i = 0; i < received; ++i) {
          const auto addr = reinterpret_cast<const sockaddr*>(mmsghdrs[i].msg_hdr.msg_name);
          const socklen_t addr_len = mmsghdrs[i].msg_hdr.msg_namelen;
          const SocketAddress source = AddressCodec::from_native(addr, static_cast<int>(addr_len));
          const std::span<const std::byte> bytes(
              reinterpret_cast<const std::byte*>(handles[i].data.data()),
              static_cast<std::size_t>(mmsghdrs[i].msg_len));

          if (keepalive_reads_) {
            auto callback = recv_callback_;
            if (callback) {
              callback(Datagram{bytes, source, std::move(handles[i].keepalive)});
            }
          } else {
            auto callback = view_callback_;
            if (callback) {
              callback(DatagramView{bytes, source});
            }
          }
          // view 模式下 keepalive 没被用户持有，自动回收
        }
        // 没用到的 buffer 回收
        for (unsigned int i = static_cast<unsigned int>(received); i < kRecvBatch; ++i) {
          handles[i].keepalive.reset();
        }
        continue;
      }

      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      if (error == EAGAIN || error == EWOULDBLOCK) {
        break;  // 空 batch 直接退出，epoll 下次触发再来
      }

      receiving_ = false;
      update_interest();
      deliver_recv_error(from_socket_error(error));
      return;
    }
  }

  void flush_sends() {
    auto self = shared_from_this();
    static constexpr unsigned int kSendBatch = 32;

    while (!sends_.empty() && fd_ >= 0) {
      const unsigned int batch_n = std::min<unsigned int>(sends_.size(), kSendBatch);
      std::vector<sockaddr_storage> targets(batch_n);  // 每个 item 独立目标地址
      std::vector<iovec> iovecs(batch_n);
      std::vector<mmsghdr> mmsghdrs(batch_n);
      std::vector<std::size_t> indices(batch_n);  // 记录 sends_ 里第几个

      unsigned int idx = 0;
      for (auto it = sends_.begin(); idx < batch_n && it != sends_.end(); ++it, ++idx) {
        if (!it->has_destination) {
          break;
        }
        int addr_len = 0;
        if (!AddressCodec::to_native(it->destination, targets[idx], addr_len)) {
          auto callback = std::move(it->callback);
          auto keeps = std::move(it->keepalive);
          auto owned = std::move(it->owned);
          it = sends_.erase(it);
          if (callback) {
            loop_->post([callback = std::move(callback), keeps = std::move(keeps),
                         owned = std::move(owned)]() mutable {
              callback(std::unexpected(make_error(ErrorCode::InvalidArgument)));
            });
          }
          --idx;
          continue;
        }

        iovecs[idx].iov_base = const_cast<char*>(reinterpret_cast<const char*>(it->data));
        iovecs[idx].iov_len = it->size;

        mmsghdrs[idx].msg_hdr.msg_name = &targets[idx];
        mmsghdrs[idx].msg_hdr.msg_namelen = addr_len;
        mmsghdrs[idx].msg_hdr.msg_iov = &iovecs[idx];
        mmsghdrs[idx].msg_hdr.msg_iovlen = 1;
        mmsghdrs[idx].msg_hdr.msg_control = nullptr;
        mmsghdrs[idx].msg_hdr.msg_controllen = 0;
        mmsghdrs[idx].msg_hdr.msg_flags = 0;
        mmsghdrs[idx].msg_len = 0;

        indices[idx] = idx;  // 对应 sends_ 的第 idx 个
      }

      if (idx == 0) {
        // 全是 connected UDP，走原有 send 单条逻辑（保留兼容）
        break;
      }

      const int sent = ::sendmmsg(fd_, mmsghdrs.data(), idx, 0);

      if (sent > 0) {
        // sendmmsg 成功 sent 个 — 处理这 sent 个回调 + pop
        for (int i = 0; i < sent; ++i) {
          auto& item = sends_.front();
          WriteCallback callback = std::move(item.callback);
          auto keepalive = std::move(item.keepalive);
          auto owned = std::move(item.owned);
          sends_.pop_front();
          if (callback) {
            loop_->post([callback = std::move(callback),
                         keepalive = std::move(keepalive),
                         owned = std::move(owned)]() mutable { callback({}); });
          }
        }
        // 如果 batch 里还有未 send 的（EAGAIN 之前可能发了一部分），退出等 EPOLLOUT
        if (static_cast<unsigned int>(sent) < idx) {
          break;
        }
        continue;
      }

      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      if (error == EAGAIN || error == EWOULDBLOCK) {
        break;
      }

      fail_sends(from_socket_error(error));
      return;
    }

    // 处理 connected UDP 的非 batch 残留（只有 connected_ 时才会进这里）
    while (!sends_.empty() && fd_ >= 0) {
      SendItem& item = sends_.front();
      if (item.has_destination) {
        // 带目标地址的在上面已经 sendmmsg 处理过了
        break;
      }
      const ssize_t count = ::send(fd_, item.data, item.size, 0);
      if (count >= 0) {
        WriteCallback callback = std::move(item.callback);
        auto keepalive = std::move(item.keepalive);
        auto owned = std::move(item.owned);
        sends_.pop_front();
        if (callback) {
          loop_->post([callback = std::move(callback),
                       keepalive = std::move(keepalive),
                       owned = std::move(owned)]() mutable { callback({}); });
        }
        continue;
      }
      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      if (error == EAGAIN || error == EWOULDBLOCK) {
        break;
      }
      fail_sends(from_socket_error(error));
      return;
    }

    update_interest();
  }

  void fail_sends(Error error) {
    std::deque<SendItem> pending;
    pending.swap(sends_);
    for (auto& item : pending) {
      if (item.callback) {
        loop_->post([callback = std::move(item.callback),
                     keepalive = std::move(item.keepalive),
                     owned = std::move(item.owned),
                     error]() mutable { callback(std::unexpected(error)); });
      }
    }
    update_interest();
  }

  void destroy_with_callbacks() {
    if (fd_ < 0 && !work_ref_) {
      return;
    }

    auto self = shared_from_this();
    if (fd_ >= 0 && watching_) {
      linux_poller(*loop_).del(this);
      watching_ = false;
    }
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
    connected_ = false;
    receiving_ = false;

    auto recv_cb = std::move(recv_callback_);
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    std::deque<SendItem> pending;
    pending.swap(sends_);
    release_ref();

    if (recv_cb) {
      recv_cb(std::unexpected(make_error(ErrorCode::Canceled)));
    }
    for (auto& item : pending) {
      if (item.callback) {
        item.callback(std::unexpected(make_error(ErrorCode::Canceled)));
      }
    }
  }

  void destroy() noexcept {
    if (fd_ >= 0 && watching_) {
      linux_poller(*loop_).del(this);
      watching_ = false;
    }
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
    recv_callback_ = nullptr;
    view_callback_ = nullptr;
    sends_.clear();
    release_ref();
  }

  void release_ref() {
    if (work_ref_) {
      work_ref_ = false;
      loop_->end_work();
    }
  }

  std::shared_ptr<LoopImpl> loop_;
  Family family_ = Family::IPv4;
  UdpOptions options_;
  int fd_ = -1;
  bool connected_ = false;
  bool receiving_ = false;
  bool watching_ = false;
  bool work_ref_ = false;
  std::uint32_t interest_ = 0;
  SocketAddress connected_peer_;
  UdpRecvCallback recv_callback_;
  UdpRecvViewCallback view_callback_;
  bool keepalive_reads_ = false;
  std::size_t recv_chunk_size_ = kMaxDatagramSize;
  bool recv_started_ = false;
  std::vector<std::byte> span_recv_buffer_;
  std::shared_ptr<BufferPool> recv_pool_;
  std::deque<SendItem> sends_;
};

} // namespace detail

UdpSocket::UdpSocket(Loop& loop, Family family, const UdpOptions& options) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::UdpSocketImpl>(impl->shared_from_this(), family, options);
}

UdpSocket::~UdpSocket() = default;

VoidResult UdpSocket::bind(const SocketAddress& local) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->bind(local);
}

VoidResult UdpSocket::connect(const SocketAddress& remote) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->connect(remote);
}

VoidResult UdpSocket::disconnect() {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->disconnect();
}

VoidResult UdpSocket::send(std::span<const std::byte> data, WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send(data, std::move(cb));
}

VoidResult UdpSocket::send(WriteBuffer buffer, WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send(std::move(buffer), std::move(cb));
}

VoidResult UdpSocket::send_to(const SocketAddress& remote,
                              std::span<const std::byte> data,
                              WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_to(remote, data, std::move(cb));
}

VoidResult UdpSocket::send_to(const SocketAddress& remote,
                              WriteBuffer buffer,
                              WriteCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_to(remote, std::move(buffer), std::move(cb));
}

VoidResult UdpSocket::set_recv_options(const UdpRecvOptions& options) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_recv_options(options);
}

VoidResult UdpSocket::start_recv(UdpRecvCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->start_recv(std::move(cb));
}

VoidResult UdpSocket::start_recv(UdpRecvViewCallback cb) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->start_recv(std::move(cb));
}

void UdpSocket::stop_recv() {
  if (impl_) {
    impl_->stop_recv();
  }
}

VoidResult UdpSocket::set_broadcast(bool enable) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_broadcast(enable);
}

VoidResult UdpSocket::set_ttl(int ttl) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_ttl(ttl);
}

VoidResult UdpSocket::set_multicast_ttl(int ttl) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_multicast_ttl(ttl);
}

VoidResult UdpSocket::set_multicast_loop(bool enable) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_multicast_loop(enable);
}

VoidResult UdpSocket::set_multicast_interface(const SocketAddress& iface) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_multicast_interface(iface);
}

VoidResult UdpSocket::set_membership(const SocketAddress& group,
                                     const SocketAddress& iface,
                                     bool join) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_membership(group, iface, join);
}

Result<SocketAddress> UdpSocket::local_address() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->local_address();
}

Result<int> UdpSocket::recv_buffer_size() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->recv_buffer_size();
}

VoidResult UdpSocket::set_recv_buffer_size(int bytes) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_recv_buffer_size(bytes);
}

Result<int> UdpSocket::send_buffer_size() const {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->send_buffer_size();
}

VoidResult UdpSocket::set_send_buffer_size(int bytes) {
  if (!impl_) {
    return std::unexpected(detail::make_error(ErrorCode::BadFileDescriptor));
  }
  return impl_->set_send_buffer_size(bytes);
}

bool UdpSocket::is_open() const {
  return impl_ && impl_->is_open();
}

bool UdpSocket::is_connected() const {
  return impl_ && impl_->is_connected();
}

void UdpSocket::close() {
  if (impl_) {
    impl_->close();
  }
}

} // namespace uvnet
