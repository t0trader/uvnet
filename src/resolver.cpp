#include "uvnet/resolver.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#if !defined(_WIN32)
#include <netdb.h>
#endif

#include "detail/loop.hpp"
#include "detail/platform.hpp"

namespace uvnet {
namespace {

// 惰性初始化的 DNS 线程池；所有回调最终都投递回循环线程
class ThreadPool {
 public:
  static ThreadPool& instance() {
    static ThreadPool pool;
    return pool;
  }

  void submit(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) {
        return;
      }
      tasks_.push_back(std::move(task));
    }
    condition_.notify_one();
  }

 private:
  ThreadPool() {
    std::size_t count = std::thread::hardware_concurrency();
    if (count == 0) {
      count = 2;
    }
    count = std::clamp<std::size_t>(count, 2, 4);
    for (std::size_t i = 0; i < count; ++i) {
      workers_.emplace_back([this] { worker_loop(); });
    }
  }

  ~ThreadPool() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    condition_.notify_all();
    for (auto& worker : workers_) {
      worker.join();
    }
  }

  void worker_loop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        if (stopping_) {
          return;
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      if (task) {
        task();
      }
    }
  }

  std::vector<std::thread> workers_;
  std::deque<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool stopping_ = false;
};

int native_socket_type(SocketType type) {
  switch (type) {
    case SocketType::Stream: return SOCK_STREAM;
    case SocketType::Datagram: return SOCK_DGRAM;
    default: return 0;
  }
}

} // namespace

namespace detail {

class ResolverImpl : public std::enable_shared_from_this<ResolverImpl> {
 public:
  explicit ResolverImpl(std::shared_ptr<LoopImpl> loop) : loop_(std::move(loop)) {}

  ~ResolverImpl() { alive_.store(false, std::memory_order_release); }

  void resolve(std::string host,
               std::uint16_t port,
               ResolveOptions options,
               Resolver::ResolveCallback callback) {
    if (!callback) {
      return;
    }

    loop_->begin_work();
    auto self = shared_from_this();

    ThreadPool::instance().submit(
        [self, host = std::move(host), port, options, callback = std::move(callback)]() mutable {
          ensure_winsock();

          addrinfo hints{};
          hints.ai_family = AddressCodec::native_family(options.family);
          hints.ai_socktype = native_socket_type(options.socket_type);
          hints.ai_protocol = 0;
          hints.ai_flags = 0;
          if (options.passive && host.empty()) {
            hints.ai_flags |= AI_PASSIVE;
          }
          if (options.numeric_host) {
            hints.ai_flags |= AI_NUMERICHOST;
          }

          const std::string service = std::to_string(port);
          const char* node = host.empty() ? nullptr : host.c_str();

          addrinfo* result = nullptr;
          const int error = ::getaddrinfo(node, service.c_str(), &hints, &result);

          Result<std::vector<SocketAddress>> converted = std::vector<SocketAddress>{};
          if (error != 0) {
            converted = std::unexpected(from_gai_error(error));
          } else {
            std::vector<SocketAddress> addresses;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
              if (it->ai_addr == nullptr) {
                continue;
              }
              SocketAddress address =
                  AddressCodec::from_native(it->ai_addr, static_cast<int>(it->ai_addrlen));
              if (address.family() != Family::Unspecified) {
                addresses.push_back(address);
              }
            }
            if (result != nullptr) {
              ::freeaddrinfo(result);
            }

            if (addresses.empty()) {
              converted = std::unexpected(make_error(ErrorCode::HostNotFound));
            } else {
              converted = std::move(addresses);
            }
          }

          self->loop_->post([self, callback = std::move(callback),
                             converted = std::move(converted)]() mutable {
            if (self->alive_.load(std::memory_order_acquire) && callback) {
              callback(std::move(converted));
            }
            self->loop_->end_work();
          });
        });
  }

  void reverse(const SocketAddress& address, Resolver::ReverseCallback callback) {
    if (!callback) {
      return;
    }

    loop_->begin_work();
    auto self = shared_from_this();

    ThreadPool::instance().submit([self, address, callback = std::move(callback)]() mutable {
      ensure_winsock();

      sockaddr_storage storage{};
      int length = 0;
      if (!AddressCodec::to_native(address, storage, length)) {
        self->loop_->post([self, callback = std::move(callback)]() mutable {
          if (self->alive_.load(std::memory_order_acquire) && callback) {
            callback(std::unexpected(make_error(ErrorCode::InvalidArgument)));
          }
          self->loop_->end_work();
        });
        return;
      }

      char hostname[1025] = {};
      const int error = ::getnameinfo(reinterpret_cast<sockaddr*>(&storage),
                                      static_cast<socklen_t>(length),
                                      hostname,
                                      sizeof(hostname),
                                      nullptr,
                                      0,
                                      0);

      Result<std::string> text = std::string(hostname);
      if (error != 0) {
        text = std::unexpected(from_gai_error(error));
      }

      self->loop_->post([self, callback = std::move(callback), text = std::move(text)]() mutable {
        if (self->alive_.load(std::memory_order_acquire) && callback) {
          callback(std::move(text));
        }
        self->loop_->end_work();
      });
    });
  }

 private:
  std::shared_ptr<LoopImpl> loop_;
  std::atomic<bool> alive_{true};
};

} // namespace detail

Resolver::Resolver(Loop& loop) {
  auto* impl = loop.native();
  if (impl == nullptr) {
    throw std::runtime_error("uvnet: invalid loop");
  }
  impl_ = std::make_shared<detail::ResolverImpl>(impl->shared_from_this());
}

Resolver::~Resolver() = default;

void Resolver::resolve(std::string host,
                       std::uint16_t port,
                       ResolveOptions options,
                       ResolveCallback cb) {
  if (impl_) {
    impl_->resolve(std::move(host), port, options, std::move(cb));
  }
}

void Resolver::reverse(const SocketAddress& address, ReverseCallback cb) {
  if (impl_) {
    impl_->reverse(address, std::move(cb));
  }
}

} // namespace uvnet
