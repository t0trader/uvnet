#include <thread>
#include <vector>

#include "test_support.hpp"

namespace {

using namespace uvnet;

void test_timers(Backend backend) {
  Loop loop(backend);
  uvtest::Guard guard(loop);

  Timer first(loop);
  Timer second(loop);
  Timer repeat(loop);

  bool first_fired = false;
  bool second_fired = false;
  bool repeat_done = false;
  int repeat_ticks = 0;
  std::vector<int> order;

  auto maybe_finish = [&] {
    if (first_fired && second_fired && repeat_done) {
      guard.cancel();
    }
  };

  CHECK_OK(first.start(std::chrono::milliseconds(30), [&] {
    order.push_back(1);
    first_fired = true;
    maybe_finish();
  }));

  CHECK_OK(second.start(std::chrono::milliseconds(60), [&] {
    order.push_back(2);
    CHECK(first_fired);
    second_fired = true;
    maybe_finish();
  }));

  CHECK_OK(repeat.start(std::chrono::milliseconds(15), std::chrono::milliseconds(15), [&] {
    order.push_back(3);
    ++repeat_ticks;
    if (repeat_ticks == 4) {
      repeat.stop();
      repeat_done = true;
      maybe_finish();
    }
  }));

  loop.run();

  CHECK(first_fired);
  CHECK(second_fired);
  CHECK(repeat_done);
  CHECK(repeat_ticks == 4);
  CHECK(order.front() == 3);
}

void test_async(Backend backend) {
  Loop loop(backend);
  uvtest::Guard guard(loop);

  int calls = 0;
  std::unique_ptr<Async> notifier;

  notifier = std::make_unique<Async>(loop, [&] {
    ++calls;
    notifier->close();
    guard.cancel();
  });

  std::thread sender([&] { notifier->send(); });
  sender.join();

  loop.run();

  CHECK(calls == 1);
}

void test_backend(Backend backend) {
  Loop loop(backend);
  CHECK(loop.backend() == backend);
}

} // namespace

int main() {
#if defined(_WIN32)
  const Backend backends[] = {Backend::Iocp, Backend::Readiness};
#else
  const Backend backends[] = {Backend::Epoll};
#endif
  for (const Backend backend : backends) {
    test_backend(backend);
    test_timers(backend);
    test_async(backend);
  }
  return uvtest::finish("loop");
}
