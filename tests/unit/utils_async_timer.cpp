#include <chrono>

#include "gtest/gtest.h"

#include "utils/async_timer.hpp"

using AsyncTimer = utils::AsyncTimer;

void checkTimeSimple() {
  const auto before = std::chrono::steady_clock::now();
  AsyncTimer timer{1};
  auto is_expired = timer.IsExpired();
  while (!is_expired) {
    is_expired = timer.IsExpired();
  }

  const auto after = std::chrono::steady_clock::now();
  constexpr auto kMinimumElapsedMs = 1000;
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(after - before).count(), kMinimumElapsedMs);
}

// TODO(Benjamin Antal) Somehow mock the system calls? Wrap into a class?
TEST(AsyncTimer, SimpleWait) { checkTimeSimple(); }

TEST(AsyncTimer, DoubleWait) {
  checkTimeSimple();
  checkTimeSimple();
}
