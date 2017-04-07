#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <atomic>
#include "utils/scheduler.hpp"

/**
 * Scheduler runs every 2 seconds and increases one variable. Test thread
 * increases other variable. Scheduler checks if variables have the same
 * value.
 */
TEST(Scheduler, TestFunctionExecuting) {
  std::atomic<int> x{0}, y{0};
  std::function<void()> func{[&x, &y]() {
    EXPECT_EQ(y.load(), x.load());
    x++;
  }};
  Scheduler<> scheduler;
  scheduler.Run(std::chrono::seconds(1), func);

  std::this_thread::sleep_for(std::chrono::milliseconds(980));
  y++;
  EXPECT_EQ(x.load(), y.load());

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler.Stop();
  y++;
  EXPECT_EQ(x.load(), y.load());
}
