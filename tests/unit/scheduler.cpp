#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/scheduler.hpp"
#include <atomic>

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
  Scheduler<> scheduler(std::chrono::seconds(1), func);
  
  std::this_thread::sleep_for(std::chrono::milliseconds(980)); 
  y++;
  EXPECT_EQ(x.load(), y.load());

 
  std::this_thread::sleep_for(std::chrono::seconds(1));
  y++;
  EXPECT_EQ(x.load(), y.load());

  std::this_thread::sleep_for(std::chrono::seconds(1));
  y++;
  EXPECT_EQ(x.load(), y.load());

  EXPECT_EQ(x.load(), 3);
}

/**
 * Scheduler will not run because time is set to -1.
 */
TEST(Scheduler, DoNotRunScheduler) {
  std::atomic<int> x{0}; 
  std::function<void()> func{[&x]() { x++; }};
  Scheduler<> scheduler(std::chrono::seconds(-1), func);

  std::this_thread::sleep_for(std::chrono::seconds(3));

  EXPECT_EQ(x.load(), 0);
}
