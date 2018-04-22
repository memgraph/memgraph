#include <atomic>

#include "gtest/gtest.h"

#include "utils/watchdog.hpp"

using namespace std::chrono_literals;

TEST(Watchdog, Run) {
  std::atomic<int> count(0);
  utils::Watchdog dog(200ms, 200ms, [&count]() { ++count; });

  std::this_thread::sleep_for(250ms);
  EXPECT_EQ(count, 1);

  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(count, 2);

  std::this_thread::sleep_for(50ms);
  dog.Notify();

  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(count, 2);
  dog.Notify();

  std::this_thread::sleep_for(250ms);
  EXPECT_EQ(count, 3);
}

TEST(Watchdog, Blocker) {
  std::atomic<int> count(0);
  utils::Watchdog dog(200ms, 200ms, [&count]() { ++count; });

  std::this_thread::sleep_for(250ms);
  EXPECT_EQ(count, 1);

  dog.Block();

  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(count, 1);

  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(count, 1);

  dog.Unblock();

  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(count, 1);

  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(count, 2);
  dog.Notify();

  std::this_thread::sleep_for(100ms);
  dog.Unblock();

  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(count, 3);
}
