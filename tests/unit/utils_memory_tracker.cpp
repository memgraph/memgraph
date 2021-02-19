#include <thread>

#include <gtest/gtest.h>

#include <utils/memory_tracker.hpp>

TEST(MemoryTrackerTest, ExceptionEnabler) {
  utils::MemoryTracker memory_tracker;

  constexpr size_t hard_limit = 10;
  memory_tracker.SetHardLimit(hard_limit);

  std::atomic<bool> can_continue{false};
  std::atomic<bool> enabler_created{false};
  std::thread t1{[&] {
    // wait until the second thread creates exception enabler
    while (!enabler_created)
      ;
    ASSERT_NO_THROW(memory_tracker.Alloc(hard_limit + 1));
    ASSERT_EQ(memory_tracker.Amount(), hard_limit + 1);

    // tell the second thread it can finish its test
    can_continue = true;
  }};

  std::thread t2{[&] {
    utils::MemoryTracker::OutOfMemoryExceptionEnabler exception_enabler;
    enabler_created = true;
    ASSERT_THROW(memory_tracker.Alloc(hard_limit + 1), utils::OutOfMemoryException);

    // hold the enabler until the first thread finishes
    while (!can_continue)
      ;
  }};

  t1.join();
  t2.join();
}

TEST(MemoryTrackerTest, ExceptionBlocker) {
  utils::MemoryTracker memory_tracker;

  constexpr size_t hard_limit = 10;
  memory_tracker.SetHardLimit(hard_limit);

  utils::MemoryTracker::OutOfMemoryExceptionEnabler exception_enabler;
  {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker exception_blocker;

    ASSERT_NO_THROW(memory_tracker.Alloc(hard_limit + 1));
    ASSERT_EQ(memory_tracker.Amount(), hard_limit + 1);
  }
  ASSERT_THROW(memory_tracker.Alloc(hard_limit + 1), utils::OutOfMemoryException);
}
