// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <latch>
#include <thread>
#include <vector>

#include "utils/shared_quota.hpp"

using namespace memgraph::utils;

// 1. Divided into N chunks, but only a single thread is calling
TEST(QuotaTest, SingleThreadUse) {
  const int64_t limit = 1000;
  const int64_t batch = 100;

  QuotaCoordinator coord(limit);
  int64_t processed = 0;

  while (true) {
    auto handle = coord.Acquire(batch);
    if (!handle) break;
    while (handle->Consume(1) > 0) {
      processed++;
    }
  }

  ASSERT_EQ(processed, limit) << "Single thread should consume exactly the limit";
}

// 2. All threads will use up the limit (High Contention)
TEST(QuotaTest, MultiThreadFullConsumption) {
  const int64_t limit = 50000;
  const int thread_count = 8;
  const int64_t batch = 100;

  QuotaCoordinator coord(limit);
  std::atomic<int64_t> global_processed{0};

  auto worker = [&]() {
    while (true) {
      auto handle = coord.Acquire(batch);
      if (!handle) break;
      while (handle->Consume(1) > 0) {
        global_processed.fetch_add(1, std::memory_order_relaxed);
      }
    }
  };

  std::vector<std::jthread> threads;
  for (int i = 0; i < thread_count; ++i) threads.emplace_back(worker);
  threads.clear();  // Join all

  ASSERT_EQ(global_processed.load(), limit) << "Threads should sum up to exactly the limit";
}

// 3. One thread did not use up the limit, others did (Return Unused)
// This tests if returned quota is correctly picked up by others.
TEST(QuotaTest, ReturnUnusedQuota) {
  const int64_t limit = 200;
  QuotaCoordinator coord(limit);
  std::atomic<int64_t> global_processed{0};

  // Barrier to ensure Thread A grabs the first chunk before Thread B starts
  std::latch start_latch(2);

  // Thread A: Grabs a big chunk, but only processes 1 item, returns the rest.
  std::jthread t1([&]() {
    start_latch.arrive_and_wait();
    // Request a huge batch
    auto handle = coord.Acquire(100);
    // Consume 1
    if (handle->Consume(1) > 0) global_processed++;
    // Destructor returns 99 to the pool
  });

  // Thread B: Standard worker, should pick up the 99 returned by A + the rest.
  std::jthread t2([&]() {
    start_latch.arrive_and_wait();
    // Little sleep to let T1 grab the first chunk
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    while (true) {
      auto handle = coord.Acquire(10);
      if (!handle) break;
      while (handle->Consume(1) > 0) {
        global_processed++;
      }
    }
  });

  t1.join();
  t2.join();

  ASSERT_EQ(global_processed.load(), limit) << "Total processed should be limit, despite partial returns";
}

// 4. Some threads wait for more, while some are finished and return the leftover
// This covers the specific DEADLOCK SCENARIO where Quota=0, but ActiveHolders > 0.
TEST(QuotaTest, WaitAndWakeScenario) {
  const int64_t limit = 100;
  QuotaCoordinator coord(limit);
  std::atomic<int64_t> global_processed{0};

  std::latch t1_Acquired(1);

  // Thread A: Takes ALL the quota immediately.
  std::jthread t1([&]() {
    auto handle = coord.Acquire(100);  // Takes everything. Quota is now 0.
    t1_Acquired.count_down();          // Signal T2 to start trying

    // Simulate work...
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Consume half
    for (int i = 0; i < 50; ++i) {
      handle->Consume(1);
      global_processed++;
    }
    // Handle destroys here, RETURNING 50 to the pool.
    // This should WAKE UP T2.
  });

  // Thread B: Tries to Acquire when Quota is 0. Should Block, then Wake.
  std::jthread t2([&]() {
    t1_Acquired.wait();  // Ensure T1 has drained the pool first

    // This call should BLOCK until T1 destroys its handle->
    auto handle = coord.Acquire(10);

    // If we are here, T1 must have returned quota.
    while (handle) {
      while (handle->Consume(1) > 0) {
        global_processed++;
      }
      // Have to destroy the handle before reacquiring it
      handle.reset();
      handle = coord.Acquire(10);
    }
  });

  t1.join();
  t2.join();

  ASSERT_EQ(global_processed.load(), limit);
}

// 5. Scenario where threads finish EXACTLY at the limit (Active Holders -> 0 notification)
// This ensures that if the limit is 0 and no one returns anything (just finish),
// waiting threads are notified that the job is done.
TEST(QuotaTest, WakeOnZeroRemaining) {
  const int64_t limit = 50;
  QuotaCoordinator coord(limit);
  std::atomic<int64_t> global_processed{0};
  std::latch t1_holding(1);

  // Thread A: Takes all 50, consumes all 50. Returns 0.
  std::jthread t1([&]() {
    auto handle = coord.Acquire(50);
    while (handle) {
      // Consume until locally we have all the remaining quota
      if (handle->Count() + global_processed.load() == limit) t1_holding.count_down();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      while (handle->Consume(1) > 0) global_processed++;
      handle.reset();
      handle = coord.Acquire(50);
    }
  });

  // Thread B: Sees 0 quota, Waits.
  // Should wake up when T1 finishes, realize active_holders is 0, and exit.
  std::jthread t2([&]() {
    t1_holding.wait();
    // Should block here, then return handle with 0 count
    auto handle = coord.Acquire(10);
    ASSERT_FALSE(handle);
  });

  t1.join();
  t2.join();

  ASSERT_EQ(global_processed.load(), limit);
}

// 6. Zero Limit Edge Case
TEST(QuotaTest, ZeroLimitInit) {
  QuotaCoordinator coord(0);
  auto handle = coord.Acquire(100);
  ASSERT_FALSE(handle);
}

// 7. Variable Consumption Amount
TEST(QuotaTest, VariableConsumption) {
  const int64_t limit = 100;
  const int64_t batch = 10;

  QuotaCoordinator coord(limit);
  int64_t processed = 0;

  while (true) {
    auto handle = coord.Acquire(batch);
    if (!handle) break;
    // Consume in chunks of 3
    int64_t increment = 3;
    while (true) {
      int64_t consumed = handle->Consume(increment);
      if (consumed == 0) break;
      processed += consumed;
      // If we got less than asked, it means handle is empty, loop will break on next iteration or handle re-acquire
      // logic is needed if we were manual. But here we just drain the handle.
    }
  }
  ASSERT_EQ(processed, limit);
}

// ============================================================================
// SharedQuota Tests
// ============================================================================

// 1. Basic construction and usage
TEST(SharedQuotaTest, BasicUsage) {
  const int64_t limit = 100;
  const int64_t n = 10;

  SharedQuota quota(limit, n);
  int64_t consumed = 0;

  while (quota.Decrement(1) > 0) {
    consumed++;
  }

  ASSERT_EQ(consumed, limit) << "SharedQuota should consume exactly the limit";
}

// 2. Copy constructor - both instances share the same coordinator
TEST(SharedQuotaTest, CopyConstructor) {
  const int64_t limit = 100;
  const int64_t n = 10;

  SharedQuota original(limit, n);
  SharedQuota copy(original);
  copy.Reacquire();  // This is just for the test

  std::atomic<int64_t> original_consumed{0};
  std::atomic<int64_t> copy_consumed{0};

  // Consume from both in separate threads to avoid deadlock
  std::jthread thread_original([&]() {
    while (original.Decrement(1) > 0) {
      original_consumed++;
    }
  });

  std::jthread thread_copy([&]() {
    while (copy.Decrement(1) > 0) {
      copy_consumed++;
    }
  });

  thread_original.join();
  thread_copy.join();

  ASSERT_EQ(original_consumed + copy_consumed, limit) << "Copy and original should share the same quota pool";
  ASSERT_GT(original_consumed.load(), 0) << "Original should have consumed some";
  ASSERT_GT(copy_consumed.load(), 0) << "Copy should have consumed some";
}

// 3. Copy assignment operator
TEST(SharedQuotaTest, CopyAssignment) {
  const int64_t limit = 100;
  const int64_t n = 10;

  SharedQuota original(limit, n);
  SharedQuota other(50, 5);  // Different quota

  // Consume some from original first
  std::atomic<int64_t> original_consumed{0};
  for (int i = 0; i < 30 && original.Decrement(1) > 0; ++i) {
    original_consumed++;
  }

  // Copy assign - other now shares original's coordinator
  other = original;

  std::atomic<int64_t> other_consumed{0};

  // Consume remaining from both in separate threads to avoid deadlock
  std::jthread thread_original([&]() {
    while (original.Decrement(1) > 0) {
      original_consumed++;
    }
  });

  std::jthread thread_other([&]() {
    while (other.Decrement(1) > 0) {
      other_consumed++;
    }
  });

  thread_original.join();
  thread_other.join();

  ASSERT_EQ(original_consumed + other_consumed, limit)
      << "After copy assignment, both should share the same quota pool";
}

// 4. Move constructor - transfers ownership
TEST(SharedQuotaTest, MoveConstructor) {
  const int64_t limit = 100;
  const int64_t n = 10;

  SharedQuota original(limit, n);

  // Consume some first
  int64_t consumed = 0;
  for (int i = 0; i < 30 && original.Decrement(1) > 0; ++i) {
    consumed++;
  }

  // Move construct
  SharedQuota moved(std::move(original));

  // Original should no longer work (handle is nullopt)
  ASSERT_EQ(original.Decrement(1), 0) << "Moved-from SharedQuota should not work";

  // Moved should continue consuming
  while (moved.Decrement(1) > 0) {
    consumed++;
  }

  ASSERT_EQ(consumed, limit) << "Move constructor should preserve remaining quota";
}

// 5. Move assignment operator
TEST(SharedQuotaTest, MoveAssignment) {
  const int64_t limit = 100;
  const int64_t n = 10;

  SharedQuota original(limit, n);
  SharedQuota other(50, 5);

  // Consume some from original
  int64_t consumed = 0;
  for (int i = 0; i < 40 && original.Decrement(1) > 0; ++i) {
    consumed++;
  }

  // Move assign
  other = std::move(original);

  // Original should no longer work
  ASSERT_EQ(original.Decrement(1), 0) << "Moved-from SharedQuota should not work";

  // Other should continue where original left off
  while (other.Decrement(1) > 0) {
    consumed++;
  }

  ASSERT_EQ(consumed, limit) << "Move assignment should preserve remaining quota";
}

// 6. Self-copy assignment (edge case)
TEST(SharedQuotaTest, SelfCopyAssignment) {
  const int64_t limit = 50;
  const int64_t n = 5;

  SharedQuota quota(limit, n);

  // Consume some
  int64_t consumed = 0;
  for (int i = 0; i < 20 && quota.Decrement(1) > 0; ++i) {
    consumed++;
  }

  // Self assignment should be safe
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wself-assign-overloaded"
  quota = quota;
#pragma GCC diagnostic pop

  // Should still work
  while (quota.Decrement(1) > 0) {
    consumed++;
  }

  ASSERT_EQ(consumed, limit) << "Self-copy assignment should not break the quota";
}

// 7. Self-move assignment (edge case)
TEST(SharedQuotaTest, SelfMoveAssignment) {
  const int64_t limit = 50;
  const int64_t n = 5;

  SharedQuota quota(limit, n);

  // Consume some
  int64_t consumed = 0;
  for (int i = 0; i < 20 && quota.Decrement(1) > 0; ++i) {
    consumed++;
  }

  // Self move assignment should be safe
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wself-move"
  quota = std::move(quota);
#pragma GCC diagnostic pop

  // Should still work
  while (quota.Decrement(1) > 0) {
    consumed++;
  }

  ASSERT_EQ(consumed, limit) << "Self-move assignment should not break the quota";
}

// 8. Reset method
TEST(SharedQuotaTest, ResetMethod) {
  const int64_t limit = 100;
  const int64_t n = 2;

  SharedQuota quota(limit, n);

  // Consume exactly one batch worth
  int64_t consumed = 0;
  for (int i = 0; i < 25 && quota.Decrement(1) > 0; ++i) {
    consumed++;
  }

  // Reset to get a new handle
  quota.Reacquire();

  // Continue consuming
  while (quota.Decrement(1) > 0) {
    consumed++;
  }

  ASSERT_EQ(consumed, limit) << "Reset should allow continued consumption";
}

// 9. Multiple copies in multi-threaded environment
TEST(SharedQuotaTest, MultiThreadWithCopies) {
  const int64_t limit = 10000;
  const int64_t n = 100;
  const int thread_count = 4;

  SharedQuota original(limit, n);
  std::atomic<int64_t> global_consumed{0};

  auto worker = [&global_consumed](SharedQuota quota) {
    while (quota.Decrement(1) > 0) {
      global_consumed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::jthread> threads;
  for (int i = 0; i < thread_count; ++i) {
    // Each thread gets a COPY of the SharedQuota
    threads.emplace_back(worker, original);
  }

  // Original also participates
  while (original.Decrement(1) > 0) {
    global_consumed.fetch_add(1, std::memory_order_relaxed);
  }

  threads.clear();  // Join all

  ASSERT_EQ(global_consumed.load(), limit) << "All copies should share the same quota pool in multi-threaded usage";
}

// 10. Variable Decrement Amount
TEST(SharedQuotaTest, VariableDecrement) {
  const int64_t limit = 100;
  const int64_t n = 20;

  SharedQuota quota(limit, n);
  int64_t consumed = 0;

  // Consume with increments of 5
  while (true) {
    int64_t res = quota.Decrement(5);
    if (res == 0) break;
    consumed += res;
  }

  ASSERT_EQ(consumed, limit);

  // Test with arbitrary increments
  SharedQuota quota2(limit, n);
  consumed = 0;
  int64_t res = quota2.Decrement(33);  // 33
  consumed += res;
  ASSERT_EQ(res, 33);

  res = quota2.Decrement(33);  // 66
  consumed += res;
  ASSERT_EQ(res, 33);

  res = quota2.Decrement(33);  // 99
  consumed += res;
  ASSERT_EQ(res, 33);

  res = quota2.Decrement(33);  // 100 - only 1 left
  consumed += res;
  ASSERT_EQ(res, 1);

  res = quota2.Decrement(33);  // full
  ASSERT_EQ(res, 0);

  ASSERT_EQ(consumed, limit);
}
