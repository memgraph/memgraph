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

#include <atomic>
#include <chrono>
#include <optional>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "utils/skip_list.hpp"

using namespace memgraph::utils;

TEST(SkipListRangeChunking, BasicRangeChunking) {
  SkipList<int> skiplist;
  // NOTE: Using few elements to force using 0 layer. This makes the checks easier, since this layer is completely
  // filled.

  // Insert elements from 0 to 200
  auto accessor = skiplist.access();
  for (int i = 0; i <= 200; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  auto chunks = accessor.create_chunks(10, std::optional<int>{10}, std::optional<int>{160});
  ASSERT_GT(chunks.size(), 0);

  // Process chunks and verify we only get elements in the range
  std::vector<std::jthread> threads;
  std::vector<int> collected_elements;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &collected_elements]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is within range
        ASSERT_GE(*it, 10);
        ASSERT_LE(*it, 160);  // Range is inclusive, so 160 + 1
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 151 / 10 * 3);
      }

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  threads.clear();

  // Verify we got all elements in the range
  ASSERT_EQ(collected_elements.size(), 151);

  // Verify all elements are unique and in range
  std::sort(collected_elements.begin(), collected_elements.end());
  for (size_t i = 0; i < collected_elements.size(); ++i) {
    ASSERT_EQ(collected_elements[i], 10 + static_cast<int>(i));
  }
}

TEST(SkipListRangeChunking, LowerRangeChunking) {
  SkipList<int> skiplist;
  // NOTE: Using few elements to force using 0 layer. This makes the checks easier, since this layer is completely
  // filled.

  // Insert elements from 0 to 200
  auto accessor = skiplist.access();
  for (int i = 0; i < 200; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  auto chunks = accessor.create_chunks(10, std::optional<int>{71}, std::optional<int>{std::nullopt});
  ASSERT_GT(chunks.size(), 0);

  // Process chunks and verify we only get elements in the range
  std::vector<std::jthread> threads;
  std::vector<int> collected_elements;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &collected_elements]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is within range
        ASSERT_GE(*it, 71);
        ASSERT_LE(*it, 199);
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 129 / 10 * 3);
      }

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  threads.clear();

  // Verify we got all elements in the range
  ASSERT_EQ(collected_elements.size(), 129);

  // Verify all elements are unique and in range
  std::sort(collected_elements.begin(), collected_elements.end());
  for (size_t i = 0; i < collected_elements.size(); ++i) {
    ASSERT_EQ(collected_elements[i], 71 + static_cast<int>(i));
  }
}

TEST(SkipListRangeChunking, UpperRangeChunking) {
  SkipList<int> skiplist;
  // NOTE: Using few elements to force using 0 layer. This makes the checks easier, since this layer is completely
  // filled.

  // Insert elements from 0 to 200
  auto accessor = skiplist.access();
  for (int i = 0; i < 200; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  auto chunks = accessor.create_chunks(10, std::optional<int>{std::nullopt}, std::optional<int>{134});
  ASSERT_GT(chunks.size(), 0);

  // Process chunks and verify we only get elements in the range
  std::vector<std::jthread> threads;
  std::vector<int> collected_elements;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &collected_elements]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is within range
        ASSERT_GE(*it, 0);
        ASSERT_LE(*it, 135);
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 135 / 10 * 3);
      }

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  threads.clear();

  // Verify we got all elements in the range
  ASSERT_EQ(collected_elements.size(), 135);

  // Verify all elements are unique and in range
  std::sort(collected_elements.begin(), collected_elements.end());
  for (size_t i = 0; i < collected_elements.size(); ++i) {
    ASSERT_EQ(collected_elements[i], static_cast<int>(i));
  }
}

// ============================================================================
// TEST 4: Edge Cases Test
// ============================================================================

TEST(SkipListRangeChunking, EdgeCases) {
  SkipList<int> skiplist;

  // Insert elements from 0 to 100
  auto accessor = skiplist.access();
  for (int i = 0; i < 100; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 100);

  // Test 1: Empty range (lower > upper)
  auto empty_chunks = accessor.create_chunks(4, std::optional<int>{200}, std::optional<int>{100});
  ASSERT_GT(empty_chunks.size(), 0);

  int empty_count = 0;
  for (auto &chunk : empty_chunks) {
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      std::cout << "val: " << *it << std::endl;
      empty_count++;
    }
  }
  ASSERT_EQ(empty_count, 0);

  // Test 2: Single element range
  auto single_chunks = accessor.create_chunks(4, std::optional<int>{50}, std::optional<int>{50});
  ASSERT_GT(single_chunks.size(), 0);

  int single_count = 0;
  for (auto &chunk : single_chunks) {
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      single_count++;
      ASSERT_EQ(*it, 50);
    }
  }
  ASSERT_EQ(single_count, 1);

  // Test 3: Range with no elements (outside existing range)
  auto no_elements_chunks = accessor.create_chunks(4, std::optional<int>{200}, std::optional<int>{300});
  ASSERT_GT(no_elements_chunks.size(), 0);

  int no_elements_count = 0;
  for (auto &chunk : no_elements_chunks) {
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      std::cout << "val: " << *it << std::endl;
      no_elements_count++;
    }
  }
  ASSERT_EQ(no_elements_count, 0);

  // Test 4: Range at the beginning
  auto beginning_chunks = accessor.create_chunks(4, std::optional<int>{0}, std::optional<int>{10});
  ASSERT_GT(beginning_chunks.size(), 0);

  int beginning_count = 0;
  for (auto &chunk : beginning_chunks) {
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      beginning_count++;
      ASSERT_GE(*it, 0);
      ASSERT_LE(*it, 10);
    }
  }
  ASSERT_EQ(beginning_count, 11);

  // Test 5: Range at the end
  auto end_chunks = accessor.create_chunks(4, std::optional<int>{90}, std::optional<int>{99});
  ASSERT_GT(end_chunks.size(), 0);

  int end_count = 0;
  for (auto &chunk : end_chunks) {
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      end_count++;
      ASSERT_GE(*it, 90);
      ASSERT_LE(*it, 99);
    }
  }
  ASSERT_EQ(end_count, 10);

  // Test 5: Out of range at the end
  auto end_out_of_range = accessor.create_chunks(4, std::optional<int>{90}, std::optional<int>{110});
  ASSERT_GT(end_out_of_range.size(), 0);

  int out_of_range_count = 0;
  for (auto &chunk : end_out_of_range) {
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      out_of_range_count++;
      ASSERT_GE(*it, 90);
      ASSERT_LE(*it, 99);
    }
  }
  ASSERT_EQ(out_of_range_count, 10);
}

TEST(SkipListRangeChunking, BigDatasetRangeChunking) {
  SkipList<int> skiplist;

  // Insert large dataset from 0 to 100000
  auto accessor = skiplist.access();
  for (int i = 0; i <= 100000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 100001);

  // Test chunking with range [10000, 90000]
  auto chunks = accessor.create_chunks(16, std::optional<int>{10000}, std::optional<int>{90000});
  ASSERT_GT(chunks.size(), 0);

  // Process chunks and verify we only get elements in the range
  std::vector<std::jthread> threads;
  std::vector<int> collected_elements;
  std::atomic<int> total_processed{0};

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &collected_elements, &total_processed]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is within range
        ASSERT_GE(*it, 10000);
        ASSERT_LE(*it, 90000);
      }

      total_processed.fetch_add(local_count, std::memory_order_relaxed);

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  threads.clear();

  // Verify we got all elements in the range
  ASSERT_EQ(total_processed.load(), 80001);  // 90000 - 10000 + 1 = 80001 elements
  ASSERT_EQ(collected_elements.size(), 80001);

  // Verify all elements are unique and in range
  std::sort(collected_elements.begin(), collected_elements.end());
  for (size_t i = 0; i < collected_elements.size(); ++i) {
    ASSERT_EQ(collected_elements[i], 10000 + static_cast<int>(i));
  }
}

TEST(SkipListRangeChunking, BigDatasetLowerBoundOnly) {
  SkipList<int> skiplist;

  // Insert large dataset from 0 to 100000
  auto accessor = skiplist.access();
  for (int i = 0; i <= 100000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 100001);

  // Test chunking with only lower bound (from 50000 onwards)
  auto chunks = accessor.create_chunks(12, std::optional<int>{50000}, std::optional<int>{std::nullopt});
  ASSERT_GT(chunks.size(), 0);

  // Process chunks and verify we only get elements >= 50000
  std::vector<std::jthread> threads;
  std::vector<int> collected_elements;
  std::atomic<int> total_processed{0};

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &collected_elements, &total_processed]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is >= lower bound
        ASSERT_GE(*it, 50000);
      }
      EXPECT_GT(local_count, 0);
      EXPECT_LT(local_count, 50000 / 12 * 3);

      total_processed.fetch_add(local_count, std::memory_order_relaxed);

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  threads.clear();

  // Verify we got all elements >= 50000
  ASSERT_EQ(total_processed.load(), 50001);  // 100000 - 50000 + 1 = 50001 elements
  ASSERT_EQ(collected_elements.size(), 50001);

  // Verify all elements are unique and >= 50000
  std::sort(collected_elements.begin(), collected_elements.end());
  for (size_t i = 0; i < collected_elements.size(); ++i) {
    ASSERT_EQ(collected_elements[i], 50000 + static_cast<int>(i));
  }
}

TEST(SkipListRangeChunking, BigDatasetUpperBoundOnly) {
  SkipList<int> skiplist;

  // Insert large dataset from 0 to 100000
  auto accessor = skiplist.access();
  for (int i = 0; i <= 100000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 100001);

  // Test chunking with only upper bound (up to 75000)
  auto chunks = accessor.create_chunks(12, std::optional<int>{std::nullopt}, std::optional<int>{75000});
  ASSERT_GT(chunks.size(), 0);

  // Process chunks and verify we only get elements <= 75000
  std::vector<std::jthread> threads;
  std::vector<int> collected_elements;
  std::atomic<int> total_processed{0};

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &collected_elements, &total_processed]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is <= upper bound
        ASSERT_LE(*it, 75000);
      }

      EXPECT_GT(local_count, 0);
      EXPECT_LT(local_count, 75000 / 12 * 3);
      total_processed.fetch_add(local_count, std::memory_order_relaxed);

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  threads.clear();

  // Verify we got all elements <= 75000
  ASSERT_EQ(total_processed.load(), 75001);  // 75000 - 0 + 1 = 75001 elements
  ASSERT_EQ(collected_elements.size(), 75001);

  // Verify all elements are unique and <= 75000
  std::sort(collected_elements.begin(), collected_elements.end());
  for (size_t i = 0; i < collected_elements.size(); ++i) {
    ASSERT_EQ(collected_elements[i], static_cast<int>(i));
  }
}

TEST(SkipListRangeChunking, BigDatasetConcurrentOperations) {
  SkipList<int> skiplist;

  // Insert initial large dataset
  auto accessor = skiplist.access();
  for (int i = 0; i <= 50000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 50001);

  // Create chunks for range [10000, 40000]
  auto chunks = accessor.create_chunks(8, std::optional<int>{10000}, std::optional<int>{40000});
  ASSERT_GT(chunks.size(), 0);

  // Start concurrent insertion threads
  std::atomic<bool> stop_insertions{false};
  std::atomic<int> inserted_count{0};

  std::vector<std::jthread> insertion_threads;
  for (int t = 0; t < 4; ++t) {
    insertion_threads.emplace_back([&skiplist, &stop_insertions, &inserted_count, t]() {
      auto thread_accessor = skiplist.access();
      int new_value = 100000 + t * 10000;
      while (!stop_insertions.load(std::memory_order_acquire)) {
        auto res = thread_accessor.insert(new_value++);
        if (res.second) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  // Let the processing run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Process chunks in parallel
  std::atomic<int> total_processed{0};
  std::vector<std::jthread> processing_threads;

  for (size_t i = 0; i < chunks.size(); ++i) {
    processing_threads.emplace_back([&chunks, i, &total_processed]() {
      int local_count = 0;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        // Verify element is within the specified range
        ASSERT_GE(*it, 10000);
        ASSERT_LE(*it, 40000);
        if (local_count % 1000 == 0) {
          // Make sure the iterator and insertions are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      EXPECT_GT(local_count, 0);
      EXPECT_LT(local_count, 30000 / 8 * 3);
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  // Stop insertions
  stop_insertions.store(true, std::memory_order_release);
  for (auto &thread : insertion_threads) {
    thread.join();
  }

  // Wait for processing threads
  for (auto &thread : processing_threads) {
    thread.join();
  }

  // Verify we processed exactly the elements in the range
  ASSERT_EQ(total_processed.load(), 30001);  // 40000 - 10000 + 1 = 30001 elements
}

TEST(SkipListRangeChunking, BigDatasetConcurrentDeletionsAndInsertions) {
  SkipList<int> skiplist;
  {
    // Insert initial large dataset
    auto accessor = skiplist.access();
    for (int i = 0; i <= 50000; ++i) {
      auto res = accessor.insert(i);
      ASSERT_TRUE(res.second);
    }

    ASSERT_EQ(accessor.size(), 50001);
  }

  auto accessor = skiplist.access();
  // Create chunks for range [10000, 40000]
  auto chunks = accessor.create_chunks(8, std::optional<int>{10000}, std::optional<int>{40000});
  ASSERT_GT(chunks.size(), 0);

  // Start concurrent deletion and insertion threads
  std::atomic<bool> stop_operations{false};
  std::atomic<int> deleted_count{0};
  std::atomic<int> inserted_count{0};
  std::atomic<int> reinserted_count{0};

  // Deletion threads - delete elements from the range
  std::vector<std::jthread> deletion_threads;
  for (int t = 0; t < 2; ++t) {
    deletion_threads.emplace_back([&skiplist, &stop_operations, &deleted_count, t]() {
      auto thread_accessor = skiplist.access();
      int start_value = 10000 + t * 15000;  // Different ranges for each thread
      int current_value = start_value;
      while (!stop_operations.load(std::memory_order_acquire)) {
        bool removed = thread_accessor.remove(current_value);
        if (removed) {
          ++deleted_count;
        }
        current_value += 2;  // Skip every other element
        if (current_value > 40000) {
          current_value = start_value;  // Wrap around
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  // Insertion threads - insert new elements outside the range
  std::vector<std::jthread> insertion_threads;
  for (int t = 0; t < 2; ++t) {
    insertion_threads.emplace_back([&skiplist, &stop_operations, &inserted_count, t]() {
      auto thread_accessor = skiplist.access();
      int new_value = 100000 + t * 10000;
      while (!stop_operations.load(std::memory_order_acquire)) {
        auto res = thread_accessor.insert(new_value++);
        if (res.second) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  // Re-insertion threads - re-insert elements that were deleted
  std::vector<std::jthread> reinsertion_threads;
  for (int t = 0; t < 2; ++t) {
    reinsertion_threads.emplace_back([&skiplist, &stop_operations, &reinserted_count, t]() {
      auto thread_accessor = skiplist.access();
      int start_value = 10000 + t * 15000;
      int current_value = start_value;
      while (!stop_operations.load(std::memory_order_acquire)) {
        auto res = thread_accessor.insert(current_value);
        if (res.second) {
          ++reinserted_count;
        }
        current_value += 3;  // Different pattern from deletions
        if (current_value > 40000) {
          current_value = start_value;  // Wrap around
        }
        std::this_thread::sleep_for(std::chrono::microseconds(2));
      }
    });
  }

  // Let the operations run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Process chunks in parallel while operations are happening
  std::atomic<int> total_processed{0};
  std::vector<std::jthread> processing_threads;
  std::vector<int> collected_elements;

  for (size_t i = 0; i < chunks.size(); ++i) {
    processing_threads.emplace_back([&chunks, i, &total_processed, &collected_elements]() {
      int local_count = 0;
      std::vector<int> local_elements;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        local_elements.push_back(*it);
        // Verify element is within the specified range
        ASSERT_GE(*it, 10000);
        ASSERT_LE(*it, 40000);
        if (local_count % 500 == 0) {
          // Make sure the iterator and operations are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      EXPECT_GT(local_count, 0);
      EXPECT_LT(local_count, 30000 / 8 * 3);
      total_processed.fetch_add(local_count, std::memory_order_relaxed);

      // Thread-safe collection of elements
      static std::mutex mutex;
      std::lock_guard<std::mutex> lock(mutex);
      collected_elements.insert(collected_elements.end(), local_elements.begin(), local_elements.end());
    });
  }

  // Let processing run for a while
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Stop all operations
  stop_operations.store(true, std::memory_order_release);

  deletion_threads.clear();
  insertion_threads.clear();
  reinsertion_threads.clear();
  processing_threads.clear();

  std::cout << "Deleted elements: " << deleted_count.load() << std::endl;
  std::cout << "Inserted new elements: " << inserted_count.load() << std::endl;
  std::cout << "Re-inserted elements: " << reinserted_count.load() << std::endl;
  std::cout << "Elements processed in range: " << total_processed.load() << std::endl;
  std::cout << "Unique elements collected: " << collected_elements.size() << std::endl;

  // Verify we processed elements in the range (exact count may vary due to concurrent operations)
  ASSERT_GE(total_processed.load(), 10000);  // At least some elements should be processed
  ASSERT_LE(total_processed.load(), 30001);  // At most all elements in range

  // Verify all collected elements are in the correct range
  for (const auto &item : collected_elements) {
    ASSERT_GE(item, 10000);
    ASSERT_LE(item, 40000);
  }
}
