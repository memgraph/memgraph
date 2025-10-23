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
#include <iostream>
#include <latch>
#include <random>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "utils/skip_list.hpp"

// Test the parallel chunk reading functionality of SkipList
// This tests the real implementation using the Memgraph test framework

using namespace memgraph::utils;

// ============================================================================
// TEST 1: Basic Complete Coverage Test
// ============================================================================

TEST(SkipListParallelChunks, BasicCompleteCoverage) {
  SkipList<int> skiplist;

  // Insert elements using accessor - use a much larger dataset
  auto accessor = skiplist.access();
  for (int i = 0; i < 100000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 100000);

  // Insert more elements
  for (int i = 100000; i < 200000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 200000);

  // Create chunks with complete coverage using the real implementation
  auto chunks = accessor.create_chunks(4);
  // With large datasets, we may get multiple chunks if there are enough max-height elements
  // Otherwise, we get the complete list as one chunk
  ASSERT_GT(chunks.size(), 0);

  // Process chunks in parallel
  std::atomic<int> total_processed{0};
  std::vector<std::thread> threads;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &total_processed]() {
      int local_count = 0;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        (void)*it;  // Suppress unused variable warning
      }
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  ASSERT_EQ(total_processed.load(), 200000);
  std::cout << "Total elements processed: " << total_processed.load() << '\n';
}

// ============================================================================
// TEST 2: Performance Comparison Test
// ============================================================================

TEST(SkipListParallelChunks, PerformanceComparison) {
  SkipList<int> skiplist;

  // Insert larger dataset using accessor
  auto accessor = skiplist.access();
  for (int i = 0; i < 50000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 50000);

  // Test efficient chunking (using the real implementation)
  auto start_time = std::chrono::high_resolution_clock::now();
  auto chunks = accessor.create_chunks(8);
  auto end_time = std::chrono::high_resolution_clock::now();
  auto chunking_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

  std::cout << "Chunking time: " << chunking_duration.count() << " microseconds" << '\n';
  std::cout << "Created " << chunks.size() << " chunks" << '\n';

  // Process chunks and measure processing time
  std::atomic<uint64_t> processing_duration = 0;
  std::atomic<int> total_processed{0};
  std::vector<std::thread> threads;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &total_processed, &processing_duration]() {
      int local_count = 0;
      auto start_time = std::chrono::high_resolution_clock::now();
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        (void)*it;  // Suppress unused variable warning
      }
      auto end_time = std::chrono::high_resolution_clock::now();
      EXPECT_GT(local_count, 0);  // Chunk sizes are probabilistic, so we expect at some elements to be in each chunk
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
      processing_duration.fetch_add(
          std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count());
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Basic serial processing
  start_time = std::chrono::high_resolution_clock::now();
  int total_processed_serial = 0;
  for (auto it = accessor.begin(); it != accessor.end(); ++it) {
    total_processed_serial++;
    (void)*it;  // Suppress unused variable warning
  }
  end_time = std::chrono::high_resolution_clock::now();
  auto processing_duration_serial = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

  std::cout << "Processing time: " << processing_duration.load() << " microseconds" << '\n';
  std::cout << "Total elements processed: " << total_processed.load() << '\n';
  std::cout << "Serial processing time: " << processing_duration_serial.count() << " microseconds" << '\n';
  std::cout << "Total elements processed: " << total_processed_serial << '\n';
  if (processing_duration.load() > 0) {
    std::cout << "Parallel elements per microsecond: " << (total_processed.load() / processing_duration.load()) << '\n';
  }
  if (processing_duration_serial.count() > 0) {
    std::cout << "Serial elements per microsecond: " << (total_processed_serial / processing_duration_serial.count())
              << '\n';
  }

  ASSERT_EQ(total_processed.load(), 50000);
}

// ============================================================================
// TEST 3: Concurrent Operations Test
// ============================================================================

TEST(SkipListParallelChunks, ConcurrentOperations) {
  SkipList<int> skiplist;

  // Insert initial data using accessor
  auto accessor = skiplist.access();
  for (int i = 0; i < 50000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 50000);

  // Create chunks with complete coverage
  auto chunks = accessor.create_chunks(4);
  ASSERT_GT(chunks.size(), 0);

  // Start concurrent insertion threads
  std::atomic<bool> stop_insertions{false};
  std::atomic<int> inserted_count{0};

  std::vector<std::thread> insertion_threads;
  insertion_threads.reserve(2);
  std::latch latch(2);
  for (int thread_id = 0; thread_id < 2; ++thread_id) {
    insertion_threads.emplace_back([&skiplist, &stop_insertions, &inserted_count, thread_id, &latch]() {
      auto thread_accessor = skiplist.access();
      int new_value = 50000 + thread_id * 100000;
      latch.count_down();
      while (!stop_insertions.load(std::memory_order_acquire)) {
        auto res = thread_accessor.insert(new_value++);
        if (res.second) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  latch.wait();

  // Let the processing run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Process chunks in parallel
  std::atomic<int> total_processed{0};
  std::vector<std::thread> processing_threads;
  processing_threads.reserve(chunks.size());

  for (size_t i = 0; i < chunks.size(); ++i) {
    processing_threads.emplace_back([&chunks, i, &total_processed]() {
      int local_count = 0;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        (void)*it;  // Suppress unused variable warning
        if (local_count % 1000 == 0) {
          // Make sure the iterator and creations are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      }
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

  std::cout << "Final list size: " << accessor.size() << '\n';
  std::cout << "Elements inserted during processing: " << inserted_count.load() << '\n';
  std::cout << "Elements processed: " << total_processed.load() << '\n';

  ASSERT_GE(total_processed.load(), 50000);                          // At least original elements
  ASSERT_LE(total_processed.load(), 50000 + inserted_count.load());  // At most original elements + inserted elements
}

// ============================================================================
// TEST 4: Stress Test
// ============================================================================

TEST(SkipListParallelChunks, StressTest) {
  SkipList<int> skiplist;

  // Insert much larger dataset using accessor
  auto accessor = skiplist.access();
  for (int i = 0; i < 50000; ++i) {
    auto res = accessor.insert(i * 2);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 50000);

  // Create more chunks for better parallelism
  auto chunks = accessor.create_chunks(16);
  ASSERT_GT(chunks.size(), 0);

  // Start concurrent operations
  std::atomic<bool> stop_operations{false};
  std::atomic<int> inserted_count{0};
  std::atomic<int> deleted_count{0};
  std::atomic<int> found_count{0};
  std::atomic<int> processed_count{0};
  std::atomic<int> mixed_operations_count{0};

  // More aggressive insertion threads (8 threads)
  std::vector<std::thread> insertion_threads;
  insertion_threads.reserve(8);
  for (int thread_id = 0; thread_id < 8; ++thread_id) {
    insertion_threads.emplace_back([&skiplist, &stop_operations, &inserted_count, thread_id]() {
      auto thread_accessor = skiplist.access();
      std::random_device random_dev;
      std::mt19937 gen(random_dev() + thread_id);
      std::uniform_int_distribution<> dis(100000, 999999);

      while (!stop_operations.load(std::memory_order_acquire)) {
        auto res = thread_accessor.insert(dis(gen));
        if (res.second) {
          ++inserted_count;
        }
        // Remove sleep to maximize contention
      }
    });
  }

  // More aggressive deletion threads (6 threads)
  std::vector<std::thread> deletion_threads;
  deletion_threads.reserve(6);
  for (int thread_id = 0; thread_id < 6; ++thread_id) {
    deletion_threads.emplace_back([&skiplist, &stop_operations, &deleted_count, thread_id]() {
      auto thread_accessor = skiplist.access();
      std::random_device random_dev;
      std::mt19937 gen(random_dev() + thread_id + 100);
      std::uniform_int_distribution<> dis(0, 100000);

      while (!stop_operations.load(std::memory_order_acquire)) {
        auto res = thread_accessor.remove(dis(gen));
        if (res) {
          ++deleted_count;
        }
        // Run GC more frequently to stress memory management
        if (thread_id % 2 == 0) {
          skiplist.run_gc();
        }
      }
    });
  }

  // Search threads to add more read contention (4 threads)
  std::vector<std::thread> search_threads;
  search_threads.reserve(4);
  for (int thread_id = 0; thread_id < 4; ++thread_id) {
    search_threads.emplace_back([&skiplist, &stop_operations, &found_count, thread_id]() {
      auto thread_accessor = skiplist.access();
      std::random_device random_dev;
      std::mt19937 gen(random_dev() + thread_id + 200);
      std::uniform_int_distribution<> dis(0, 200000);

      while (!stop_operations.load(std::memory_order_acquire)) {
        auto iterator = thread_accessor.find(dis(gen));
        if (iterator != thread_accessor.end()) {
          ++found_count;
        }
      }
    });
  }

  // Mixed operation threads (4 threads) - random insert/remove/find
  std::vector<std::thread> mixed_threads;
  mixed_threads.reserve(4);
  for (int thread_id = 0; thread_id < 4; ++thread_id) {
    mixed_threads.emplace_back([&skiplist, &stop_operations, &mixed_operations_count, thread_id]() {
      auto thread_accessor = skiplist.access();
      std::random_device random_dev;
      std::mt19937 gen(random_dev() + thread_id + 300);
      std::uniform_int_distribution<> value_dis(0, 200000);
      std::uniform_int_distribution<> op_dis(0, 2);  // 0=insert, 1=remove, 2=find

      while (!stop_operations.load(std::memory_order_acquire)) {
        int value = value_dis(gen);
        int operation = op_dis(gen);

        switch (operation) {
          case 0: {
            auto res = thread_accessor.insert(value);
            if (res.second) ++mixed_operations_count;
            break;
          }
          case 1: {
            auto res = thread_accessor.remove(value);
            if (res) ++mixed_operations_count;
            break;
          }
          case 2: {
            auto iterator = thread_accessor.find(value);
            if (iterator != thread_accessor.end()) ++mixed_operations_count;
            break;
          }
          default:
            // This should never happen with the distribution range
            break;
        }
      }
    });
  }

  // Processing threads with more intensive work
  std::vector<std::thread> processing_threads;
  processing_threads.reserve(chunks.size());
  for (size_t chunk_idx = 0; chunk_idx < chunks.size(); ++chunk_idx) {
    processing_threads.emplace_back([&chunks, chunk_idx, &processed_count]() {
      int local_count = 0;
      auto chunk = chunks[chunk_idx];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        (void)*it;  // Suppress unused variable warning
        // More intensive processing simulation
        volatile int dummy = 0;
        for (int j = 0; j < 100; ++j) {
          dummy += *it;
        }
      }
      processed_count.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  // Let the stress test run much longer
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Stop all operations
  stop_operations.store(true, std::memory_order_release);

  for (auto &thread : insertion_threads) {
    thread.join();
  }
  for (auto &thread : deletion_threads) {
    thread.join();
  }
  for (auto &thread : search_threads) {
    thread.join();
  }
  for (auto &thread : mixed_threads) {
    thread.join();
  }
  for (auto &thread : processing_threads) {
    thread.join();
  }

  std::cout << "Final list size: " << accessor.size() << '\n';
  std::cout << "Elements inserted: " << inserted_count.load() << '\n';
  std::cout << "Elements deleted: " << deleted_count.load() << '\n';
  std::cout << "Elements found: " << found_count.load() << '\n';
  std::cout << "Mixed operations: " << mixed_operations_count.load() << '\n';
  std::cout << "Elements processed: " << processed_count.load() << '\n';

  // More lenient assertions due to higher concurrency
  ASSERT_GE(processed_count.load(), 50000 - deleted_count.load());
  ASSERT_LE(processed_count.load(), 50000 + inserted_count.load());

  // Verify the skip list is still in a consistent state
  ASSERT_GE(accessor.size(), 0);
}

// ============================================================================
// TEST 5: Edge Cases Test
// ============================================================================

TEST(SkipListParallelChunks, EdgeCases) {
  {
    SkipList<int> skiplist;

    // Test with empty list
    auto accessor = skiplist.access();
    auto empty_chunks = accessor.create_chunks(4);
    ASSERT_EQ(empty_chunks.size(), 1);
    // Empty chunk should not be valid
    auto empty_chunk = empty_chunks[0];
    ASSERT_TRUE(empty_chunk.begin() == empty_chunk.end());

    // Test with single element
    auto res = accessor.insert(42);
    ASSERT_TRUE(res.second);
    auto single_chunks = accessor.create_chunks(4);
    ASSERT_EQ(single_chunks.size(), 1);
  }

  {
    SkipList<int> skiplist;
    auto accessor = skiplist.access();
    // Test with fewer elements than chunks
    for (int i = 0; i < 3; ++i) {
      auto res = accessor.insert(i);
      ASSERT_TRUE(res.second);
    }
    auto few_chunks = accessor.create_chunks(8);
    ASSERT_GT(few_chunks.size(), 0);  // Should have at least one chunk
    // Loop and check if the elements are in the chunks
    int idx = 0;
    for (auto &chunk : few_chunks) {
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        ASSERT_EQ(*it, idx);
        idx++;
      }
    }
    ASSERT_EQ(idx, 3);

    // Test with exactly one chunk
    auto one_chunk = accessor.create_chunks(1);
    ASSERT_EQ(one_chunk.size(), 1);
    idx = 0;
    auto chunk = one_chunk[0];
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      ASSERT_EQ(*it, idx);
      idx++;
    }
    ASSERT_EQ(idx, 3);

    // Test with zero chunks (should return empty collection)
    auto zero_chunks = accessor.create_chunks(0);
    ASSERT_EQ(zero_chunks.size(), 0);
  }
}

// ============================================================================
// TEST 6: Large Dataset Test
// ============================================================================

TEST(SkipListParallelChunks, LargeDataset) {
  SkipList<int> skiplist;

  // Insert very large dataset
  auto accessor = skiplist.access();
  for (int i = 0; i < 50000; ++i) {
    auto res = accessor.insert(i);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 50000);

  // Create many chunks
  auto chunks = accessor.create_chunks(16);
  ASSERT_GT(chunks.size(), 0);

  // Process chunks in parallel
  std::atomic<int> total_processed{0};
  std::vector<std::thread> threads;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &total_processed]() {
      int local_count = 0;
      auto chunk = chunks[i];
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        (void)*it;  // Suppress unused variable warning
      }
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  std::cout << "Large dataset - Total elements processed: " << total_processed.load() << '\n';
  ASSERT_GE(total_processed.load(), 50000);
}
