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
#include "utils/timer.hpp"

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
      for (const auto &item : chunks[i]) {
        local_count++;
        (void)item;  // Suppress unused variable warning
      }
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  ASSERT_EQ(total_processed.load(), 200000);
  std::cout << "Total elements processed: " << total_processed.load() << std::endl;
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

  std::cout << "Chunking time: " << chunking_duration.count() << " microseconds" << std::endl;
  std::cout << "Created " << chunks.size() << " chunks" << std::endl;

  // Process chunks and measure processing time
  std::atomic<uint64_t> processing_duration = 0;
  std::atomic<int> total_processed{0};
  std::vector<std::thread> threads;

  for (size_t i = 0; i < chunks.size(); ++i) {
    threads.emplace_back([&chunks, i, &total_processed, &processing_duration]() {
      int local_count = 0;
      auto start_time = std::chrono::high_resolution_clock::now();
      for (const auto &item : chunks[i]) {
        local_count++;
        (void)item;  // Suppress unused variable warning
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
  for (const auto &item : accessor) {
    total_processed_serial++;
    (void)item;  // Suppress unused variable warning
  }
  end_time = std::chrono::high_resolution_clock::now();
  auto processing_duration_serial = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

  std::cout << "Processing time: " << processing_duration.load() << " microseconds" << std::endl;
  std::cout << "Total elements processed: " << total_processed.load() << std::endl;
  std::cout << "Serial processing time: " << processing_duration_serial.count() << " microseconds" << std::endl;
  std::cout << "Total elements processed: " << total_processed_serial << std::endl;
  if (processing_duration.load() > 0) {
    std::cout << "Parallel elements per microsecond: " << (total_processed.load() / processing_duration.load())
              << std::endl;
  }
  if (processing_duration_serial.count() > 0) {
    std::cout << "Serial elements per microsecond: " << (total_processed_serial / processing_duration_serial.count())
              << std::endl;
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
  for (int t = 0; t < 2; ++t) {
    insertion_threads.emplace_back([&skiplist, &stop_insertions, &inserted_count, t, &latch]() {
      auto thread_accessor = skiplist.access();
      int new_value = 50000 + t * 100000;
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
      for (const auto &item : chunks[i]) {
        local_count++;
        (void)item;  // Suppress unused variable warning
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

  std::cout << "Final list size: " << accessor.size() << std::endl;
  std::cout << "Elements inserted during processing: " << inserted_count.load() << std::endl;
  std::cout << "Elements processed: " << total_processed.load() << std::endl;

  ASSERT_GE(total_processed.load(), 50000);                          // At least original elements
  ASSERT_LE(total_processed.load(), 50000 + inserted_count.load());  // At most original elements + inserted elements
}

// ============================================================================
// TEST 4: Stress Test
// ============================================================================

TEST(SkipListParallelChunks, StressTest) {
  SkipList<int> skiplist;

  // Insert large dataset using accessor
  auto accessor = skiplist.access();
  for (int i = 0; i < 5000; ++i) {
    auto res = accessor.insert(i * 2);
    ASSERT_TRUE(res.second);
  }

  ASSERT_EQ(accessor.size(), 5000);

  // Create chunks
  auto chunks = accessor.create_chunks(8);
  ASSERT_GT(chunks.size(), 0);

  // Start concurrent operations
  std::atomic<bool> stop_operations{false};
  std::atomic<int> inserted_count{0};
  std::atomic<int> processed_count{0};

  // Insertion threads
  std::vector<std::thread> insertion_threads;
  insertion_threads.reserve(2);
  for (int t = 0; t < 2; ++t) {
    insertion_threads.emplace_back([&skiplist, &stop_operations, &inserted_count, t]() {
      auto thread_accessor = skiplist.access();
      int new_value = t * 1000;
      while (!stop_operations.load(std::memory_order_acquire)) {
        auto res = thread_accessor.insert(new_value++);
        if (res.second) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
    });
  }

  // Deletion threads
  std::vector<std::thread> deletion_threads;
  deletion_threads.reserve(2);
  std::atomic<int> deleted_count{0};
  for (int t = 0; t < 2; ++t) {
    deletion_threads.emplace_back([&skiplist, &stop_operations, &deleted_count]() {
      auto thread_accessor = skiplist.access();
      int new_value = 0;
      while (!stop_operations.load(std::memory_order_acquire)) {
        auto res = thread_accessor.remove(new_value++);
        if (res) {
          ++deleted_count;
        }
        skiplist.run_gc();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
    });
  }

  // Processing threads
  std::vector<std::thread> processing_threads;
  processing_threads.reserve(chunks.size());
  for (size_t i = 0; i < chunks.size(); ++i) {
    processing_threads.emplace_back([&chunks, i, &processed_count]() {
      int local_count = 0;
      for (const auto &item : chunks[i]) {
        local_count++;
        (void)item;  // Suppress unused variable warning
        // Simulate some processing time
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
      processed_count.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  // Let the stress test run
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Stop all operations
  stop_operations.store(true, std::memory_order_release);
  for (auto &thread : insertion_threads) {
    thread.join();
  }
  for (auto &thread : deletion_threads) {
    thread.join();
  }
  for (auto &thread : processing_threads) {
    thread.join();
  }

  std::cout << "Final list size: " << accessor.size() << std::endl;
  std::cout << "Elements inserted: " << inserted_count.load() << std::endl;
  std::cout << "Elements deleted: " << deleted_count.load() << std::endl;
  std::cout << "Elements processed: " << processed_count.load() << std::endl;

  ASSERT_GE(processed_count.load(), 5000 - deleted_count.load());
  ASSERT_LE(processed_count.load(), 5000 + inserted_count.load());
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
    ASSERT_EQ(empty_chunks[0].begin(), empty_chunks[0].end());

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
    int i = 0;
    for (const auto &chunk : few_chunks) {
      for (const auto &item : chunk) {
        ASSERT_EQ(item, i);
        i++;
      }
    }
    ASSERT_EQ(i, 3);

    // Test with exactly one chunk
    auto one_chunk = accessor.create_chunks(1);
    ASSERT_EQ(one_chunk.size(), 1);
    i = 0;
    for (const auto &item : one_chunk[0]) {
      ASSERT_EQ(item, i);
      i++;
    }
    ASSERT_EQ(i, 3);

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
      for (const auto &item : chunks[i]) {
        local_count++;
        (void)item;  // Suppress unused variable warning
      }
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  std::cout << "Large dataset - Total elements processed: " << total_processed.load() << std::endl;
  ASSERT_GE(total_processed.load(), 50000);
}
