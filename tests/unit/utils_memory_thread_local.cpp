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
#include <memory>
#include <thread>
#include <vector>

// #include "query/typed_value.hpp"
#include "utils/memory.hpp"

using namespace memgraph::utils;

TEST(ThreadLocalMemoryResourceTest, BasicAllocation) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);

  // Allocate some memory
  void *ptr1 = resource.allocate(64, 8);
  void *ptr2 = resource.allocate(128, 16);
  void *ptr3 = resource.allocate(256, 32);

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);

  // Verify pointers are different
  EXPECT_NE(ptr1, ptr2);
  EXPECT_NE(ptr1, ptr3);
  EXPECT_NE(ptr2, ptr3);

  // Verify alignment
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 8, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 16, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 32, 0);
}

TEST(ThreadLocalMemoryResourceTest, ThreadIsolation) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);
  std::vector<void *> thread_allocations;
  std::mutex allocations_mutex;

  constexpr size_t num_threads = 4;
  constexpr size_t allocations_per_thread = 10;

  // Launch threads that allocate memory with different thread IDs
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &thread_allocations, &allocations_mutex, i]() {
      // Initialize this thread with a unique ID
      resource.Initialize(static_cast<uint16_t>(i + 1));

      std::vector<void *> local_allocations;
      for (size_t j = 0; j < allocations_per_thread; ++j) {
        size_t size = 8 + (j % 64);
        size_t alignment = 8 << (j % 4);  // 8, 16, 32, 64

        void *ptr = resource.allocate(size, alignment);
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);

        local_allocations.push_back(ptr);
      }

      // Store allocations for verification
      {
        std::lock_guard<std::mutex> lock(allocations_mutex);
        thread_allocations.insert(thread_allocations.end(), local_allocations.begin(), local_allocations.end());
      }

      // Reset thread state
      ThreadLocalMemoryResource::ResetThread();
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Verify all allocations are unique (each thread gets its own memory resource)
  EXPECT_EQ(thread_allocations.size(), num_threads * allocations_per_thread);
  std::sort(thread_allocations.begin(), thread_allocations.end());
  auto it = std::unique(thread_allocations.begin(), thread_allocations.end());
  EXPECT_EQ(it, thread_allocations.end());
}

TEST(ThreadLocalMemoryResourceTest, SafeWrapper) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);

  // Test SafeWrapper RAII mechanism
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 42);

    // Allocate memory while wrapper is active
    void *ptr1 = resource.allocate(64, 8);
    void *ptr2 = resource.allocate(128, 16);

    EXPECT_NE(ptr1, nullptr);
    EXPECT_NE(ptr2, nullptr);
    EXPECT_NE(ptr1, ptr2);

    // Verify alignment
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 8, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 16, 0);
  }

  // After wrapper goes out of scope, thread should be reset
  // This should still work because it falls back to default upstream (thread 0)
  void *ptr3 = resource.allocate(32, 8);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 8, 0);
}

TEST(ThreadLocalMemoryResourceTest, ConcurrentAccess) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);
  constexpr int num_threads = 4;
  constexpr int operations_per_thread = 50;

  std::vector<std::thread> threads;
  std::atomic<int> total_allocations{0};

  // Start threads that allocate memory concurrently
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &total_allocations, operations_per_thread, i]() {
      // Each thread uses a different ID
      resource.Initialize(static_cast<uint16_t>(i + 1));

      for (int j = 0; j < operations_per_thread; ++j) {
        // Allocate some memory
        void *ptr = resource.allocate(64, 8);
        EXPECT_NE(ptr, nullptr);
        total_allocations.fetch_add(1);

        // Simulate some work
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }

      // Reset thread state
      ThreadLocalMemoryResource::ResetThread();
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(total_allocations.load(), num_threads * operations_per_thread);
}

TEST(ThreadLocalMemoryResourceTest, DefaultUpstreamFallback) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);

  // Allocate without initializing any thread ID - should use default upstream (thread 0)
  void *ptr1 = resource.allocate(64, 8);
  void *ptr2 = resource.allocate(128, 16);

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr1, ptr2);

  // Verify alignment
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 8, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 16, 0);
}

TEST(ThreadLocalMemoryResourceTest, MultipleInitialization) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);

  // Initialize with different IDs multiple times
  resource.Initialize(1);
  void *ptr1 = resource.allocate(64, 8);
  EXPECT_NE(ptr1, nullptr);

  resource.Initialize(2);
  void *ptr2 = resource.allocate(64, 8);
  EXPECT_NE(ptr2, nullptr);

  resource.Initialize(1);  // Re-initialize with same ID
  void *ptr3 = resource.allocate(64, 8);
  EXPECT_NE(ptr3, nullptr);

  // All pointers should be valid
  EXPECT_NE(ptr1, ptr2);
  EXPECT_NE(ptr1, ptr3);
  EXPECT_NE(ptr2, ptr3);
}

TEST(ThreadLocalMemoryResourceTest, ThreadResetAndReinitialize) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);

  // Initialize and allocate
  resource.Initialize(5);
  void *ptr1 = resource.allocate(64, 8);
  EXPECT_NE(ptr1, nullptr);

  // Reset thread
  ThreadLocalMemoryResource::ResetThread();

  // Should fall back to default upstream
  void *ptr2 = resource.allocate(64, 8);
  EXPECT_NE(ptr2, nullptr);

  // Re-initialize with different ID
  resource.Initialize(10);
  void *ptr3 = resource.allocate(64, 8);
  EXPECT_NE(ptr3, nullptr);

  // All should be valid
  EXPECT_NE(ptr1, ptr2);
  EXPECT_NE(ptr1, ptr3);
  EXPECT_NE(ptr2, ptr3);
}

TEST(ThreadLocalMemoryResourceTest, StressTest) {
  auto resource_factory = []() -> std::unique_ptr<std::pmr::memory_resource> {
    return std::make_unique<MonotonicBufferResource>(512);
  };

  ThreadLocalMemoryResource resource(resource_factory);
  constexpr int num_threads = 8;
  constexpr int operations_per_thread = 1000;

  std::vector<std::thread> threads;
  std::atomic<int> successful_allocations{0};

  // Start threads that perform many allocations
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &successful_allocations, operations_per_thread, i]() {
      resource.Initialize(static_cast<uint16_t>(i + 1));

      for (int j = 0; j < operations_per_thread; ++j) {
        size_t size = 1 + (j % 128);      // Sizes from 1 to 128 bytes
        size_t alignment = 1 << (j % 7);  // Alignments: 1, 2, 4, 8, 16, 32, 64

        try {
          void *ptr = resource.allocate(size, alignment);
          EXPECT_NE(ptr, nullptr);
          EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);
          successful_allocations.fetch_add(1);
        } catch (const std::bad_alloc &) {
          // This is acceptable in stress tests
        }
      }

      ThreadLocalMemoryResource::ResetThread();
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Should have successfully allocated a significant number of blocks
  EXPECT_GT(successful_allocations.load(), num_threads * operations_per_thread / 2);
}

TEST(ThreadLocalMemoryResourceTest, DifferentResourceTypes) {
  // Test with different types of upstream resources
  auto monotonic_factory = []() -> ThreadLocalMemoryResource::memory_t {
    return std::make_unique<MonotonicBufferResource>(1024);
  };

  auto pool_factory = []() -> ThreadLocalMemoryResource::memory_t { return std::make_unique<PoolResource>(10); };

  // Test with MonotonicBufferResource
  {
    ThreadLocalMemoryResource resource(monotonic_factory);
    resource.Initialize(1);

    void *ptr1 = resource.allocate(64, 8);
    void *ptr2 = resource.allocate(128, 16);

    EXPECT_NE(ptr1, nullptr);
    EXPECT_NE(ptr2, nullptr);
    EXPECT_NE(ptr1, ptr2);
  }

  // Test with PoolResource
  {
    ThreadLocalMemoryResource resource(pool_factory);
    resource.Initialize(2);

    void *ptr1 = resource.allocate(64, 8);
    void *ptr2 = resource.allocate(128, 16);

    EXPECT_NE(ptr1, nullptr);
    EXPECT_NE(ptr2, nullptr);
    EXPECT_NE(ptr1, ptr2);
  }
}

TEST(ThreadLocalMemoryResourceTest, IsEqual) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource1(resource_factory);
  ThreadLocalMemoryResource resource2(resource_factory);

  // Same resource should be equal to itself
  EXPECT_TRUE(resource1.is_equal(resource1));

  // Different resource instances should not be equal
  EXPECT_FALSE(resource1.is_equal(resource2));
}

TEST(ThreadLocalMemoryResourceTest, EdgeCaseAlignment) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);
  resource.Initialize(1);

  // Test various alignment requirements
  void *ptr1 = resource.allocate(1, 1);    // Minimal alignment
  void *ptr2 = resource.allocate(8, 8);    // Standard alignment
  void *ptr3 = resource.allocate(16, 16);  // Larger alignment
  void *ptr4 = resource.allocate(32, 32);  // Even larger alignment
  void *ptr5 = resource.allocate(64, 64);  // Maximum common alignment

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);
  EXPECT_NE(ptr5, nullptr);

  // Verify alignments
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 1, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 8, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 16, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr4) % 32, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr5) % 64, 0);
}

/*TEST(ThreadLocalMemoryResourceTest, TypedValueMoveBetweenThreads) {
  auto resource_factory = []() {
    return std::make_unique<MonotonicBufferResource>(1024);
  };

  ThreadLocalMemoryResource resource(resource_factory);

  // Create a TypedValue with complex data in thread 1
  memgraph::query::TypedValue original_value;
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 1);

    // Create an allocator for this thread
    memgraph::utils::Allocator<memgraph::query::TypedValue> alloc(&resource);

    // Create a list with multiple elements
    memgraph::query::TypedValue::TVector list_data(alloc);
    list_data.emplace_back(42, alloc);
    list_data.emplace_back("nested_string", alloc);
    list_data.emplace_back(3.14, alloc);

    original_value = memgraph::query::TypedValue(std::move(list_data), alloc);

    // Verify the original value was created with thread 1's memory resource
    EXPECT_EQ(original_value.get_allocator().resource(), &resource);
  }

  // Move the TypedValue to thread 2
  memgraph::query::TypedValue moved_value;
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 2);

    // Move the value from thread 1 to thread 2
    moved_value = std::move(original_value);

    // Verify the moved value now uses thread 2's memory resource
    EXPECT_EQ(moved_value.get_allocator().resource(), &resource);

    // Verify the original value is now in a moved-from state (should be Null)
    EXPECT_EQ(original_value.type(), memgraph::query::TypedValue::Type::Null);

    // Verify the moved value contains the correct data
    EXPECT_EQ(moved_value.type(), memgraph::query::TypedValue::Type::List);
    const auto &moved_list = moved_value.ValueList();
    EXPECT_EQ(moved_list.size(), 3);
    EXPECT_EQ(moved_list[0].ValueInt(), 42);
    EXPECT_EQ(moved_list[1].ValueString(), "nested_string");
    EXPECT_EQ(moved_list[2].ValueDouble(), 3.14);
  }

  // Move back to thread 1 to verify the data is still intact
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 1);

    memgraph::query::TypedValue back_to_thread1 = std::move(moved_value);

    // Verify it's now using thread 1's memory resource again
    EXPECT_EQ(back_to_thread1.get_allocator().resource(), &resource);

    // Verify the data is still intact
    EXPECT_EQ(back_to_thread1.type(), memgraph::query::TypedValue::Type::List);
    const auto &final_list = back_to_thread1.ValueList();
    EXPECT_EQ(final_list.size(), 3);
    EXPECT_EQ(final_list[0].ValueInt(), 42);
    EXPECT_EQ(final_list[1].ValueString(), "nested_string");
    EXPECT_EQ(final_list[2].ValueDouble(), 3.14);

    // Verify the intermediate moved value is now in moved-from state
    EXPECT_EQ(moved_value.type(), memgraph::query::TypedValue::Type::Null);
  }
}

TEST(ThreadLocalMemoryResourceTest, TypedValueMoveWithDifferentAllocators) {
  auto resource_factory = []() {
    return std::make_unique<MonotonicBufferResource>(1024);
  };

  ThreadLocalMemoryResource resource(resource_factory);

  // Create TypedValue in thread 1
  memgraph::query::TypedValue value1;
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 1);

    // Create an allocator for this thread
    memgraph::utils::Allocator<memgraph::query::TypedValue> alloc(&resource);

    // Create a string value
    value1 = memgraph::query::TypedValue("thread1_string", alloc);
    EXPECT_EQ(value1.get_allocator().resource(), &resource);
  }

  // Move to thread 2 (different allocator)
  memgraph::query::TypedValue value2;
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 2);

    // Move from thread 1 to thread 2
    value2 = std::move(value1);

    // Since allocators are different, this should trigger a copy, not a move
    // The moved-from value should be reset to Null
    EXPECT_EQ(value1.type(), memgraph::query::TypedValue::Type::Null);

    // The new value should use thread 2's allocator
    EXPECT_EQ(value2.get_allocator().resource(), &resource);
    EXPECT_EQ(value2.type(), memgraph::query::TypedValue::Type::String);
    EXPECT_EQ(value2.ValueString(), "thread1_string");
  }

  // Move within the same thread (same allocator) - should be a true move
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 2);

    memgraph::query::TypedValue value3 = std::move(value2);

    // This should be a true move since allocators are the same
    EXPECT_EQ(value2.type(), memgraph::query::TypedValue::Type::Null);
    EXPECT_EQ(value3.type(), memgraph::query::TypedValue::Type::String);
    EXPECT_EQ(value3.ValueString(), "thread1_string");
    EXPECT_EQ(value3.get_allocator().resource(), &resource);
  }
}*/

TEST(ThreadLocalMemoryResourceTest, MoveSemanticsBetweenThreads) {
  auto resource_factory = []() { return std::make_unique<MonotonicBufferResource>(1024); };

  ThreadLocalMemoryResource resource(resource_factory);

  // Create a vector with complex data in thread 1
  std::vector<int> original_data;
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 1);

    // Create a vector using the thread's memory resource
    std::pmr::vector<int> data(&resource);
    data.resize(1000);
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] = static_cast<int>(i);
    }

    // Move the data to a regular vector (this will copy the data)
    original_data = std::vector<int>(data.begin(), data.end());

    // Verify the data was created correctly
    EXPECT_EQ(original_data.size(), 1000);
    EXPECT_EQ(original_data[0], 0);
    EXPECT_EQ(original_data[999], 999);
  }

  // Move the data to thread 2
  std::vector<int> moved_data;
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 2);

    // Move the data from thread 1 to thread 2
    moved_data = std::move(original_data);

    // Verify the original data is now empty (moved-from state)
    EXPECT_EQ(original_data.size(), 0);

    // Verify the moved data contains the correct values
    EXPECT_EQ(moved_data.size(), 1000);
    EXPECT_EQ(moved_data[0], 0);
    EXPECT_EQ(moved_data[999], 999);

    // Create a new vector in thread 2 using the thread's memory resource
    std::pmr::vector<int> thread2_data(&resource);
    thread2_data.resize(500);
    for (size_t i = 0; i < thread2_data.size(); ++i) {
      thread2_data[i] = static_cast<int>(i * 2);
    }

    // Verify thread 2's data
    EXPECT_EQ(thread2_data.size(), 500);
    EXPECT_EQ(thread2_data[0], 0);
    EXPECT_EQ(thread2_data[499], 998);
  }

  // Move back to thread 1 to verify the data is still intact
  {
    ThreadLocalMemoryResource::SafeWrapper wrapper(&resource, 1);

    std::vector<int> back_to_thread1 = std::move(moved_data);

    // Verify the data is still intact
    EXPECT_EQ(back_to_thread1.size(), 1000);
    EXPECT_EQ(back_to_thread1[0], 0);
    EXPECT_EQ(back_to_thread1[999], 999);

    // Verify the intermediate moved data is now empty
    EXPECT_EQ(moved_data.size(), 0);
  }
}
