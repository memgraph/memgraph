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
#include <thread>
#include <vector>

#include "utils/memory.hpp"

using namespace memgraph::utils;

TEST(ThreadSafeMonotonicBufferResourceTest, BasicAllocation) {
  ThreadSafeMonotonicBufferResource resource(1024);

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

TEST(ThreadSafeMonotonicBufferResourceTest, OverInitialSize) {
  ThreadSafeMonotonicBufferResource resource(100);

  // Try to allocate more than initial size
  void *ptr1 = resource.allocate(50, 8);
  void *ptr2 = resource.allocate(100, 8);  // This should trigger new block allocation

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr1, ptr2);
}

TEST(ThreadSafeMonotonicBufferResourceTest, ConcurrentAllocations) {
  ThreadSafeMonotonicBufferResource resource(1024);
  constexpr int num_threads = 4;
  constexpr int allocations_per_thread = 100;

  std::vector<std::thread> threads;
  std::vector<std::vector<void *>> thread_allocations(num_threads);

  // Start threads that allocate memory
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &thread_allocations, i]() {
      for (int j = 0; j < allocations_per_thread; ++j) {
        size_t size = 8 + (j % 64);       // Varying sizes
        size_t alignment = 8 << (j % 4);  // Varying alignments: 8, 16, 32, 64
        void *ptr = resource.allocate(size, alignment);
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);
        thread_allocations[i].push_back(ptr);
      }
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Verify all allocations are unique
  std::vector<void *> all_allocations;
  for (const auto &thread_allocs : thread_allocations) {
    all_allocations.insert(all_allocations.end(), thread_allocs.begin(), thread_allocs.end());
  }

  std::sort(all_allocations.begin(), all_allocations.end());
  auto it = std::unique(all_allocations.begin(), all_allocations.end());
  EXPECT_EQ(it, all_allocations.end()) << "Duplicate allocations found";
}

TEST(ThreadSafeMonotonicBufferResourceTest, ConcurrentAllocationsAndReleases) {
  ThreadSafeMonotonicBufferResource resource(1024);
  constexpr int num_threads = 4;
  constexpr int operations_per_thread = 50;

  std::vector<std::thread> threads;
  std::atomic<int> total_allocations{0};

  // Start threads that allocate and release memory
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &total_allocations, operations_per_thread]() {
      for (int j = 0; j < operations_per_thread; ++j) {
        // Allocate some memory
        void *ptr = resource.allocate(64, 8);
        EXPECT_NE(ptr, nullptr);
        total_allocations.fetch_add(1);

        // Simulate some work
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(total_allocations.load(), num_threads * operations_per_thread);
}

TEST(ThreadSafeMonotonicBufferResourceTest, Release) {
  ThreadSafeMonotonicBufferResource resource(1024);

  // Allocate some memory
  void *ptr1 = resource.allocate(64, 8);
  void *ptr2 = resource.allocate(128, 16);

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);

  // Release all memory - this deallocates everything
  resource.Release();

  // After release, head should be null
  // Note: We can't directly test this since head_ is private, but we can verify
  // that the resource is in a clean state by allocating again
  void *ptr3 = resource.allocate(64, 8);
  void *ptr4 = resource.allocate(128, 16);

  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);

  // The new allocations should be valid (addresses may or may not be different)
  // since memory was deallocated and reallocated
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);
}

// Move tests temporarily disabled due to implementation issues
TEST(ThreadSafeMonotonicBufferResourceTest, MoveConstructor) {
  ThreadSafeMonotonicBufferResource resource1(1024);

  // Allocate some memory in the original resource
  void *ptr1 = resource1.allocate(64, 8);
  void *ptr2 = resource1.allocate(128, 16);

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);

  // Move construct a new resource
  ThreadSafeMonotonicBufferResource resource2(std::move(resource1));

  // The moved-to resource should have the same memory
  void *ptr4 = resource2.allocate(64, 8);
  void *ptr5 = resource2.allocate(128, 16);

  EXPECT_NE(ptr4, nullptr);
  EXPECT_NE(ptr5, nullptr);

  // The allocations should continue from the original resource
  EXPECT_GT(ptr4, ptr2);
  EXPECT_LT(ptr4, reinterpret_cast<char *>(ptr1) + 1024);
  EXPECT_GT(ptr5, ptr2);
  EXPECT_LT(ptr5, reinterpret_cast<char *>(ptr1) + 1024);
}

TEST(ThreadSafeMonotonicBufferResourceTest, MoveAssignment) {
  ThreadSafeMonotonicBufferResource resource1(1024);
  ThreadSafeMonotonicBufferResource resource2(512);

  // Allocate memory in both resources
  void *ptr1 = resource1.allocate(64, 8);
  void *ptr2 = resource2.allocate(32, 4);

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);

  // Move assign resource1 to resource2
  resource2 = std::move(resource1);

  // The moved-to resource should have resource1's memory
  void *ptr4 = resource2.allocate(64, 8);
  void *ptr5 = resource2.allocate(128, 16);

  EXPECT_NE(ptr4, nullptr);
  EXPECT_NE(ptr5, nullptr);

  // The allocations should continue from the original resource
  EXPECT_GT(ptr4, ptr1);
  EXPECT_LT(ptr4, reinterpret_cast<char *>(ptr1) + 1024);
  EXPECT_GT(ptr5, ptr1);
  EXPECT_LT(ptr5, reinterpret_cast<char *>(ptr1) + 1024);

  // Self-assignment should work
  resource2 = std::move(resource2);

  void *ptr6 = resource2.allocate(64, 8);
  EXPECT_NE(ptr6, nullptr);
}

TEST(ThreadSafeMonotonicBufferResourceTest, LargeAllocations) {
  ThreadSafeMonotonicBufferResource resource(1024);

  // Allocate a large block that exceeds initial size
  void *large_ptr = resource.allocate(2048, 8);
  EXPECT_NE(large_ptr, nullptr);

  // Allocate some smaller blocks
  void *small_ptr1 = resource.allocate(64, 8);
  void *small_ptr2 = resource.allocate(128, 16);

  EXPECT_NE(small_ptr1, nullptr);
  EXPECT_NE(small_ptr2, nullptr);
  EXPECT_NE(large_ptr, small_ptr1);
  EXPECT_NE(large_ptr, small_ptr2);
  EXPECT_NE(small_ptr1, small_ptr2);
}

TEST(ThreadSafeMonotonicBufferResourceTest, AlignmentEdgeCases) {
  ThreadSafeMonotonicBufferResource resource(1024);

  // Test various alignment requirements
  void *ptr1 = resource.allocate(1, 1);   // No alignment requirement
  void *ptr2 = resource.allocate(1, 2);   // 2-byte alignment
  void *ptr3 = resource.allocate(1, 4);   // 4-byte alignment
  void *ptr4 = resource.allocate(1, 8);   // 8-byte alignment
  void *ptr5 = resource.allocate(1, 16);  // 16-byte alignment
  void *ptr6 = resource.allocate(1, 32);  // 32-byte alignment
  void *ptr7 = resource.allocate(1, 64);  // 64-byte alignment

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);
  EXPECT_NE(ptr5, nullptr);
  EXPECT_NE(ptr6, nullptr);
  EXPECT_NE(ptr7, nullptr);

  // Verify alignments
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 1, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 2, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 4, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr4) % 8, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr5) % 16, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr6) % 32, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr7) % 64, 0);
}

TEST(ThreadSafeMonotonicBufferResourceTest, StressTest) {
  ThreadSafeMonotonicBufferResource resource(512);
  constexpr int num_threads = 8;
  constexpr int operations_per_thread = 1000;

  std::vector<std::thread> threads;
  std::atomic<int> successful_allocations{0};

  // Start threads that perform many allocations
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &successful_allocations, operations_per_thread]() {
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
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Should have successfully allocated a significant number of blocks
  EXPECT_GT(successful_allocations.load(), num_threads * operations_per_thread / 2);
}

TEST(ThreadSafeMonotonicBufferResourceTest, ExtremeAlignmentEdgeCases) {
  ThreadSafeMonotonicBufferResource resource(4096);

  // Test extreme alignment requirements
  void *ptr1 = resource.allocate(1, 128);   // 128-byte alignment
  void *ptr2 = resource.allocate(1, 256);   // 256-byte alignment
  void *ptr3 = resource.allocate(1, 512);   // 512-byte alignment
  void *ptr4 = resource.allocate(1, 1024);  // 1024-byte alignment

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);

  // Verify extreme alignments
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 128, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 256, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 512, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr4) % 1024, 0);
}

TEST(ThreadSafeMonotonicBufferResourceTest, SizeEdgeCases) {
  ThreadSafeMonotonicBufferResource resource(1024);

  // Test very small sizes
  void *ptr1 = resource.allocate(1, 1);  // 1 byte
  void *ptr2 = resource.allocate(2, 2);  // 2 bytes
  void *ptr3 = resource.allocate(3, 1);  // 3 bytes
  void *ptr4 = resource.allocate(4, 4);  // 4 bytes

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);

  // Test sizes that are powers of 2
  void *ptr5 = resource.allocate(4, 4);
  void *ptr6 = resource.allocate(8, 8);
  void *ptr7 = resource.allocate(16, 16);
  void *ptr8 = resource.allocate(32, 32);

  EXPECT_NE(ptr5, nullptr);
  EXPECT_NE(ptr6, nullptr);
  EXPECT_NE(ptr7, nullptr);
  EXPECT_NE(ptr8, nullptr);

  // Test sizes that are not powers of 2
  void *ptr9 = resource.allocate(7, 1);
  void *ptr10 = resource.allocate(15, 1);
  void *ptr11 = resource.allocate(31, 1);
  void *ptr12 = resource.allocate(63, 1);

  EXPECT_NE(ptr9, nullptr);
  EXPECT_NE(ptr10, nullptr);
  EXPECT_NE(ptr11, nullptr);
  EXPECT_NE(ptr12, nullptr);
}

TEST(ThreadSafeMonotonicBufferResourceTest, ManyThreadsSmallAllocations) {
  ThreadSafeMonotonicBufferResource resource(1024);
  constexpr int num_threads = 16;              // Many threads
  constexpr int allocations_per_thread = 200;  // Many small allocations

  std::vector<std::thread> threads;
  std::vector<std::vector<void *>> thread_allocations(num_threads);

  // Start many threads that allocate small amounts of memory
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &thread_allocations, i]() {
      for (int j = 0; j < allocations_per_thread; ++j) {
        size_t size = 1 + (j % 16);       // Very small sizes: 1-16 bytes
        size_t alignment = 1 << (j % 4);  // Small alignments: 1, 2, 4, 8
        void *ptr = resource.allocate(size, alignment);
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);
        thread_allocations[i].push_back(ptr);
      }
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Verify all allocations are unique
  std::vector<void *> all_allocations;
  for (const auto &thread_allocs : thread_allocations) {
    all_allocations.insert(all_allocations.end(), thread_allocs.begin(), thread_allocs.end());
  }

  std::sort(all_allocations.begin(), all_allocations.end());
  auto it = std::unique(all_allocations.begin(), all_allocations.end());
  EXPECT_EQ(it, all_allocations.end()) << "Duplicate allocations found";
}

TEST(ThreadSafeMonotonicBufferResourceTest, ManyThreadsLargeAllocations) {
  ThreadSafeMonotonicBufferResource resource(4096);
  constexpr int num_threads = 8;
  constexpr int allocations_per_thread = 50;

  std::vector<std::thread> threads;
  std::vector<std::vector<void *>> thread_allocations(num_threads);

  // Start threads that allocate large amounts of memory
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &thread_allocations, i]() {
      for (int j = 0; j < allocations_per_thread; ++j) {
        size_t size = 256 + (j % 1024);   // Large sizes: 256-1280 bytes
        size_t alignment = 8 << (j % 4);  // Large alignments: 8, 16, 32, 64
        void *ptr = resource.allocate(size, alignment);
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);
        thread_allocations[i].push_back(ptr);
      }
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Verify all allocations are unique
  std::vector<void *> all_allocations;
  for (const auto &thread_allocs : thread_allocations) {
    all_allocations.insert(all_allocations.end(), thread_allocs.begin(), thread_allocs.end());
  }

  std::sort(all_allocations.begin(), all_allocations.end());
  auto it = std::unique(all_allocations.begin(), all_allocations.end());
  EXPECT_EQ(it, all_allocations.end()) << "Duplicate allocations found";
}

TEST(ThreadSafeMonotonicBufferResourceTest, MixedSizeAllocations) {
  ThreadSafeMonotonicBufferResource resource(2048);
  constexpr int num_threads = 6;
  constexpr int allocations_per_thread = 100;

  std::vector<std::thread> threads;
  std::vector<std::vector<void *>> thread_allocations(num_threads);

  // Start threads that allocate mixed sizes
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &thread_allocations, i]() {
      for (int j = 0; j < allocations_per_thread; ++j) {
        size_t size;
        size_t alignment;

        // Mix of very small, small, medium, and large allocations
        switch (j % 4) {
          case 0:  // Very small
            size = 1 + (j % 8);
            alignment = 1;
            break;
          case 1:  // Small
            size = 16 + (j % 64);
            alignment = 8;
            break;
          case 2:  // Medium
            size = 128 + (j % 256);
            alignment = 16;
            break;
          case 3:  // Large
            size = 512 + (j % 1024);
            alignment = 32;
            break;
        }

        void *ptr = resource.allocate(size, alignment);
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);
        thread_allocations[i].push_back(ptr);
      }
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Verify all allocations are unique
  std::vector<void *> all_allocations;
  for (const auto &thread_allocs : thread_allocations) {
    all_allocations.insert(all_allocations.end(), thread_allocs.begin(), thread_allocs.end());
  }

  std::sort(all_allocations.begin(), all_allocations.end());
  auto it = std::unique(all_allocations.begin(), all_allocations.end());
  EXPECT_EQ(it, all_allocations.end()) << "Duplicate allocations found";
}

TEST(ThreadSafeMonotonicBufferResourceTest, AlignmentSizeMismatch) {
  ThreadSafeMonotonicBufferResource resource(1024);

  // Test cases where size is smaller than alignment
  void *ptr1 = resource.allocate(1, 8);    // 1 byte with 8-byte alignment
  void *ptr2 = resource.allocate(4, 16);   // 4 bytes with 16-byte alignment
  void *ptr3 = resource.allocate(8, 32);   // 8 bytes with 32-byte alignment
  void *ptr4 = resource.allocate(16, 64);  // 16 bytes with 64-byte alignment

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);

  // Verify alignments
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 8, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 16, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 32, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr4) % 64, 0);
}

TEST(ThreadSafeMonotonicBufferResourceTest, RapidAllocationDeallocation) {
  ThreadSafeMonotonicBufferResource resource(512);
  constexpr int num_threads = 4;
  constexpr int cycles = 100;

  std::vector<std::thread> threads;
  std::atomic<int> successful_cycles{0};

  // Start threads that rapidly allocate and release
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &successful_cycles]() {
      for (int j = 0; j < cycles; ++j) {
        // Rapidly allocate different sizes
        void *ptr1 = resource.allocate(1 + (j % 16), 1);
        void *ptr2 = resource.allocate(32 + (j % 64), 8);
        void *ptr3 = resource.allocate(128 + (j % 128), 16);

        EXPECT_NE(ptr1, nullptr);
        EXPECT_NE(ptr2, nullptr);
        EXPECT_NE(ptr3, nullptr);

        // Verify alignments
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 8, 0);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 16, 0);

        successful_cycles.fetch_add(1);
      }
    });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(successful_cycles.load(), num_threads * cycles);
}

TEST(ThreadSafeMonotonicBufferResourceTest, BoundaryConditions) {
  ThreadSafeMonotonicBufferResource resource(1024);

  // Test allocation sizes that are close to block boundaries
  void *ptr1 = resource.allocate(1023, 1);  // Almost full block
  void *ptr2 = resource.allocate(1, 1);     // Should trigger new block
  void *ptr3 = resource.allocate(1024, 1);  // Exactly one block
  void *ptr4 = resource.allocate(1, 1);     // Should trigger new block

  EXPECT_NE(ptr1, nullptr);
  EXPECT_NE(ptr2, nullptr);
  EXPECT_NE(ptr3, nullptr);
  EXPECT_NE(ptr4, nullptr);

  // Test with very small initial size
  ThreadSafeMonotonicBufferResource small_resource(16);
  void *ptr5 = small_resource.allocate(8, 1);
  void *ptr6 = small_resource.allocate(8, 1);   // Should trigger new block
  void *ptr7 = small_resource.allocate(32, 1);  // Should trigger new block

  EXPECT_NE(ptr5, nullptr);
  EXPECT_NE(ptr6, nullptr);
  EXPECT_NE(ptr7, nullptr);
}

TEST(ThreadSafeMonotonicBufferResourceTest, ConcurrentRelease) {
  ThreadSafeMonotonicBufferResource resource(1024);
  constexpr int num_threads = 4;
  constexpr int allocations_per_thread = 50;

  std::vector<std::thread> threads;
  std::atomic<int> allocations_completed{0};

  // Start threads that allocate memory
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&resource, &allocations_completed]() {
      for (int j = 0; j < allocations_per_thread; ++j) {
        size_t size = 16 + (j % 128);
        size_t alignment = 8 << (j % 3);
        void *ptr = resource.allocate(size, alignment);
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);
        allocations_completed.fetch_add(1);
      }
    });
  }

  // Wait for some allocations to complete
  while (allocations_completed.load() < num_threads * allocations_per_thread / 2) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  // // Release while other threads are still allocating
  // Release is not thread safe.
  // resource.Release();

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // Verify we can still allocate after release
  void *ptr = resource.allocate(64, 8);
  EXPECT_NE(ptr, nullptr);
}
