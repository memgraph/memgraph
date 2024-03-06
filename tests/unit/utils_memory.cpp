// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <cstring>
#include <limits>

#include <gtest/gtest.h>

#include "utils/memory.hpp"

class TestMemory final : public memgraph::utils::MemoryResource {
 public:
  size_t new_count_{0};
  size_t delete_count_{0};

 private:
  static constexpr size_t kPadSize = 32;
  void *DoAllocate(size_t bytes, size_t alignment) override {
    new_count_++;
    EXPECT_TRUE(alignment != 0U && (alignment & (alignment - 1U)) == 0U) << "Alignment must be power of 2";
    EXPECT_NE(bytes, 0);
    const size_t pad_size = 32;
    EXPECT_TRUE(bytes + pad_size > bytes) << "TestMemory size overflow";
    EXPECT_TRUE(bytes + pad_size + alignment > bytes + alignment) << "TestMemory size overflow";
    EXPECT_TRUE(2U * alignment > alignment) << "TestMemory alignment overflow";
    // Allocate a block containing extra alignment and kPadSize bytes, but
    // aligned to 2 * alignment. Then we can offset the ptr so that it's never
    // aligned to 2 * alignment. This ought to make allocator alignment issues
    // more obvious.
    void *ptr = memgraph::utils::NewDeleteResource()->Allocate(alignment + bytes + kPadSize, 2U * alignment);
    // Clear allocated memory to 0xFF, marking the invalid region.
    memset(ptr, 0xFF, alignment + bytes + pad_size);
    // Offset the ptr so it's not aligned to 2 * alignment, but still aligned to
    // alignment.
    ptr = static_cast<char *>(ptr) + alignment;
    // Clear the valid region to 0x00, so that we can more easily test that the
    // allocator is doing the right thing.
    memset(ptr, 0, bytes);
    return ptr;
  }

  void DoDeallocate(void *ptr, size_t bytes, size_t alignment) override {
    delete_count_++;
    // Deallocate the original ptr, before alignment adjustment.
    return memgraph::utils::NewDeleteResource()->Deallocate(static_cast<char *>(ptr) - alignment,
                                                            alignment + bytes + kPadSize, 2U * alignment);
  }

  bool DoIsEqual(const memgraph::utils::MemoryResource &other) const noexcept override { return this == &other; }
};

void *CheckAllocation(memgraph::utils::MemoryResource *mem, size_t bytes,
                      size_t alignment = alignof(std::max_align_t)) {
  void *ptr = mem->Allocate(bytes, alignment);
  if (alignment > alignof(std::max_align_t)) alignment = alignof(std::max_align_t);
  EXPECT_TRUE(ptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0) << "Allocated misaligned pointer!";
  // There should be no 0xFF bytes because they are either padded at the end of
  // the allocated block or are found in already checked allocations.
  EXPECT_FALSE(memchr(ptr, 0xFF, bytes)) << "Invalid memory region!";
  // Mark the checked allocation with 0xFF bytes.
  memset(ptr, 0xFF, bytes);
  return ptr;
}

TEST(MonotonicBufferResource, AllocationWithinInitialSize) {
  TestMemory test_mem;
  {
    memgraph::utils::MonotonicBufferResource mem(1024, &test_mem);
    void *fst_ptr = CheckAllocation(&mem, 24, 1);
    void *snd_ptr = CheckAllocation(&mem, 1000, 1);
    EXPECT_EQ(test_mem.new_count_, 1);
    EXPECT_EQ(test_mem.delete_count_, 0);
    mem.Deallocate(snd_ptr, 1000, 1);
    mem.Deallocate(fst_ptr, 24, 1);
    EXPECT_EQ(test_mem.delete_count_, 0);
    mem.Release();
    EXPECT_EQ(test_mem.delete_count_, 1);
    CheckAllocation(&mem, 1024);
    EXPECT_EQ(test_mem.new_count_, 2);
    EXPECT_EQ(test_mem.delete_count_, 1);
  }
  EXPECT_EQ(test_mem.delete_count_, 2);
}

TEST(MonotonicBufferResource, AllocationOverInitialSize) {
  TestMemory test_mem;
  {
    memgraph::utils::MonotonicBufferResource mem(1024, &test_mem);
    CheckAllocation(&mem, 1025, 1);
    EXPECT_EQ(test_mem.new_count_, 1);
  }
  EXPECT_EQ(test_mem.delete_count_, 1);
  {
    memgraph::utils::MonotonicBufferResource mem(1024, &test_mem);
    CheckAllocation(&mem, 1025);
    EXPECT_EQ(test_mem.new_count_, 2);
  }
  EXPECT_EQ(test_mem.delete_count_, 2);
}

TEST(MonotonicBufferResource, AllocationOverCapacity) {
  TestMemory test_mem;
  {
    memgraph::utils::MonotonicBufferResource mem(1000, &test_mem);
    CheckAllocation(&mem, 24, 1);
    EXPECT_EQ(test_mem.new_count_, 1);
    CheckAllocation(&mem, 976);
    EXPECT_EQ(test_mem.new_count_, 2);
    EXPECT_EQ(test_mem.delete_count_, 0);
    mem.Release();
    EXPECT_EQ(test_mem.new_count_, 2);
    EXPECT_EQ(test_mem.delete_count_, 2);
    CheckAllocation(&mem, 1025, 1);
    EXPECT_EQ(test_mem.new_count_, 3);
    CheckAllocation(&mem, 1023, 1);
    // MonotonicBufferResource state after Release is called may or may not
    // allocate a larger block right from the start (i.e. tracked buffer sizes
    // before Release may be retained).
    EXPECT_TRUE(test_mem.new_count_ >= 3);
  }
  EXPECT_TRUE(test_mem.delete_count_ >= 3);
}

TEST(MonotonicBufferResource, AllocationWithAlignmentNotPowerOf2) {
  memgraph::utils::MonotonicBufferResource mem(1024);
  EXPECT_THROW(mem.Allocate(24, 3), std::bad_alloc);
  EXPECT_THROW(mem.Allocate(24, 0), std::bad_alloc);
}

TEST(MonotonicBufferResource, AllocationWithSize0) {
  memgraph::utils::MonotonicBufferResource mem(1024);
  EXPECT_THROW(mem.Allocate(0), std::bad_alloc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(MonotonicBufferResource, AllocationWithAlignmentGreaterThanMaxAlign) {
  TestMemory test_mem;
  memgraph::utils::MonotonicBufferResource mem(1024, &test_mem);
  CheckAllocation(&mem, 24, 2U * alignof(std::max_align_t));
}

TEST(MonotonicBufferResource, AllocationWithSizeOverflow) {
  size_t max_size = std::numeric_limits<size_t>::max();
  memgraph::utils::MonotonicBufferResource mem(1024);
  // Setup so that the next allocation aligning max_size causes overflow.
  mem.Allocate(1, 1);
  EXPECT_THROW(mem.Allocate(max_size, 4), std::bad_alloc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(MonotonicBufferResource, AllocationWithInitialBufferOnStack) {
  TestMemory test_mem;
  static constexpr size_t stack_data_size = 1024;
  char stack_data[stack_data_size];
  memset(stack_data, 0x42, stack_data_size);
  memgraph::utils::MonotonicBufferResource mem(&stack_data[0], stack_data_size, &test_mem);
  {
    char *ptr = reinterpret_cast<char *>(CheckAllocation(&mem, 1, 1));
    EXPECT_EQ(&stack_data[0], ptr);
    EXPECT_EQ(test_mem.new_count_, 0);
  }
  {
    char *ptr = reinterpret_cast<char *>(CheckAllocation(&mem, 1023, 1));
    EXPECT_EQ(&stack_data[1], ptr);
    EXPECT_EQ(test_mem.new_count_, 0);
  }
  CheckAllocation(&mem, 1);
  EXPECT_EQ(test_mem.new_count_, 1);
  mem.Release();
  // We will once more allocate from stack so reset it.
  memset(stack_data, 0x42, stack_data_size);
  EXPECT_EQ(test_mem.delete_count_, 1);
  {
    char *ptr = reinterpret_cast<char *>(CheckAllocation(&mem, 1024, 1));
    EXPECT_EQ(&stack_data[0], ptr);
    EXPECT_EQ(test_mem.new_count_, 1);
  }
  mem.Release();
  // Next allocation doesn't fit to stack so no need to reset it.
  EXPECT_EQ(test_mem.delete_count_, 1);
  {
    char *ptr = reinterpret_cast<char *>(CheckAllocation(&mem, 1025, 1));
    EXPECT_NE(&stack_data[0], ptr);
    EXPECT_EQ(test_mem.new_count_, 2);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PoolResource, SingleSmallBlockAllocations) {
  TestMemory test_mem;
  const size_t max_blocks_per_chunk = 3U;
  const size_t max_block_size = 64U;
  memgraph::utils::PoolResource mem(max_blocks_per_chunk, max_block_size, &test_mem);
  // Fill the first chunk.
  CheckAllocation(&mem, 64U, 1U);
  // May allocate more than once due to bookkeeping.
  EXPECT_GE(test_mem.new_count_, 1U);
  // Reset tracking and continue filling the first chunk.
  test_mem.new_count_ = 0U;
  CheckAllocation(&mem, 64U, 64U);
  CheckAllocation(&mem, 64U);
  EXPECT_EQ(test_mem.new_count_, 0U);
  // Reset tracking and fill the second chunk
  test_mem.new_count_ = 0U;
  CheckAllocation(&mem, 64U, 32U);
  auto *ptr1 = CheckAllocation(&mem, 32U, 64U);  // this will become 64b block
  auto *ptr2 = CheckAllocation(&mem, 64U, 32U);
  // We expect one allocation for chunk and at most one for bookkeeping.
  EXPECT_TRUE(test_mem.new_count_ >= 1U && test_mem.new_count_ <= 2U);
  test_mem.delete_count_ = 0U;
  mem.Deallocate(ptr1, 32U, 64U);
  mem.Deallocate(ptr2, 64U, 32U);
  EXPECT_EQ(test_mem.delete_count_, 0U);
  mem.Release();
  EXPECT_GE(test_mem.delete_count_, 2U);
  CheckAllocation(&mem, 64U, 1U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PoolResource, MultipleSmallBlockAllocations) {
  TestMemory test_mem;
  const size_t max_blocks_per_chunk = 1U;
  const size_t max_block_size = 64U;
  memgraph::utils::PoolResource mem(max_blocks_per_chunk, max_block_size, &test_mem);
  CheckAllocation(&mem, 64U);
  CheckAllocation(&mem, 18U, 2U);
  CheckAllocation(&mem, 24U, 8U);
  // May allocate more than once per chunk due to bookkeeping.
  EXPECT_GE(test_mem.new_count_, 3U);
  // Reset tracking and fill the second chunk
  test_mem.new_count_ = 0U;
  CheckAllocation(&mem, 64U);
  CheckAllocation(&mem, 18U, 2U);
  CheckAllocation(&mem, 24U, 8U);
  // We expect one allocation for chunk and at most one for bookkeeping.
  EXPECT_TRUE(test_mem.new_count_ >= 3U && test_mem.new_count_ <= 6U);
  mem.Release();
  EXPECT_GE(test_mem.delete_count_, 6U);
  CheckAllocation(&mem, 64U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PoolResource, BigBlockAllocations) {
  TestMemory test_mem;
  TestMemory test_mem_unpooled;
  const size_t max_blocks_per_chunk = 3U;
  const size_t max_block_size = 64U;
  memgraph::utils::PoolResource mem(max_blocks_per_chunk, max_block_size, &test_mem, &test_mem_unpooled);
  CheckAllocation(&mem, max_block_size + 1, 1U);
  // May allocate more than once per block due to bookkeeping.
  EXPECT_GE(test_mem_unpooled.new_count_, 1U);
  CheckAllocation(&mem, max_block_size + 1, 1U);
  EXPECT_GE(test_mem_unpooled.new_count_, 2U);
  auto *ptr = CheckAllocation(&mem, max_block_size * 2, 1U);
  EXPECT_GE(test_mem_unpooled.new_count_, 3U);
  mem.Deallocate(ptr, max_block_size * 2, 1U);
  EXPECT_GE(test_mem_unpooled.delete_count_, 1U);
  mem.Release();
  EXPECT_GE(test_mem_unpooled.delete_count_, 3U);
  CheckAllocation(&mem, max_block_size + 1, 1U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PoolResource, BlockSizeIsNotMultipleOfAlignment) {
  const size_t max_blocks_per_chunk = 3U;
  const size_t max_block_size = 64U;
  memgraph::utils::PoolResource mem(max_blocks_per_chunk, max_block_size);
  EXPECT_THROW(mem.Allocate(64U, 24U), std::bad_alloc);
  EXPECT_THROW(mem.Allocate(63U), std::bad_alloc);
  EXPECT_THROW(mem.Allocate(max_block_size + 1, max_block_size), std::bad_alloc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PoolResource, AllocationWithOverflow) {
  {
    const size_t max_blocks_per_chunk = 2U;
    memgraph::utils::PoolResource mem(max_blocks_per_chunk, std::numeric_limits<size_t>::max());
    EXPECT_THROW(mem.Allocate(std::numeric_limits<size_t>::max(), 1U), std::bad_alloc);
    // Throws because initial chunk block is aligned to
    // memgraph::utils::Ceil2(block_size), which wraps in this case.
    EXPECT_THROW(mem.Allocate((std::numeric_limits<size_t>::max() - 1U) / max_blocks_per_chunk, 1U), std::bad_alloc);
  }
  {
    const size_t max_blocks_per_chunk = memgraph::utils::impl::Pool::MaxBlocksInChunk;
    memgraph::utils::PoolResource mem(max_blocks_per_chunk, std::numeric_limits<size_t>::max());
    EXPECT_THROW(mem.Allocate(std::numeric_limits<size_t>::max(), 1U), std::bad_alloc);
    // Throws because initial chunk block is aligned to
    // memgraph::utils::Ceil2(block_size), which wraps in this case.
    EXPECT_THROW(mem.Allocate((std::numeric_limits<size_t>::max() - 1U) / max_blocks_per_chunk, 1U), std::bad_alloc);
  }
}

TEST(PoolResource, BlockDeallocation) {
  TestMemory test_mem;
  const size_t max_blocks_per_chunk = 2U;
  const size_t max_block_size = 64U;
  memgraph::utils::PoolResource mem(max_blocks_per_chunk, max_block_size, &test_mem);
  auto *ptr = CheckAllocation(&mem, max_block_size);
  test_mem.new_count_ = 0U;
  // Do another allocation before deallocating `ptr`, so that we are sure that
  // the chunk of 2 blocks is still alive and therefore `ptr` may be reused when
  // it's deallocated. If we deallocate now, the implementation may choose to
  // free the whole chunk, and we do not want that for the purposes of this
  // test.
  CheckAllocation(&mem, max_block_size);
  EXPECT_EQ(test_mem.new_count_, 0U);
  EXPECT_EQ(test_mem.delete_count_, 0U);
  mem.Deallocate(ptr, max_block_size);
  EXPECT_EQ(test_mem.delete_count_, 0U);
  // CheckAllocation(&mem, max_block_size) will fail as PoolResource should
  // reuse free blocks.
  EXPECT_EQ(ptr, mem.Allocate(max_block_size));
  EXPECT_EQ(test_mem.new_count_, 0U);
}

class AllocationTrackingMemory final : public memgraph::utils::MemoryResource {
 public:
  std::vector<size_t> allocated_sizes_;

 private:
  void *DoAllocate(size_t bytes, size_t alignment) override {
    allocated_sizes_.push_back(bytes);
    return memgraph::utils::NewDeleteResource()->Allocate(bytes, alignment);
  }

  void DoDeallocate(void *ptr, size_t bytes, size_t alignment) override {
    return memgraph::utils::NewDeleteResource()->Deallocate(ptr, bytes, alignment);
  }

  bool DoIsEqual(const memgraph::utils::MemoryResource &other) const noexcept override { return this == &other; }
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(MonotonicBufferResource, ResetGrowthFactor) {
  AllocationTrackingMemory test_mem;
  static constexpr size_t stack_data_size = 1024;
  char stack_data[stack_data_size];
  memgraph::utils::MonotonicBufferResource mem(&stack_data[0], stack_data_size, &test_mem);
  mem.Allocate(stack_data_size + 1);
  mem.Release();
  mem.Allocate(stack_data_size + 1);
  ASSERT_EQ(test_mem.allocated_sizes_.size(), 2);
  ASSERT_EQ(test_mem.allocated_sizes_.front(), test_mem.allocated_sizes_.back());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
class ContainerWithAllocatorLast final {
 public:
  using allocator_type = memgraph::utils::Allocator<int>;

  ContainerWithAllocatorLast() = default;
  explicit ContainerWithAllocatorLast(int value) : value_(value) {}
  ContainerWithAllocatorLast(int value, memgraph::utils::MemoryResource *memory) : memory_(memory), value_(value) {}

  ContainerWithAllocatorLast(const ContainerWithAllocatorLast &other) : value_(other.value_) {}
  ContainerWithAllocatorLast(const ContainerWithAllocatorLast &other, memgraph::utils::MemoryResource *memory)
      : memory_(memory), value_(other.value_) {}

  memgraph::utils::MemoryResource *memory_{nullptr};
  int value_{0};
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
class ContainerWithAllocatorFirst final {
 public:
  using allocator_type = memgraph::utils::Allocator<int>;

  ContainerWithAllocatorFirst() = default;
  explicit ContainerWithAllocatorFirst(int value) : value_(value) {}
  ContainerWithAllocatorFirst(std::allocator_arg_t, memgraph::utils::MemoryResource *memory, int value)
      : memory_(memory), value_(value) {}

  ContainerWithAllocatorFirst(const ContainerWithAllocatorFirst &other) : value_(other.value_) {}
  ContainerWithAllocatorFirst(std::allocator_arg_t, memgraph::utils::MemoryResource *memory,
                              const ContainerWithAllocatorFirst &other)
      : memory_(memory), value_(other.value_) {}

  memgraph::utils::MemoryResource *memory_{nullptr};
  int value_{0};
};

template <class T>
class AllocatorTest : public ::testing::Test {};

using ContainersWithAllocators = ::testing::Types<ContainerWithAllocatorLast, ContainerWithAllocatorFirst>;

TYPED_TEST_CASE(AllocatorTest, ContainersWithAllocators);

TYPED_TEST(AllocatorTest, PropagatesToStdUsesAllocator) {
  std::vector<TypeParam, memgraph::utils::Allocator<TypeParam>> vec(memgraph::utils::NewDeleteResource());
  vec.emplace_back(42);
  const auto &c = vec.front();
  EXPECT_EQ(c.value_, 42);
  EXPECT_EQ(c.memory_, memgraph::utils::NewDeleteResource());
}

TYPED_TEST(AllocatorTest, PropagatesToStdPairUsesAllocator) {
  {
    std::vector<std::pair<ContainerWithAllocatorFirst, TypeParam>,
                memgraph::utils::Allocator<std::pair<ContainerWithAllocatorFirst, TypeParam>>>
        vec(memgraph::utils::NewDeleteResource());
    vec.emplace_back(1, 2);
    const auto &pair = vec.front();
    EXPECT_EQ(pair.first.value_, 1);
    EXPECT_EQ(pair.second.value_, 2);
    EXPECT_EQ(pair.first.memory_, memgraph::utils::NewDeleteResource());
    EXPECT_EQ(pair.second.memory_, memgraph::utils::NewDeleteResource());
  }
  {
    std::vector<std::pair<ContainerWithAllocatorLast, TypeParam>,
                memgraph::utils::Allocator<std::pair<ContainerWithAllocatorLast, TypeParam>>>
        vec(memgraph::utils::NewDeleteResource());
    vec.emplace_back(1, 2);
    const auto &pair = vec.front();
    EXPECT_EQ(pair.first.value_, 1);
    EXPECT_EQ(pair.second.value_, 2);
    EXPECT_EQ(pair.first.memory_, memgraph::utils::NewDeleteResource());
    EXPECT_EQ(pair.second.memory_, memgraph::utils::NewDeleteResource());
  }
}
