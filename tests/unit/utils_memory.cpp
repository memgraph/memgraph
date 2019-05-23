#include <cstdint>
#include <cstring>
#include <limits>

#include <gtest/gtest.h>

#include "utils/memory.hpp"

class TestMemory final : public utils::MemoryResource {
 public:
  size_t new_count_{0};
  size_t delete_count_{0};

 private:
  void *DoAllocate(size_t bytes, size_t alignment) override {
    new_count_++;
    EXPECT_TRUE(alignment != 0U && (alignment & (alignment - 1U)) == 0U)
        << "Alignment must be power of 2";
    EXPECT_TRUE(alignment <= alignof(std::max_align_t));
    EXPECT_NE(bytes, 0);
    const size_t pad_size = 32;
    EXPECT_TRUE(bytes + pad_size > bytes) << "TestMemory size overflow";
    EXPECT_TRUE(bytes + pad_size + alignment > bytes + alignment)
        << "TestMemory size overflow";
    EXPECT_TRUE(2U * alignment > alignment) << "TestMemory alignment overflow";
    // Allocate a block containing extra alignment and pad_size bytes, but
    // aligned to 2 * alignment. Then we can offset the ptr so that it's never
    // aligned to 2 * alignment. This ought to make allocator alignment issues
    // more obvious.
    void *ptr = utils::NewDeleteResource()->Allocate(
        alignment + bytes + pad_size, 2U * alignment);
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
    // Dealloate the original ptr, before alignment adjustment.
    return utils::NewDeleteResource()->Deallocate(
        static_cast<char *>(ptr) - alignment, bytes, alignment);
  }

  bool DoIsEqual(const utils::MemoryResource &other) const noexcept override {
    return this == &other;
  }
};

void *CheckAllocation(utils::MemoryResource *mem, size_t bytes,
                      size_t alignment = alignof(std::max_align_t)) {
  void *ptr = mem->Allocate(bytes, alignment);
  if (alignment > alignof(std::max_align_t))
    alignment = alignof(std::max_align_t);
  EXPECT_TRUE(ptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0)
      << "Allocated misaligned pointer!";
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
    utils::MonotonicBufferResource mem(1024, &test_mem);
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
    utils::MonotonicBufferResource mem(1024, &test_mem);
    CheckAllocation(&mem, 1025, 1);
    EXPECT_EQ(test_mem.new_count_, 1);
  }
  EXPECT_EQ(test_mem.delete_count_, 1);
  {
    utils::MonotonicBufferResource mem(1024, &test_mem);
    // Test with large alignment
    CheckAllocation(&mem, 1024, 1024);
    EXPECT_EQ(test_mem.new_count_, 2);
  }
  EXPECT_EQ(test_mem.delete_count_, 2);
}

TEST(MonotonicBufferResource, AllocationOverCapacity) {
  TestMemory test_mem;
  {
    utils::MonotonicBufferResource mem(1024, &test_mem);
    CheckAllocation(&mem, 24, 1);
    EXPECT_EQ(test_mem.new_count_, 1);
    CheckAllocation(&mem, 1000, 64);
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
  utils::MonotonicBufferResource mem(1024);
  EXPECT_THROW(mem.Allocate(24, 3), std::bad_alloc);
  EXPECT_THROW(mem.Allocate(24, 0), std::bad_alloc);
}

TEST(MonotonicBufferResource, AllocationWithSize0) {
  utils::MonotonicBufferResource mem(1024);
  EXPECT_THROW(mem.Allocate(0), std::bad_alloc);
}

TEST(MonotonicBufferResource, AllocationWithSizeOverflow) {
  size_t max_size = std::numeric_limits<size_t>::max();
  utils::MonotonicBufferResource mem(1024);
  // Setup so that the next allocation aligning max_size causes overflow.
  mem.Allocate(1, 1);
  EXPECT_THROW(mem.Allocate(max_size, 4), std::bad_alloc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
class ContainerWithAllocatorLast final {
 public:
  using allocator_type = utils::Allocator<int>;

  ContainerWithAllocatorLast() = default;
  explicit ContainerWithAllocatorLast(int value) : value_(value) {}
  ContainerWithAllocatorLast(int value, utils::MemoryResource *memory)
      : memory_(memory), value_(value) {}

  ContainerWithAllocatorLast(const ContainerWithAllocatorLast &other)
      : value_(other.value_) {}
  ContainerWithAllocatorLast(const ContainerWithAllocatorLast &other,
                             utils::MemoryResource *memory)
      : memory_(memory), value_(other.value_) {}

  utils::MemoryResource *memory_{nullptr};
  int value_{0};
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
class ContainerWithAllocatorFirst final {
 public:
  using allocator_type = utils::Allocator<int>;

  ContainerWithAllocatorFirst() = default;
  explicit ContainerWithAllocatorFirst(int value) : value_(value) {}
  ContainerWithAllocatorFirst(std::allocator_arg_t,
                              utils::MemoryResource *memory, int value)
      : memory_(memory), value_(value) {}

  ContainerWithAllocatorFirst(const ContainerWithAllocatorFirst &other)
      : value_(other.value_) {}
  ContainerWithAllocatorFirst(std::allocator_arg_t,
                              utils::MemoryResource *memory,
                              const ContainerWithAllocatorFirst &other)
      : memory_(memory), value_(other.value_) {}

  utils::MemoryResource *memory_{nullptr};
  int value_{0};
};

template <class T>
class AllocatorTest : public ::testing::Test {};

using ContainersWithAllocators =
    ::testing::Types<ContainerWithAllocatorLast, ContainerWithAllocatorFirst>;

TYPED_TEST_CASE(AllocatorTest, ContainersWithAllocators);

TYPED_TEST(AllocatorTest, PropagatesToStdUsesAllocator) {
  std::vector<TypeParam, utils::Allocator<TypeParam>> vec(
      utils::NewDeleteResource());
  vec.emplace_back(42);
  const auto &c = vec.front();
  EXPECT_EQ(c.value_, 42);
  EXPECT_EQ(c.memory_, utils::NewDeleteResource());
}
