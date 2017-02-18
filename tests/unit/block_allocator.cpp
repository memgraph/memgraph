#include "gtest/gtest.h"

#include "utils/memory/block_allocator.hpp"

TEST(BlockAllocatorTest, UnusedVsReleaseSize) {
  BlockAllocator<64> block_allocator(10);
  void *block = block_allocator.acquire();
  block_allocator.release(block);
  EXPECT_EQ(block_allocator.unused_size(), 9);
  EXPECT_EQ(block_allocator.release_size(), 1);
}

TEST(BlockAllocatorTest, CountMallocAndFreeCalls) {
  // TODO: implementation
  EXPECT_EQ(true, true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
