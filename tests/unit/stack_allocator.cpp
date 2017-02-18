#include "gtest/gtest.h"

#include "utils/memory/stack_allocator.hpp"

struct Object {
  int a;
  int b;

  Object(int a, int b) : a(a), b(b) {}
};

TEST(StackAllocatorTest, AllocationAndObjectValidity) {
  StackAllocator allocator;
  for (int i = 0; i < 64 * 1024; ++i) {
    auto object = allocator.make<Object>(1, 2);
    ASSERT_EQ(object->a, 1);
    ASSERT_EQ(object->b, 2);
  }
}

TEST(StackAllocatorTest, CountMallocAndFreeCalls) {
  // TODO: implementation
  EXPECT_EQ(true, true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
