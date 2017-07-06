#include "gtest/gtest.h"

#include "memory/allocator.hpp"

TEST(AllocatorTest, ABlockOfIntegersCanBeAllocated) {
  constexpr int N = 100;

  fast_allocator<int> a;

  int* xs = a.allocate(N);

  for (int i = 0; i < N; ++i) xs[i] = i;

  // can we read them back?
  for (int i = 0; i < N; ++i) ASSERT_EQ(xs[i], i);

  // we should be able to free the memory
  a.deallocate(xs, N);
}

TEST(AllocatorTest, AllocatorShouldWorkWithStructures) {
  struct TestObject {
    TestObject(int a, int b, int c, int d) : a(a), b(b), c(c), d(d) {}

    int a, b, c, d;
  };

  fast_allocator<TestObject> a;

  // allocate a single object
  {
    auto* test = a.allocate(1);
    *test = TestObject(1, 2, 3, 4);

    ASSERT_EQ(test->a, 1);
    ASSERT_EQ(test->b, 2);
    ASSERT_EQ(test->c, 3);
    ASSERT_EQ(test->d, 4);

    a.deallocate(test, 1);
  }

  // Allocate a block of structures
  {
    constexpr int N = 8;
    auto* tests = a.allocate(N);

    // structures should not overlap!
    for (int i = 0; i < N; ++i) tests[i] = TestObject(i, i, i, i);

    for (int i = 0; i < N; ++i) {
      ASSERT_EQ(tests[i].a, i);
      ASSERT_EQ(tests[i].b, i);
      ASSERT_EQ(tests[i].c, i);
      ASSERT_EQ(tests[i].d, i);
    }

    a.deallocate(tests, N);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
