#include "catch.hpp"

#include "utils/memory/allocator.hpp"

TEST_CASE("A block of integers can be allocated") {
  constexpr int N = 100;

  fast_allocator<int> a;

  int* xs = a.allocate(N);

  for (int i = 0; i < N; ++i) xs[i] = i;

  // can we read them back?
  for (int i = 0; i < N; ++i) REQUIRE(xs[i] == i);

  // we should be able to free the memory
  a.deallocate(xs, N);
}

TEST_CASE("Allocator should work with structures") {
  struct TestObject {
    TestObject(int a, int b, int c, int d) : a(a), b(b), c(c), d(d) {}

    int a, b, c, d;
  };

  fast_allocator<TestObject> a;

  SECTION("Allocate a single object") {
    auto* test = a.allocate(1);
    *test = TestObject(1, 2, 3, 4);

    REQUIRE(test->a == 1);
    REQUIRE(test->b == 2);
    REQUIRE(test->c == 3);
    REQUIRE(test->d == 4);

    a.deallocate(test, 1);
  }

  SECTION("Allocate a block of structures") {
    constexpr int N = 8;
    auto* tests = a.allocate(N);

    // structures should not overlap!
    for (int i = 0; i < N; ++i) tests[i] = TestObject(i, i, i, i);

    for (int i = 0; i < N; ++i) {
      REQUIRE(tests[i].a == i);
      REQUIRE(tests[i].b == i);
      REQUIRE(tests[i].c == i);
      REQUIRE(tests[i].d == i);
    }

    a.deallocate(tests, N);
  }
}
