#include <vector>

#include "gtest/gtest.h"

#include "utils/synchronized.hpp"

class NoMoveNoCopy {
 public:
  NoMoveNoCopy(int, int) {}

  NoMoveNoCopy(const NoMoveNoCopy &) = delete;
  NoMoveNoCopy(NoMoveNoCopy &&) = delete;
  NoMoveNoCopy &operator=(const NoMoveNoCopy &) = delete;
  NoMoveNoCopy &operator=(NoMoveNoCopy &&) = delete;
  ~NoMoveNoCopy() = default;
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(Synchronized, Constructors) {
  {
    utils::Synchronized<std::vector<int>> vec;
    EXPECT_TRUE(vec->empty());
  }
  {
    std::vector<int> data = {1, 2, 3};
    utils::Synchronized<std::vector<int>> vec(data);
    EXPECT_EQ(data.size(), 3);
    EXPECT_EQ(vec->size(), 3);
  }
  {
    std::vector<int> data = {1, 2, 3};
    utils::Synchronized<std::vector<int>> vec(std::move(data));
    // data is guaranteed by the standard to be empty after move
    // NOLINTNEXTLINE(bugprone-use-after-move, hicpp-invalid-access-moved)
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(vec->size(), 3);
  }
  { utils::Synchronized<NoMoveNoCopy> object(3, 4); }
}

bool test_lock_locked = false;

class TestLock {
 public:
  void lock() {
    ASSERT_FALSE(test_lock_locked);
    test_lock_locked = true;
  }
  void unlock() {
    ASSERT_TRUE(test_lock_locked);
    test_lock_locked = false;
  }
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(Synchronized, Usage) {
  utils::Synchronized<std::vector<int>, TestLock> my_vector;
  {
    // LockedPtr
    auto ptr = my_vector.Lock();
    ASSERT_TRUE(test_lock_locked);
    ptr->push_back(5);
  }
  ASSERT_FALSE(test_lock_locked);

  {
    // Indirection operator
    my_vector->push_back(6);
  }

  {
    // Lambda
    my_vector.WithLock([](auto &my_vector) {
      ASSERT_TRUE(test_lock_locked);
      EXPECT_EQ(my_vector.size(), 2);
      EXPECT_EQ(my_vector[0], 5);
      EXPECT_EQ(my_vector[1], 6);
    });
    ASSERT_FALSE(test_lock_locked);
  }
}
