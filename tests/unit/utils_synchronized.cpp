// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <vector>

#include "gtest/gtest.h"

#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

static_assert(utils::SharedMutex<std::shared_mutex>, "std::shared_mutex must be considered as shared mutex!");
static_assert(utils::SharedMutex<utils::RWLock>, "utils::RWLock must be considered as shared mutex!");
static_assert(!utils::SharedMutex<std::mutex>, "std::mutex must not be considered as shared mutex!");

class NoMoveNoCopy {
 public:
  NoMoveNoCopy(int, int) {}

  NoMoveNoCopy(const NoMoveNoCopy &) = delete;
  NoMoveNoCopy(NoMoveNoCopy &&) = delete;
  NoMoveNoCopy &operator=(const NoMoveNoCopy &) = delete;
  NoMoveNoCopy &operator=(NoMoveNoCopy &&) = delete;
  ~NoMoveNoCopy() = default;
};

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

bool test_shared_lock_locked = false;

class TestSharedLock {
 public:
  void lock() {
    ASSERT_FALSE(test_lock_locked);
    test_lock_locked = true;
  }
  void unlock() {
    ASSERT_TRUE(test_lock_locked);
    test_lock_locked = false;
  }
  void lock_shared() {
    ASSERT_FALSE(test_shared_lock_locked);
    test_shared_lock_locked = true;
  }
  void unlock_shared() {
    ASSERT_TRUE(test_shared_lock_locked);
    test_shared_lock_locked = false;
  }
};

template <typename TSynchronizedVector>
void CheckWriteLock(TSynchronizedVector &my_vector) {
  ASSERT_TRUE(my_vector->empty()) << "Cannot use not empty vector";
  {
    SCOPED_TRACE("LockedPtr");
    auto ptr = my_vector.Lock();
    ASSERT_TRUE(test_lock_locked);
    ptr->push_back(5);
  }
  ASSERT_FALSE(test_lock_locked);

  {
    SCOPED_TRACE("Indirection operator");
    my_vector->push_back(6);
  }

  {
    SCOPED_TRACE("Lambda");
    my_vector.WithLock([](auto &my_vector) {
      ASSERT_TRUE(test_lock_locked);
      EXPECT_EQ(my_vector.size(), 2);
      EXPECT_EQ(my_vector[0], 5);
      EXPECT_EQ(my_vector[1], 6);
    });
    ASSERT_FALSE(test_lock_locked);
  }
}

template <typename TSynchronizedVector>
void CheckReadLock(TSynchronizedVector &my_vector, const size_t expected_size) {
  {
    SCOPED_TRACE("ReadLockPtr");
    auto ptr = my_vector.ReadLock();
    ASSERT_TRUE(test_shared_lock_locked);
    EXPECT_EQ(expected_size, ptr->size());
  }
  ASSERT_FALSE(test_shared_lock_locked);

  {
    auto ptr1 = my_vector.ReadLock();
    ASSERT_TRUE(test_shared_lock_locked);

    {
      test_shared_lock_locked = false;
      auto ptr2 = my_vector.ReadLock();
      EXPECT_EQ(expected_size, ptr1->size());
      EXPECT_EQ(expected_size, ptr2->size());
    }
    test_shared_lock_locked = true;
  }
  ASSERT_FALSE(test_shared_lock_locked);

  {
    SCOPED_TRACE("Indirection operator");
    EXPECT_EQ(expected_size, my_vector->size());
  }
  {
    SCOPED_TRACE("Indirection operator with other ReadLock");

    [[maybe_unused]] auto ptr = my_vector.ReadLock();
    ASSERT_TRUE(test_shared_lock_locked);
    test_shared_lock_locked = false;
    EXPECT_EQ(expected_size, my_vector->size());
    ASSERT_FALSE(test_shared_lock_locked);
    test_shared_lock_locked = true;
  }
  ASSERT_FALSE(test_shared_lock_locked);

  {
    SCOPED_TRACE("Lambda");
    my_vector.WithReadLock([&expected_size](auto &my_vector) {
      ASSERT_TRUE(test_shared_lock_locked);
      EXPECT_EQ(my_vector.size(), expected_size);
    });
    ASSERT_FALSE(test_shared_lock_locked);
  }
  {
    SCOPED_TRACE("Lambda with other ReadLock");
    auto ptr1 = my_vector.ReadLock();
    ASSERT_TRUE(test_shared_lock_locked);

    {
      test_shared_lock_locked = false;
      my_vector.WithReadLock([&expected_size](const auto &my_vector) {
        ASSERT_TRUE(test_shared_lock_locked);
        EXPECT_EQ(my_vector.size(), expected_size);
      });
      ASSERT_FALSE(test_shared_lock_locked);
    }
    test_shared_lock_locked = true;
  }
  ASSERT_FALSE(test_shared_lock_locked);
}

TEST(Synchronized, Usage) {
  utils::Synchronized<std::vector<int>, TestLock> my_vector;
  CheckWriteLock(my_vector);
}

TEST(Synchronized, SharedUsage) {
  utils::Synchronized<std::vector<int>, TestSharedLock> my_vector;
  CheckWriteLock(my_vector);
  {
    SCOPED_TRACE("Non const reference");
    ASSERT_NO_FATAL_FAILURE(CheckReadLock(my_vector, 2));
  }
  {
    const utils::Synchronized<std::vector<int>, TestSharedLock> &my_const_vector = my_vector;
    SCOPED_TRACE("Const reference");
    ASSERT_NO_FATAL_FAILURE(CheckReadLock(my_const_vector, 2));
  }
}