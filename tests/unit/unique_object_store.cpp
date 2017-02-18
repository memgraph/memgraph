#include <stdlib.h>
#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"

#include "storage/unique_object_store.hpp"
#include "utils/total_ordering.hpp"

/**
 * Wraps an int and implements total ordering. Used for testing the
 * UniqueObjectStore with custom classes.
 */
class IntWrapper : TotalOrdering<IntWrapper>, TotalOrdering<IntWrapper, int> {
 public:
  IntWrapper(const int i) : i_(i) {}

  friend bool operator==(const IntWrapper &lhs, const IntWrapper &rhs) {
    return lhs.i_ == rhs.i_;
  }

  friend bool operator<(const IntWrapper &lhs, const IntWrapper &rhs) {
    return lhs.i_ < rhs.i_;
  }

 private:
  const int i_;
};

TEST(UniqueObjectStoreTest, GetKey) {
  UniqueObjectStore<std::string> store;
  EXPECT_EQ(store.GetKey("name1"), store.GetKey("name1"));
  EXPECT_NE(store.GetKey("name1"), store.GetKey("name2"));
  EXPECT_EQ(store.GetKey("name2"), store.GetKey("name2"));
}

TEST(UniqueObjectStoreTest, CustomClass) {
  UniqueObjectStore<IntWrapper> store;
  EXPECT_EQ(store.GetKey(42), store.GetKey(IntWrapper(42)));
  EXPECT_EQ(store.GetKey(1000), store.GetKey(IntWrapper(1000)));
  EXPECT_NE(store.GetKey(IntWrapper(1000)), store.GetKey(IntWrapper(42)));
}

TEST(UniqueObjectStoreTest, RandomAccess) {
  UniqueObjectStore<int> store;
  for (int i = 0; i < 50 * 50 * 4; i++) {
    int rand1 = rand() % 50;
    int rand2 = rand() % 50;
    EXPECT_EQ(store.GetKey(rand1) == store.GetKey(rand2), rand1 == rand2);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
