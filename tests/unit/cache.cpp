#include <memory>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "utils/cache.hpp"

class CacheTest : public ::testing::Test {
 public:
  void SetUp() override {
    for (long i = 0; i < 5; ++i) {
      cache_.Insert(i, to_insert_);
    }
  }

  long to_insert_ = 0;
  utils::LruCache<long, long> cache_{5};
};

TEST_F(CacheTest, InsertTest) {
  // cache is full already
  cache_.Insert(5, to_insert_); // 0 is evicted
  EXPECT_FALSE(cache_.Find(0));
  EXPECT_TRUE(cache_.Find(1));
  EXPECT_TRUE(cache_.Find(2));
  EXPECT_TRUE(cache_.Find(3));
  EXPECT_TRUE(cache_.Find(4));
  EXPECT_TRUE(cache_.Find(5));

  cache_.Insert(6, to_insert_); // 1 is evicted

  EXPECT_FALSE(cache_.Find(0));
  EXPECT_FALSE(cache_.Find(1));
  EXPECT_TRUE(cache_.Find(2));
  EXPECT_TRUE(cache_.Find(3));
  EXPECT_TRUE(cache_.Find(4));
  EXPECT_TRUE(cache_.Find(5));
  EXPECT_TRUE(cache_.Find(6));
}

TEST_F(CacheTest, GetTest) {
  // cache is full already
  // 4 -> 3 -> 2 -> 1 -> 0

  EXPECT_TRUE(cache_.Find(2));
  EXPECT_TRUE(cache_.Find(4));
  EXPECT_TRUE(cache_.Find(0));
  EXPECT_TRUE(cache_.Find(1));
  // order has changed
  // 1 -> 0 -> 4 -> 2 -> 3

  cache_.Insert(10, to_insert_);
  EXPECT_FALSE(cache_.Find(3));

  cache_.Insert(11, to_insert_);
  EXPECT_FALSE(cache_.Find(2));

  cache_.Insert(12, to_insert_);
  EXPECT_FALSE(cache_.Find(4));

  cache_.Insert(13, to_insert_);
  EXPECT_FALSE(cache_.Find(0));

  cache_.Insert(14, to_insert_);
  EXPECT_FALSE(cache_.Find(1));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
