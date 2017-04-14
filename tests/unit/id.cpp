#include "gtest/gtest.h"

#include "mvcc/id.hpp"

TEST(IdTest, BasicUsageAndTotalOrdering) {
  Id id0(0);
  Id id1(1);
  Id id2(1);
  Id id3(id2);
  Id id4 = id3;
  Id id5(5);

  ASSERT_EQ(id0 < id5, true);
  ASSERT_EQ(id1 == id2, true);
  ASSERT_EQ(id3 == id4, true);
  ASSERT_EQ(id5 > id0, true);
  ASSERT_EQ(id5 > id0, true);
  ASSERT_EQ(id5 != id3, true);
  ASSERT_EQ(id1 >= id2, true);
  ASSERT_EQ(id3 <= id4, true);
}

TEST(IdTest, MaxId) {
  EXPECT_TRUE(Id(std::numeric_limits<uint64_t>::max()) == Id::MaximalId());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
