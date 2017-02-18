#include "gtest/gtest.h"

#include "data_structures/bitset/dynamic_bitset.hpp"

TEST(DynamicBitset, BasicFunctionality) {
  DynamicBitset<> db;
  db.set(222555, 1);
  bool value = db.at(222555, 1);
  ASSERT_EQ(value, true);

  db.set(32, 1);
  value = db.at(32, 1);
  ASSERT_EQ(value, true);

  db.clear(32, 1);
  value = db.at(32, 1);
  ASSERT_EQ(value, false);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
