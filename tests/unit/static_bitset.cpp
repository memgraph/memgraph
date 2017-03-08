#include "data_structures/bitset/static_bitset.hpp"
#include <gmock/gmock.h>
#include <vector>
#include "gtest/gtest-spi.h"
#include "gtest/gtest.h"

using testing::UnorderedElementsAreArray;

TEST(StaticBitset, Intersection) {
  const int n = 50;
  Bitset<int64_t> bitset(n);
  Bitset<int64_t> bitset2(n);
  std::vector<int> V;
  std::vector<int> V2;
  for (int i = 0; i < n / 2; ++i) {
    const int pos = rand() % n;
    bitset.Set(pos);
    V.push_back(pos);
  }
  for (int i = 0; i < n / 2; ++i) {
    const int pos = rand() % n;
    bitset2.Set(pos);
    V2.push_back(pos);
  }
  Bitset<int64_t> intersected = bitset.Intersect(bitset);
  sort(V.begin(), V.end());
  V.resize(unique(V.begin(), V.end()) - V.begin());
  EXPECT_THAT(V, UnorderedElementsAreArray(intersected.Ones()));

  sort(V2.begin(), V2.end());
  V2.resize(unique(V2.begin(), V2.end()) - V2.begin());

  std::vector<int> V3;
  set_intersection(V.begin(), V.end(), V2.begin(), V2.end(), back_inserter(V3));
  Bitset<int64_t> intersected_two = bitset.Intersect(bitset2);
  EXPECT_THAT(V3, UnorderedElementsAreArray(intersected_two.Ones()));
}

TEST(StaticBitset, BasicFunctionality) {
  const int n = 50;
  Bitset<int64_t> bitset(n);
  std::vector<int> V;
  for (int i = 0; i < n / 2; ++i) {
    const int pos = rand() % n;
    bitset.Set(pos);
    V.push_back(pos);
  }
  sort(V.begin(), V.end());
  V.resize(unique(V.begin(), V.end()) - V.begin());
  EXPECT_THAT(V, UnorderedElementsAreArray(bitset.Ones()));
}

TEST(StaticBitset, SetAndReadBit) {
  const int n = 50;
  Bitset<char> bitset(n);
  bitset.Set(4);
  EXPECT_EQ(bitset.At(4), true);
  EXPECT_EQ(bitset.At(3), false);
}

TEST(StaticBitset, SetOutOfRange) {
  const int n = 50;
  Bitset<char> bitset(n);
  EXPECT_DEATH(bitset.Set(-1), "Invalid bit location.");
  EXPECT_DEATH(bitset.Set(150), "Invalid bit location.");
  bitset.Set(49);
}

TEST(StaticBitset, AtOutOfRange) {
  const int n = 50;
  Bitset<char> bitset(n);
  bitset.Set(33);
  EXPECT_DEATH(bitset.At(150), "Invalid bit location.");
  EXPECT_DEATH(bitset.At(-1), "Invalid bit location.");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
