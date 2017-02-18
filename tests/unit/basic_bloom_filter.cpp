#include "gtest/gtest.h"

#include <functional>

#include "data_structures/bloom/bloom_filter.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/hashing/fnv64.hpp"

using StringHashFunction = std::function<uint64_t(const std::string &)>;

TEST(BloomFilterTest, InsertContains) {
  StringHashFunction hash1 = fnv64<std::string>;
  StringHashFunction hash2 = fnv1a64<std::string>;
  std::vector<StringHashFunction> funcs = {hash1, hash2};

  BloomFilter<std::string, 64> bloom(funcs);

  std::string test = "test";
  std::string kifla = "kifla";

  bool contains_test = bloom.contains(test);
  ASSERT_EQ(contains_test, false);
  bloom.insert(test);
  contains_test = bloom.contains(test);
  ASSERT_EQ(contains_test, true);

  bool contains_kifla = bloom.contains(kifla);
  ASSERT_EQ(contains_kifla, false);
  bloom.insert(kifla);
  contains_kifla = bloom.contains(kifla);
  ASSERT_EQ(contains_kifla, true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
