// Copyright 2023 Memgraph Ltd.
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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "utils/math.hpp"
#include "utils/skip_list.hpp"
#include "utils/timer.hpp"

TEST(SkipList, Int) {
  memgraph::utils::SkipList<int64_t> list;
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      auto res = acc.insert(i);
      ASSERT_EQ(*res.first, i);
      ASSERT_TRUE(res.second);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      auto res = acc.insert(i);
      ASSERT_EQ(*res.first, i);
      ASSERT_FALSE(res.second);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    int64_t val = -10;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 11);
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      auto it = acc.find(i);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(*it, i);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      ASSERT_TRUE(acc.remove(i));
    }
    ASSERT_EQ(acc.size(), 0);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      ASSERT_FALSE(acc.remove(i));
    }
    ASSERT_EQ(acc.size(), 0);
  }
}

TEST(SkipList, String) {
  memgraph::utils::SkipList<std::string> list;
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto res = acc.insert(str);
      ASSERT_EQ(*res.first, str);
      ASSERT_TRUE(res.second);
      ASSERT_NE(str, "");
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto res = acc.insert(str);
      ASSERT_EQ(*res.first, str);
      ASSERT_FALSE(res.second);
      ASSERT_NE(str, "");
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    int64_t pos = 0;
    std::vector<int64_t> order{-1, -10, -2, -3, -4, -5, -6, -7, -8, -9, 0, 1, 10, 2, 3, 4, 5, 6, 7, 8, 9};
    for (auto &item : acc) {
      std::string str(fmt::format("str{}", order[pos]));
      ASSERT_EQ(item, str);
      ++pos;
    }
    ASSERT_EQ(pos, 21);
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto it = acc.find(str);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(*it, str);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      ASSERT_TRUE(acc.remove(str));
    }
    ASSERT_EQ(acc.size(), 0);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      ASSERT_FALSE(acc.remove(str));
    }
    ASSERT_EQ(acc.size(), 0);
  }
}

TEST(SkipList, StringMove) {
  memgraph::utils::SkipList<std::string> list;
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      std::string copy(str);
      auto res = acc.insert(std::move(str));
      ASSERT_EQ(str, "");
      ASSERT_EQ(*res.first, copy);
      ASSERT_TRUE(res.second);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto res = acc.insert(str);
      ASSERT_EQ(*res.first, str);
      ASSERT_FALSE(res.second);
      ASSERT_NE(str, "");
    }
    ASSERT_EQ(acc.size(), 21);
  }
}

TEST(SkipList, Basic) {
  memgraph::utils::SkipList<uint64_t> list;

  auto acc = list.access();

  auto insert_item = [&acc](auto item, bool inserted) {
    auto ret = acc.insert(item);
    ASSERT_NE(ret.first, acc.end());
    ASSERT_EQ(*ret.first, item);
    ASSERT_EQ(ret.second, inserted);
  };

  auto find_item = [&acc](auto item, bool found) {
    auto ret = acc.find(item);
    if (found) {
      ASSERT_NE(ret, acc.end());
      ASSERT_EQ(*ret, item);
    } else {
      ASSERT_EQ(ret, acc.end());
    }
  };

  ASSERT_FALSE(acc.contains(5));
  insert_item(5, true);
  insert_item(1, true);
  insert_item(2, true);
  insert_item(3, true);
  insert_item(4, true);
  insert_item(5, false);
  find_item(5, true);
  find_item(6, false);
  ASSERT_TRUE(acc.remove(5));
  ASSERT_FALSE(acc.remove(5));
  ASSERT_FALSE(acc.remove(6));
  ASSERT_EQ(acc.size(), 4);
}

struct OnlyCopyable {
  OnlyCopyable() = default;
  OnlyCopyable(OnlyCopyable &&) = delete;
  OnlyCopyable(const OnlyCopyable &) = default;
  OnlyCopyable &operator=(OnlyCopyable &&) = delete;
  OnlyCopyable &operator=(const OnlyCopyable &) = default;

  OnlyCopyable(const uint64_t val) : value{val} {}
  uint64_t value;
};

bool operator==(const OnlyCopyable &a, const OnlyCopyable &b) { return a.value == b.value; }
bool operator<(const OnlyCopyable &a, const OnlyCopyable &b) { return a.value < b.value; }

TEST(SkipList, OnlyCopyable) {
  memgraph::utils::SkipList<OnlyCopyable> list;
  std::vector<OnlyCopyable> vec{{1}, {2}, {3}, {4}, {5}};
  auto acc = list.access();
  auto ret = acc.insert(vec[1]);
  ASSERT_NE(ret.first, acc.end());
  ASSERT_EQ(*ret.first, OnlyCopyable{2});
  ASSERT_TRUE(ret.second);
}

struct OnlyMoveable {
  OnlyMoveable() = default;
  OnlyMoveable(uint64_t val) : value(val) {}
  OnlyMoveable(OnlyMoveable &&) = default;
  OnlyMoveable(const OnlyMoveable &) = delete;
  OnlyMoveable &operator=(OnlyMoveable &&) = default;
  OnlyMoveable &operator=(const OnlyMoveable &) = delete;
  uint64_t value;
};

bool operator==(const OnlyMoveable &a, const OnlyMoveable &b) { return a.value == b.value; }
bool operator<(const OnlyMoveable &a, const OnlyMoveable &b) { return a.value < b.value; }

TEST(SkipList, OnlyMoveable) {
  memgraph::utils::SkipList<OnlyMoveable> list;
  std::vector<OnlyMoveable> vec;
  vec.emplace_back(1);
  vec.emplace_back(2);
  auto acc = list.access();
  auto ret = acc.insert(std::move(vec[1]));
  ASSERT_NE(ret.first, acc.end());
  ASSERT_EQ(*ret.first, OnlyMoveable{2});
  ASSERT_TRUE(ret.second);
}

TEST(SkipList, Const) {
  memgraph::utils::SkipList<uint64_t> list;

  auto func = [](const memgraph::utils::SkipList<uint64_t> &lst) {
    auto acc = lst.access();
    return acc.find(5);
  };

  auto acc = list.access();

  MG_ASSERT(func(list) == acc.end());
}

struct MapObject {
  uint64_t key;
  std::string value;
};

bool operator==(const MapObject &a, const MapObject &b) { return a.key == b.key; }
bool operator<(const MapObject &a, const MapObject &b) { return a.key < b.key; }

bool operator==(const MapObject &a, const uint64_t &b) { return a.key == b; }
bool operator<(const MapObject &a, const uint64_t &b) { return a.key < b; }

TEST(SkipList, MapExample) {
  memgraph::utils::SkipList<MapObject> list;
  {
    auto accessor = list.access();

    // Inserts an object into the list.
    ASSERT_TRUE(accessor.insert(MapObject{5, "hello world"}).second);

    // This operation will return an iterator that isn't equal to
    // `accessor.end()`. This is because the comparison operators only use
    // the key field for comparison, the value field is ignored.
    ASSERT_NE(accessor.find(MapObject{5, "this probably isn't desired"}), accessor.end());

    // This will also succeed in removing the object.
    ASSERT_TRUE(accessor.remove(MapObject{5, "not good"}));
  }

  {
    auto accessor = list.access();

    // Inserts an object into the list.
    ASSERT_TRUE(accessor.insert({5, "hello world"}).second);

    // This successfully finds the inserted object.
    ASSERT_NE(accessor.find(5), accessor.end());

    // This successfully removes the inserted object.
    ASSERT_TRUE(accessor.remove(5));
  }
}

TEST(SkipList, Move) {
  memgraph::utils::SkipList<int64_t> list;

  {
    auto acc = list.access();
    for (int64_t i = -1000; i <= 1000; ++i) {
      acc.insert(i);
    }
    ASSERT_EQ(acc.size(), 2001);
  }

  {
    auto acc = list.access();
    int64_t val = -1000;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 1001);
    ASSERT_EQ(acc.size(), 2001);
  }

  memgraph::utils::SkipList<int64_t> moved(std::move(list));

  {
    auto acc = moved.access();
    int64_t val = -1000;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 1001);
    ASSERT_EQ(acc.size(), 2001);
  }

  {
    auto acc = list.access();
    ASSERT_DEATH(acc.insert(5), "");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(SkipList, Clear) {
  memgraph::utils::SkipList<int64_t> list;

  {
    auto acc = list.access();
    for (int64_t i = -1000; i <= 1000; ++i) {
      acc.insert(i);
    }
    ASSERT_EQ(acc.size(), 2001);
  }

  {
    auto acc = list.access();
    int64_t val = -1000;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 1001);
    ASSERT_EQ(acc.size(), 2001);
  }

  list.clear();

  {
    auto acc = list.access();
    uint64_t count = 0;
    for (auto it = acc.begin(); it != acc.end(); ++it) {
      ++count;
    }
    ASSERT_EQ(count, 0);
    ASSERT_EQ(acc.size(), 0);
  }

  {
    auto acc = list.access();
    for (int64_t i = -1000; i <= 1000; ++i) {
      acc.insert(i);
    }
    ASSERT_EQ(acc.size(), 2001);
  }

  {
    auto acc = list.access();
    int64_t val = -1000;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 1001);
    ASSERT_EQ(acc.size(), 2001);
  }
}

struct Inception {
  uint64_t id;
  memgraph::utils::SkipList<uint64_t> data;
};
bool operator==(const Inception &a, const Inception &b) { return a.id == b.id; }
bool operator<(const Inception &a, const Inception &b) { return a.id < b.id; }
bool operator==(const Inception &a, const uint64_t &b) { return a.id == b; }
bool operator<(const Inception &a, const uint64_t &b) { return a.id < b; }

TEST(SkipList, Inception) {
  memgraph::utils::SkipList<Inception> list;

  {
    for (uint64_t i = 0; i < 5; ++i) {
      memgraph::utils::SkipList<uint64_t> inner;
      auto acc_inner = inner.access();
      for (uint64_t j = 0; j < 100; ++j) {
        acc_inner.insert(j + 1000 * i);
      }
      ASSERT_EQ(acc_inner.size(), 100);
      auto acc = list.access();
      acc.insert(Inception{i, std::move(inner)});
      auto dead = inner.access();
      ASSERT_DEATH(dead.insert(5), "");
    }
  }

  {
    ASSERT_EQ(list.size(), 5);
    for (uint64_t i = 0; i < 5; ++i) {
      auto acc = list.access();
      auto it = acc.find(i);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(it->id, i);
      auto acc_inner = it->data.access();
      ASSERT_EQ(acc_inner.size(), 100);
      for (uint64_t j = 0; j < 100; ++j) {
        auto it_inner = acc_inner.find(j + 1000 * i);
        ASSERT_NE(it_inner, acc_inner.end());
        ASSERT_EQ(*it_inner, j + 1000 * i);
      }
    }
  }
}

TEST(SkipList, FindEqualOrGreater) {
  memgraph::utils::SkipList<uint64_t> list;

  {
    auto acc = list.access();
    for (uint64_t i = 1000; i < 2000; i += 2) {
      auto ret = acc.insert(i);
      ASSERT_NE(ret.first, acc.end());
      ASSERT_EQ(*ret.first, i);
      ASSERT_TRUE(ret.second);
    }
  }

  {
    auto acc = list.access();
    for (uint64_t i = 0; i < 1000; ++i) {
      auto it = acc.find_equal_or_greater(i);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(*it, 1000);
    }
    for (uint64_t i = 1000; i < 1999; ++i) {
      auto it = acc.find_equal_or_greater(i);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(*it, i + (i % 2 == 0 ? 0 : 1));
    }
    for (uint64_t i = 1999; i < 3000; ++i) {
      auto it = acc.find_equal_or_greater(i);
      ASSERT_EQ(it, acc.end());
    }
  }
}

struct Counter {
  int64_t key;
  int64_t value;
};

bool operator==(const Counter &a, const Counter &b) { return a.key == b.key && a.value == b.value; }
bool operator<(const Counter &a, const Counter &b) {
  if (a.key == b.key) return a.value < b.value;
  return a.key < b.key;
}
bool operator==(const Counter &a, int64_t b) { return a.key == b; }
bool operator<(const Counter &a, int64_t b) { return a.key < b; }

TEST(SkipList, EstimateCount) {
  memgraph::utils::SkipList<Counter> list;

  // 100k elements will yield an expected maximum height of 17
  const int kMaxElements = 100;
  const int kElementMembers = 1000;

  {
    auto acc = list.access();
    for (int64_t i = 0; i < kMaxElements; ++i) {
      for (int64_t j = 0; j < kElementMembers; ++j) {
        auto ret = acc.insert({i, j});
        ASSERT_NE(ret.first, acc.end());
        ASSERT_EQ(ret.first->key, i);
        ASSERT_EQ(ret.first->value, j);
        ASSERT_TRUE(ret.second);
      }
    }
  }

  {
    uint64_t delta_min = std::numeric_limits<uint64_t>::max(), delta_max = 0, delta_avg = 0;
    auto acc = list.access();
    memgraph::utils::Timer timer;
    for (int64_t i = 0; i < kMaxElements; ++i) {
      uint64_t count = acc.estimate_count(i);
      uint64_t delta = count >= kElementMembers ? count - kElementMembers : kElementMembers - count;
      delta_min = std::min(delta_min, delta);
      delta_max = std::max(delta_max, delta);
      delta_avg += delta;
    }
    auto duration = timer.Elapsed().count();

    delta_avg /= kMaxElements;

    std::cout << "Results for estimation from default layer:" << std::endl;
    std::cout << "    min(delta) = " << delta_min << std::endl;
    std::cout << "    avg(delta) = " << delta_avg << std::endl;
    std::cout << "    max(delta) = " << delta_max << std::endl;
    std::cout << "    duration   = " << duration << " s" << std::endl;
  }

  {
    auto acc = list.access();
    for (int64_t i = 0; i < kMaxElements; ++i) {
      uint64_t count = acc.estimate_count(i, 1);
      ASSERT_EQ(count, kElementMembers);
    }
  }
}

#define MAKE_RANGE_BOTH_DEFINED_TEST(lower, upper)                                                       \
  {                                                                                                      \
    for (int64_t i = 0; i < 10; ++i) {                                                                   \
      for (int64_t j = 0; j < 10; ++j) {                                                                 \
        auto acc = list.access();                                                                        \
        uint64_t blocks = 0;                                                                             \
        if (memgraph::utils::BoundType::lower == memgraph::utils::BoundType::EXCLUSIVE &&                \
            memgraph::utils::BoundType::upper == memgraph::utils::BoundType::EXCLUSIVE) {                \
          if (j > i) {                                                                                   \
            blocks = j - i - 1;                                                                          \
          }                                                                                              \
        } else {                                                                                         \
          if (j >= i) {                                                                                  \
            blocks = j - i;                                                                              \
            if (memgraph::utils::BoundType::lower == memgraph::utils::BoundType::INCLUSIVE &&            \
                memgraph::utils::BoundType::upper == memgraph::utils::BoundType::INCLUSIVE) {            \
              ++blocks;                                                                                  \
            }                                                                                            \
          }                                                                                              \
        }                                                                                                \
        uint64_t count = acc.estimate_range_count<int64_t>({{i, memgraph::utils::BoundType::lower}},     \
                                                           {{j, memgraph::utils::BoundType::upper}}, 1); \
        ASSERT_EQ(count, kElementMembers *blocks);                                                       \
      }                                                                                                  \
    }                                                                                                    \
  }

#define MAKE_RANGE_LOWER_INFINITY_TEST(upper_value, upper_type, blocks)                                              \
  {                                                                                                                  \
    auto acc = list.access();                                                                                        \
    uint64_t count =                                                                                                 \
        acc.estimate_range_count<int64_t>(std::nullopt, {{upper_value, memgraph::utils::BoundType::upper_type}}, 1); \
    ASSERT_EQ(count, kElementMembers *blocks);                                                                       \
  }

#define MAKE_RANGE_UPPER_INFINITY_TEST(lower_value, lower_type, blocks)                                              \
  {                                                                                                                  \
    auto acc = list.access();                                                                                        \
    uint64_t count =                                                                                                 \
        acc.estimate_range_count<int64_t>({{lower_value, memgraph::utils::BoundType::lower_type}}, std::nullopt, 1); \
    ASSERT_EQ(count, kElementMembers *blocks);                                                                       \
  }

TEST(SkipList, EstimateRangeCount) {
  memgraph::utils::SkipList<Counter> list;

  // 100k elements will yield an expected maximum height of 17
  const int kMaxElements = 100;
  const int kElementMembers = 1000;

  {
    auto acc = list.access();
    for (int64_t i = 0; i < kMaxElements; ++i) {
      for (int64_t j = 0; j < kElementMembers; ++j) {
        auto ret = acc.insert({i, j});
        ASSERT_NE(ret.first, acc.end());
        ASSERT_EQ(ret.first->key, i);
        ASSERT_EQ(ret.first->value, j);
        ASSERT_TRUE(ret.second);
      }
    }
  }

  {
    uint64_t delta_min = std::numeric_limits<uint64_t>::max(), delta_max = 0, delta_avg = 0;
    auto acc = list.access();
    memgraph::utils::Timer timer;
    for (int64_t i = 0; i < kMaxElements; ++i) {
      uint64_t count = acc.estimate_range_count<int64_t>(std::nullopt, {{i, memgraph::utils::BoundType::INCLUSIVE}});
      uint64_t must_have = kElementMembers * (i + 1);
      uint64_t delta = count >= must_have ? count - must_have : must_have - count;
      delta_min = std::min(delta_min, delta);
      delta_max = std::max(delta_max, delta);
      delta_avg += delta;
    }
    auto duration = timer.Elapsed().count();

    delta_avg /= kMaxElements;

    std::cout << "Results for estimation from default layer:" << std::endl;
    std::cout << "    min(delta) = " << delta_min << std::endl;
    std::cout << "    avg(delta) = " << delta_avg << std::endl;
    std::cout << "    max(delta) = " << delta_max << std::endl;
    std::cout << "    duration   = " << duration << " s" << std::endl;
  }

  MAKE_RANGE_BOTH_DEFINED_TEST(INCLUSIVE, INCLUSIVE);
  MAKE_RANGE_BOTH_DEFINED_TEST(INCLUSIVE, EXCLUSIVE);
  MAKE_RANGE_BOTH_DEFINED_TEST(EXCLUSIVE, INCLUSIVE);
  MAKE_RANGE_BOTH_DEFINED_TEST(EXCLUSIVE, EXCLUSIVE);

  MAKE_RANGE_LOWER_INFINITY_TEST(10, INCLUSIVE, 11);
  MAKE_RANGE_LOWER_INFINITY_TEST(10, EXCLUSIVE, 10);
  MAKE_RANGE_LOWER_INFINITY_TEST(0, INCLUSIVE, 1);
  MAKE_RANGE_LOWER_INFINITY_TEST(0, EXCLUSIVE, 0);
  MAKE_RANGE_LOWER_INFINITY_TEST(-10, INCLUSIVE, 0);
  MAKE_RANGE_LOWER_INFINITY_TEST(-10, EXCLUSIVE, 0);

  MAKE_RANGE_UPPER_INFINITY_TEST(89, INCLUSIVE, 11);
  MAKE_RANGE_UPPER_INFINITY_TEST(89, EXCLUSIVE, 10);
  MAKE_RANGE_UPPER_INFINITY_TEST(99, INCLUSIVE, 1);
  MAKE_RANGE_UPPER_INFINITY_TEST(99, EXCLUSIVE, 0);
  MAKE_RANGE_UPPER_INFINITY_TEST(109, INCLUSIVE, 0);
  MAKE_RANGE_UPPER_INFINITY_TEST(109, EXCLUSIVE, 0);

  {
    auto acc = list.access();
    uint64_t count = acc.estimate_range_count<int64_t>(std::nullopt, std::nullopt, 1);
    ASSERT_EQ(count, kMaxElements * kElementMembers);
  }
}

template <typename TElem, typename TCmp>
void BenchmarkEstimateAverageNumberOfEquals(memgraph::utils::SkipList<TElem> *list, const TCmp &cmp) {
  std::cout << "List size: " << list->size() << std::endl;
  std::cout << "The index will use layer " << memgraph::utils::SkipListLayerForAverageEqualsEstimation(list->size())
            << std::endl;
  auto acc = list->access();
  for (int layer = 1; layer <= memgraph::utils::kSkipListMaxHeight; ++layer) {
    memgraph::utils::Timer timer;
    auto estimate = acc.estimate_average_number_of_equals(cmp, layer);
    auto duration = timer.Elapsed().count();
    std::cout << "Estimate on layer " << layer << " is " << estimate << " in " << duration << std::endl;
  }
}

TEST(SkipList, EstimateAverageNumberOfEquals1) {
  memgraph::utils::SkipList<Counter> list;

  // ~500k elements will yield an expected maximum height of 19.
  const int kMaxElements = 1000;

  // Create a list that has 1, then 2, then 3, then 4, ..., up to
  // `kMaxElements` same keys next to each other.
  {
    auto acc = list.access();
    for (int64_t i = 1; i <= kMaxElements; ++i) {
      for (int64_t j = 1; j <= i; ++j) {
        auto ret = acc.insert({i, j});
        ASSERT_NE(ret.first, acc.end());
        ASSERT_EQ(ret.first->key, i);
        ASSERT_EQ(ret.first->value, j);
        ASSERT_TRUE(ret.second);
      }
    }
  }

  // There are `kMaxElements * (kMaxElements + 1) / 2` members in the list.
  ASSERT_EQ(list.size(), kMaxElements * (kMaxElements + 1) / 2);

  // Benchmark the estimation function.
  BenchmarkEstimateAverageNumberOfEquals(&list, [](const auto &a, const auto &b) { return a.key == b.key; });

  // Verify that the estimate on the lowest layer is correct.
  {
    auto acc = list.access();
    uint64_t count =
        acc.estimate_average_number_of_equals([](const auto &a, const auto &b) { return a.key == b.key; }, 1);
    // There are `kMaxElements` unique elements when observing the data with
    // the specified equation operator so we divide the number of elements with
    // `kMaxElements`.
    ASSERT_EQ(list.size(), kMaxElements * (kMaxElements + 1) / 2);
    ASSERT_EQ(count, (kMaxElements + 1) / 2);
  }
}

TEST(SkipList, EstimateAverageNumberOfEquals2) {
  memgraph::utils::SkipList<Counter> list;

  // 100k elements will yield an expected maximum height of 17.
  const int kMaxElements = 100000;
  const int kElementMembers = 1;

  // Create a list that has `kMaxElements` sets of `kElementMembers` items that
  // have same keys.
  {
    auto acc = list.access();
    for (int64_t i = 0; i < kMaxElements; ++i) {
      for (int64_t j = 0; j < kElementMembers; ++j) {
        auto ret = acc.insert({i, j});
        ASSERT_NE(ret.first, acc.end());
        ASSERT_EQ(ret.first->key, i);
        ASSERT_EQ(ret.first->value, j);
        ASSERT_TRUE(ret.second);
      }
    }
  }

  // There are `kMaxElements * kElementMembers` members in the list.
  ASSERT_EQ(list.size(), kMaxElements * kElementMembers);

  // Benchmark the estimation function.
  BenchmarkEstimateAverageNumberOfEquals(&list, [](const auto &a, const auto &b) { return a.key == b.key; });

  // Verify that the estimate on the lowest layer is correct.
  {
    auto acc = list.access();
    uint64_t count =
        acc.estimate_average_number_of_equals([](const auto &a, const auto &b) { return a.key == b.key; }, 1);
    ASSERT_EQ(count, kElementMembers);
  }
}

TEST(SkipList, EstimateAverageNumberOfEquals3) {
  memgraph::utils::SkipList<Counter> list;

  // 100k elements will yield an expected maximum height of 17
  const int kMaxElements = 100;
  const int kElementMembers = 1000;

  // Create a list that has `kMaxElements` sets of `kElementMembers` items that
  // have same keys.
  {
    auto acc = list.access();
    for (int64_t i = 0; i < kMaxElements; ++i) {
      for (int64_t j = 0; j < kElementMembers; ++j) {
        auto ret = acc.insert({i, j});
        ASSERT_NE(ret.first, acc.end());
        ASSERT_EQ(ret.first->key, i);
        ASSERT_EQ(ret.first->value, j);
        ASSERT_TRUE(ret.second);
      }
    }
  }

  // There are `kMaxElements * kElementMembers` members in the list.
  ASSERT_EQ(list.size(), kMaxElements * kElementMembers);

  // Benchmark the estimation function.
  BenchmarkEstimateAverageNumberOfEquals(&list, [](const auto &a, const auto &b) { return a.key == b.key; });

  // Verify that the estimate on the lowest layer is correct.
  {
    auto acc = list.access();
    uint64_t count =
        acc.estimate_average_number_of_equals([](const auto &a, const auto &b) { return a.key == b.key; }, 1);
    ASSERT_EQ(count, kElementMembers);
  }
}

TEST(SkipList, EstimateAverageNumberOfEquals4) {
  memgraph::utils::SkipList<Counter> list;

  // ~300k elements will yield an expected maximum height of 18.
  const int kMaxElements = 100000;

  // Create a list that has `kMaxElements` sets of 1 or 3 items that have same
  // keys. The bias is 70% for a set that has 3 items, and 30% for a set that
  // has 1 items.
  std::mt19937 gen{std::random_device{}()};
  std::uniform_real_distribution<> dis(0.0, 1.0);
  {
    auto acc = list.access();
    for (int64_t i = 0; i < kMaxElements; ++i) {
      for (int64_t j = 0; j < (dis(gen) < 0.7 ? 3 : 1); ++j) {
        auto ret = acc.insert({i, j});
        ASSERT_NE(ret.first, acc.end());
        ASSERT_EQ(ret.first->key, i);
        ASSERT_EQ(ret.first->value, j);
        ASSERT_TRUE(ret.second);
      }
    }
  }

  // Benchmark the estimation function.
  BenchmarkEstimateAverageNumberOfEquals(&list, [](const auto &a, const auto &b) { return a.key == b.key; });

  // Verify that the estimate on the lowest layer is correct.
  {
    auto acc = list.access();
    uint64_t count =
        acc.estimate_average_number_of_equals([](const auto &a, const auto &b) { return a.key == b.key; }, 1);
    // Because the test is randomized, the exact estimate on the lowest layer
    // can't be known. But it definitely must be between 1 and 3 because the
    // clusters of items are of sizes 1 and 3.
    ASSERT_GE(count, 1);
    ASSERT_LE(count, 3);
  }
}

TEST(SkipList, EstimateAverageNumberOfEquals5) {
  memgraph::utils::SkipList<Counter> list;

  // ~500k elements will yield an expected maximum height of 19.
  const int kMaxElements = 1000000;

  // Create a list that has `kMaxElements` items that have same keys.
  {
    auto acc = list.access();
    for (int64_t i = 1; i <= kMaxElements; ++i) {
      auto ret = acc.insert({1, i});
      ASSERT_NE(ret.first, acc.end());
      ASSERT_EQ(ret.first->key, 1);
      ASSERT_EQ(ret.first->value, i);
      ASSERT_TRUE(ret.second);
    }
  }

  // There are `kMaxElements` members in the list.
  ASSERT_EQ(list.size(), kMaxElements);

  // Benchmark the estimation function.
  BenchmarkEstimateAverageNumberOfEquals(&list, [](const auto &a, const auto &b) { return a.key == b.key; });

  // Verify that the estimate on the lowest layer is correct.
  {
    auto acc = list.access();
    uint64_t count =
        acc.estimate_average_number_of_equals([](const auto &a, const auto &b) { return a.key == b.key; }, 1);
    ASSERT_EQ(count, kMaxElements);
  }
}
