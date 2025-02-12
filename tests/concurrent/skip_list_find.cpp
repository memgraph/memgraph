// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include "utils/skip_list.hpp"

#include <latch>

uint64_t constexpr KEY_PREFIX = 42;
uint64_t constexpr kNumValues = 10000;

struct Entry {
  Entry(uint64_t first, uint64_t second) : values{first, second} {}

  std::pair<uint64_t, uint64_t> values;

  bool operator==(Entry const &rhs) const { return values == rhs.values; }

  bool operator<(Entry const &rhs) const { return values < rhs.values; }

  bool operator==(int64_t const rhs) const { return values.first == rhs; }

  bool operator<(uint64_t const rhs) const { return values.first < rhs; }
};

TEST(SkipList, FindEqualOrGreaterWorksConcurrently) {
  memgraph::utils::SkipList<Entry> skiplist;

  {
    auto acc{skiplist.access()};
    for (uint64_t i = 0; i < kNumValues; ++i) {
      acc.insert(Entry{KEY_PREFIX, i});
    }
    ASSERT_EQ(skiplist.size(), kNumValues);
  }

  std::stop_source ss;
  auto latch = std::latch{2};

  auto remove_job = [&] {
    latch.arrive_and_wait();

    for (uint64_t i = 0; i < kNumValues - 1; ++i) {
      auto acc{skiplist.access()};
      acc.remove(Entry{KEY_PREFIX, i});
    }

    ss.request_stop();
  };

  auto find_job = [&](std::stop_token st) {
    latch.arrive_and_wait();

    while (!st.stop_requested()) {
      auto acc{skiplist.access()};
      ASSERT_NE(acc.find_equal_or_greater(KEY_PREFIX), acc.cend());
      ASSERT_NE(acc.find(KEY_PREFIX), acc.cend());
      ASSERT_TRUE(acc.contains(KEY_PREFIX));
    }
  };

  {
    std::vector<std::jthread> threads;
    threads.emplace_back(remove_job);
    threads.emplace_back(find_job, ss.get_token());
  }

  ASSERT_EQ(skiplist.size(), 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
