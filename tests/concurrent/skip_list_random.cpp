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
#include <algorithm>
#include <numeric>
#include <ranges>
#include "utils/skip_list.hpp"

constexpr int kNumWriterThreads = 8;
constexpr int kNumDeleterThreads = kNumWriterThreads - 1;
constexpr int kNumReaderThreads = 8;
constexpr int kBlockSize = 8192;
constexpr int kDataSetSize = kNumWriterThreads * kBlockSize;

namespace stdr = std::ranges;

TEST(SkipList, IsDurableWhilstHandlingConcurrentReadsDeletesAndWrites) {
  memgraph::utils::SkipList<uint64_t> skiplist;

  std::random_device rd;
  std::mt19937 rng{rd()};

  std::vector<uint64_t> dataset;
  dataset.resize(kDataSetSize);
  std::iota(dataset.begin(), std::next(dataset.begin(), kDataSetSize), 0);
  stdr::shuffle(dataset, rng);

  {
    auto b{std::next(dataset.cbegin(), kDataSetSize - kBlockSize)};
    auto e{dataset.cend()};
    auto accessor = skiplist.access();
    for (; b != e; ++b) {
      accessor.insert(*b);
    }
  }

  std::vector<std::thread> active_threads;
  for (int i = 0; i < kNumWriterThreads - 1; ++i) {
    active_threads.emplace_back([&, offset = i * kBlockSize]() {
      auto b{std::next(dataset.cbegin(), offset)};
      auto e{std::next(b, kBlockSize)};
      for (; b != e; ++b) {
        auto accessor = skiplist.access();
        accessor.insert(*b);
      }
    });
  }

  for (int i = 0; i < kNumDeleterThreads; ++i) {
    active_threads.emplace_back([&, offset = i * kBlockSize]() {
      std::vector<uint64_t> to_delete(std::next(dataset.cbegin(), offset),
                                      std::next(dataset.cbegin(), offset + kBlockSize));
      std::random_device rd;
      stdr::shuffle(to_delete, std::mt19937{rd()});

      auto b = to_delete.begin();
      auto e = to_delete.end();
      while (b != e) {
        auto new_end = std::remove_if(to_delete.begin(), to_delete.end(), [&](uint64_t const value) {
          auto accessor = skiplist.access();
          return accessor.remove(value);
        });
        to_delete.erase(new_end, e);
        e = new_end;
      }
    });
  }

  std::vector<std::thread> passive_threads;
  std::atomic<bool> stop_passive_threads{false};
  for (int i = 0; i < kNumReaderThreads; ++i) {
    passive_threads.emplace_back([&]() {
      std::random_device rd;
      std::vector<uint64_t> to_read(dataset.cend() - kBlockSize, dataset.cend());
      stdr::shuffle(to_read, std::mt19937{rd()});

      while (!stop_passive_threads.load()) {
        for (uint64_t each : to_read) {
          using namespace std::string_literals;
          SCOPED_TRACE("Reading "s + std::to_string(each));
          auto accessor = skiplist.access();
          auto it = accessor.find(each);
          ASSERT_NE(it, accessor.cend());
        }
      }
    });
  }

  for (auto &thread : active_threads) {
    thread.join();
  }
  stop_passive_threads.store(true);
  for (auto &thread : passive_threads) {
    thread.join();
  }

  ASSERT_EQ(skiplist.size(), kBlockSize);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
