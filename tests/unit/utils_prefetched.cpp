// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "utils/prefetched.hpp"

using memgraph::utils::Prefetched;

namespace {
auto Drain(Prefetched<int> &source) -> std::vector<int> {
  std::vector<int> seen;
  int item = 0;
  while (source.Next(item)) {
    seen.push_back(item);
  }
  return seen;
}
}  // namespace

TEST(PrefetchedTest, DeliversEveryItemInOrder) {
  Prefetched<int> source{4, [](auto const &push) {
                           for (auto i = 1; i <= 200; ++i) {
                             if (!push(i)) return;
                           }
                         }};

  auto const seen = Drain(source);

  ASSERT_EQ(seen.size(), 200U);
  for (auto i = 0U; i < seen.size(); ++i) {
    ASSERT_EQ(seen[i], static_cast<int>(i) + 1);
  }
}

TEST(PrefetchedTest, AProducerThatDeliversNothingIsSpentImmediately) {
  Prefetched<int> source{4, [](auto const & /*push*/) {}};

  EXPECT_TRUE(Drain(source).empty());
}

TEST(PrefetchedTest, WhatTheProducerThrowsReachesTheConsumer) {
  Prefetched<int> source{4, [](auto const &push) {
                           push(1);
                           throw std::runtime_error{"produced badly"};
                         }};

  int item = 0;
  // Whatever was produced before the failure is still handed over first.
  ASSERT_TRUE(source.Next(item));
  EXPECT_EQ(item, 1);

  EXPECT_THROW(
      {
        while (source.Next(item)) {
        }
      },
      std::runtime_error);
}

TEST(PrefetchedTest, TheProducerIsToldToStopWhenTheConsumerGivesUp) {
  std::atomic<bool> told_to_stop{false};
  std::atomic<int> produced{0};

  {
    Prefetched<int> source{2, [&](auto const &push) {
                             for (auto i = 0; i < 1'000'000; ++i) {
                               produced.fetch_add(1, std::memory_order_relaxed);
                               if (!push(i)) {
                                 told_to_stop.store(true, std::memory_order_release);
                                 return;
                               }
                             }
                           }};

    int item = 0;
    ASSERT_TRUE(source.Next(item));
    // Destroyed here, having consumed one item out of a million.
  }

  EXPECT_TRUE(told_to_stop.load(std::memory_order_acquire))
      << "the producer should learn to stop through the value push returns";
  EXPECT_LT(produced.load(std::memory_order_relaxed), 1'000'000) << "destruction should not wait for the whole source";
}

TEST(PrefetchedTest, TheQueueDepthBoundsWhatIsProducedAhead) {
  constexpr auto kDepth = 2;
  std::atomic<int> produced{0};

  Prefetched<int> source{kDepth, [&](auto const &push) {
                           for (auto i = 0; i < 100; ++i) {
                             produced.fetch_add(1, std::memory_order_relaxed);
                             if (!push(i)) return;
                           }
                         }};

  // Give the producer every chance to run ahead before anything is consumed.
  for (auto attempt = 0; attempt < 50 && produced.load(std::memory_order_relaxed) <= kDepth; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
  }

  // It may sit one item beyond the queue, in the push that is blocking.
  EXPECT_LE(produced.load(std::memory_order_relaxed), kDepth + 1)
      << "a bounded queue is what keeps a fast producer from running away";

  int item = 0;
  while (source.Next(item)) {
  }
}

TEST(PrefetchedTest, TryNextTakesOnlyWhatHasAlreadyArrived) {
  std::atomic<bool> release{false};
  Prefetched<int> source{4, [&](auto const &push) {
                           push(1);
                           while (!release.load(std::memory_order_acquire)) {
                             std::this_thread::sleep_for(std::chrono::milliseconds{1});
                           }
                           push(2);
                         }};

  int item = 0;
  // The first item is worth waiting for; after that the consumer keeps going rather than blocking on
  // a producer that has nothing ready.
  ASSERT_TRUE(source.Next(item));
  EXPECT_EQ(item, 1);
  EXPECT_FALSE(source.TryNext(item)) << "nothing has arrived yet, so this must not wait";

  release.store(true, std::memory_order_release);
  ASSERT_TRUE(source.Next(item));
  EXPECT_EQ(item, 2);
}

TEST(PrefetchedTest, AMoveOnlyItemSurvivesTheQueue) {
  Prefetched<std::string> source{2, [](auto const &push) {
                                   push(std::string(1000, 'x'));
                                   push(std::string(1000, 'y'));
                                 }};

  std::string item;
  ASSERT_TRUE(source.Next(item));
  EXPECT_EQ(item, std::string(1000, 'x'));
  ASSERT_TRUE(source.Next(item));
  EXPECT_EQ(item, std::string(1000, 'y'));
  EXPECT_FALSE(source.Next(item));
}
