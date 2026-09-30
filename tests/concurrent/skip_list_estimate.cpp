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

// Reads the range estimators while the list is being inserted into and removed from. An estimate
// has no exact answer to check against under concurrent modification, so what is asserted is that
// the walk stays inside the list: it terminates, and it never reports more items than the list has
// ever held. Bounds that match no element are the point of the exercise, since those walk from a
// predecessor the seek found rather than from a node that is known to be present.

#include <atomic>
#include <cstdint>
#include <random>
#include <thread>
#include <vector>

#include "utils/bound.hpp"
#include "utils/logging.hpp"
#include "utils/skip_list.hpp"

const int kNumThreadsInsert = 4;
const int kNumThreadsRemove = 2;
const int kNumThreadsEstimate = 4;

// Only even keys are ever inserted, so every odd bound falls between two keys and matches nothing.
const uint64_t kMaxNum = 200000;

int main() {
  memgraph::utils::SkipList<uint64_t> list;
  std::atomic<bool> run{true};

  std::vector<std::thread> threads_modify;
  std::vector<std::thread> threads_estimate;

  for (int i = 0; i < kNumThreadsInsert; ++i) {
    threads_modify.emplace_back([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        acc.insert(num * 2);
      }
    });
  }
  for (int i = 0; i < kNumThreadsRemove; ++i) {
    threads_modify.emplace_back([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        acc.remove(num * 2);
      }
    });
  }

  constexpr uint64_t ceiling = kNumThreadsInsert * kMaxNum * 2;

  for (int i = 0; i < kNumThreadsEstimate; ++i) {
    threads_estimate.emplace_back([&list, &run, i] {
      std::mt19937 gen(7919 + i);
      std::uniform_int_distribution<uint64_t> dist(0, ceiling);
      while (run.load(std::memory_order_relaxed)) {
        auto acc = list.access();
        // Odd, so present in no list state this run can reach.
        auto const absent_lower = dist(gen) | 1U;
        auto const absent_upper = dist(gen) | 1U;

        auto const open_above =
            acc.estimate_range_count<uint64_t>({{absent_lower, memgraph::utils::BoundType::INCLUSIVE}}, std::nullopt);
        MG_ASSERT(open_above <= ceiling, "Range count {} exceeds every key the list can hold", open_above);

        auto const exclusive =
            acc.estimate_range_count<uint64_t>({{absent_lower, memgraph::utils::BoundType::EXCLUSIVE}}, std::nullopt);
        MG_ASSERT(exclusive <= ceiling, "Range count {} exceeds every key the list can hold", exclusive);

        if (absent_lower <= absent_upper) {
          auto const bounded =
              acc.estimate_range_count<uint64_t>({{absent_lower, memgraph::utils::BoundType::INCLUSIVE}},
                                                 {{absent_upper, memgraph::utils::BoundType::INCLUSIVE}});
          MG_ASSERT(bounded <= ceiling, "Range count {} exceeds every key the list can hold", bounded);
        }

        auto const whole = acc.estimate_range_count<uint64_t>(std::nullopt, std::nullopt);
        MG_ASSERT(whole <= ceiling, "Range count {} exceeds every key the list can hold", whole);
      }
    });
  }

  for (auto &thread : threads_modify) {
    thread.join();
  }
  run.store(false, std::memory_order_relaxed);
  for (auto &thread : threads_estimate) {
    thread.join();
  }

  // With the writers done the count at the exact layer is not an estimate any more. Key 1 is odd,
  // so it is in no list state this run can reach, and every key except 0 sits above it.
  auto acc = list.access();
  auto const expected = acc.size() - (acc.contains(uint64_t{0}) ? 1 : 0);
  auto const from_absent_bound =
      acc.estimate_range_count<uint64_t>({{uint64_t{1}, memgraph::utils::BoundType::INCLUSIVE}}, std::nullopt, 1);
  MG_ASSERT(from_absent_bound == expected,
            "Counting from a bound that matches no key gave {}, {} keys sit above it",
            from_absent_bound,
            expected);

  return 0;
}
