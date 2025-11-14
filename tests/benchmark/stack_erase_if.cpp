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

#include <benchmark/benchmark.h>
#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include "utils/stack.hpp"

using TDeleted = std::pair<uint64_t, int *>;
using TLocalStack = memgraph::utils::Stack<TDeleted, 8191, false>;

namespace {
void DeleteNode(const int *node) { delete node; }
}  // namespace

class StackEraseIfFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index() == 0) {
      original_elements_.clear();
      const auto num_elements = static_cast<int>(state.range(0));
      std::vector<int> indices(num_elements);
      std::ranges::iota(indices, 0);

      // Shuffle indices to randomize order
      std::mt19937 gen(42);
      std::ranges::shuffle(indices, gen);

      for (int idx : indices) {
        auto *node = new int(idx);
        original_elements_.emplace_back(static_cast<uint64_t>(idx), node);
      }
    }
  }

  void TearDown(const benchmark::State &state) override {
    if (state.thread_index() == 0) {
      for (auto &elem : original_elements_) {
        DeleteNode(elem.second);
      }
      original_elements_.clear();
      std::optional<TDeleted> item;
      while ((item = stack.Pop())) {
        DeleteNode(item->second);
      }
    }
  }

  void RestoreStack() {
    std::optional<TDeleted> item;
    while ((item = stack.Pop())) {
      DeleteNode(item->second);
    }
    for (auto it = original_elements_.rbegin(); it != original_elements_.rend(); ++it) {
      auto *node = new int(*it->second);
      stack.Push(std::make_pair(it->first, node));
    }
  }

  static uint64_t GetLastDead(uint64_t num_elements, double ratio) {
    return static_cast<uint64_t>(static_cast<double>(num_elements) * ratio);
  }

  TLocalStack stack;
  std::vector<TDeleted> original_elements_;
};

// Manual deletion approach: Pop, check, push to leftover stack
BENCHMARK_DEFINE_F(StackEraseIfFixture, ManualDeletionApproach)(benchmark::State &state) {
  const auto num_elements = static_cast<uint64_t>(state.range(0));
  const auto ratio = static_cast<double>(state.range(1)) / 100.0;
  const auto last_dead = GetLastDead(num_elements, ratio);

  while (state.KeepRunning()) {
    state.PauseTiming();
    RestoreStack();
    state.ResumeTiming();

    TLocalStack leftover;
    std::optional<TDeleted> item;
    while ((item = stack.Pop())) {
      if (item->first < last_dead) {
        DeleteNode(item->second);
      } else {
        leftover.Push(std::move(*item));
      }
    }
    stack = std::move(leftover);
  }
}

// EraseIf approach: EraseIf with deleter
BENCHMARK_DEFINE_F(StackEraseIfFixture, EraseIfApproach)(benchmark::State &state) {
  const auto num_elements = static_cast<uint64_t>(state.range(0));
  const auto ratio = static_cast<double>(state.range(1)) / 100.0;
  const auto last_dead = GetLastDead(num_elements, ratio);

  while (state.KeepRunning()) {
    state.PauseTiming();
    RestoreStack();
    state.ResumeTiming();

    stack.EraseIf([last_dead](const TDeleted &item) { return item.first < last_dead; },
                  [](const TDeleted &item) { DeleteNode(item.second); });
  }
}

// Args: (num_elements, deletion_percentage)
BENCHMARK_REGISTER_F(StackEraseIfFixture, ManualDeletionApproach)
    ->Args({10000, 25})
    ->Args({10000, 50})
    ->Args({10000, 75})
    ->Args({100000, 25})
    ->Args({100000, 50})
    ->Args({100000, 75})
    ->Args({1000000, 25})
    ->Args({1000000, 50})
    ->Args({1000000, 75})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(StackEraseIfFixture, EraseIfApproach)
    ->Args({10000, 25})
    ->Args({10000, 50})
    ->Args({10000, 75})
    ->Args({100000, 25})
    ->Args({100000, 50})
    ->Args({100000, 75})
    ->Args({1000000, 25})
    ->Args({1000000, 50})
    ->Args({1000000, 75})
    ->Unit(benchmark::kMicrosecond);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
