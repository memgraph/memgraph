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

#include <benchmark/benchmark.h>
#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "boost/unordered/unordered_flat_set.hpp"

namespace {

// Fixed seed so every container sees the same keys in the same order
constexpr uint64_t kSeed = 42;

constexpr int64_t kMinSize = 1 << 10;
constexpr int64_t kMaxSize = 1 << 22;
constexpr int64_t kSizeMultiplier = 8;

std::vector<uint64_t> RandomInts(int64_t n) {
  std::mt19937_64 rng(kSeed);
  std::vector<uint64_t> keys(n);
  std::ranges::generate(keys, rng);
  return keys;
}

std::vector<std::string> ShuffledStrings(int64_t n, std::string const &prefix = "str") {
  std::vector<std::string> keys;
  keys.reserve(n);
  for (int64_t i = 0; i < n; i++) {
    keys.emplace_back(prefix + std::to_string(i));
  }
  std::ranges::shuffle(keys, std::mt19937_64(kSeed));
  return keys;
}

template <typename K>
std::vector<K> RandomSample(std::vector<K> keys, int64_t n) {
  std::ranges::shuffle(keys, std::mt19937_64(kSeed + 1));
  keys.resize(n);
  return keys;
}

// As many lookups as there are keys; hit_pct of them are present, the rest are absent
std::vector<std::string> LookupKeys(std::vector<std::string> const &present, int64_t hit_pct) {
  auto const n = static_cast<int64_t>(present.size());
  auto const hits = n * hit_pct / 100;
  auto lookups = RandomSample(present, hits);
  auto misses = ShuffledStrings(n - hits, "miss");
  lookups.insert(lookups.end(), misses.begin(), misses.end());
  std::ranges::shuffle(lookups, std::mt19937_64(kSeed + 2));
  return lookups;
}

void SizeArgs(benchmark::Benchmark *b) {
  b->RangeMultiplier(kSizeMultiplier)->Range(kMinSize, kMaxSize)->ArgName("size")->Unit(benchmark::kMicrosecond);
}

void FindArgs(benchmark::Benchmark *b) {
  b->ArgsProduct({benchmark::CreateRange(kMinSize, kMaxSize, kSizeMultiplier), {100, 50, 0}})
      ->ArgNames({"size", "hit_pct"})
      ->Unit(benchmark::kMicrosecond);
}

}  // namespace

template <typename T>
static void BM_FlatHashSetInt64_Insert(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = RandomInts(num_items);
  for (auto _ : state) {
    T cont;
    cont.reserve(num_items);
    for (auto key : keys) {
      cont.insert(key);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_items);
}

template <typename T>
static void BM_FlatHashSetString_Insert(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = ShuffledStrings(num_items);
  for (auto _ : state) {
    T cont;
    cont.reserve(num_items);
    for (const auto &key : keys) {
      cont.insert(key);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_items);
}

// Look up as many keys as the set holds, in random order, with hit_pct of them present
template <typename T>
static void BM_FlatHashSetString_Find(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = ShuffledStrings(num_items);
  T cont(keys.begin(), keys.end());
  auto const lookups = LookupKeys(keys, state.range(1));

  for (auto _ : state) {
    int64_t found = 0;
    for (const auto &key : lookups) {
      found += cont.find(key) != cont.end();
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lookups.size()));
}

// Erase a random tenth of the set; erased elements are re-inserted outside of the timed region
template <typename T>
static void BM_FlatHashSetString_Erase(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = ShuffledStrings(num_items);
  T cont(keys.begin(), keys.end());
  auto const to_erase = RandomSample(keys, num_items / 10);

  for (auto _ : state) {
    for (const auto &key : to_erase) {
      auto erased = cont.erase(key);
      benchmark::DoNotOptimize(erased);
    }
    state.PauseTiming();
    cont.insert(to_erase.begin(), to_erase.end());
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(to_erase.size()));
}

BENCHMARK(BM_FlatHashSetInt64_Insert<absl::flat_hash_set<uint64_t>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashSetInt64_Insert<boost::unordered_flat_set<uint64_t>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashSetString_Insert<absl::flat_hash_set<std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashSetString_Insert<boost::unordered_flat_set<std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashSetString_Find<absl::flat_hash_set<std::string>>)->Apply(FindArgs);
BENCHMARK(BM_FlatHashSetString_Find<boost::unordered_flat_set<std::string>>)->Apply(FindArgs);
BENCHMARK(BM_FlatHashSetString_Erase<absl::flat_hash_set<std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashSetString_Erase<boost::unordered_flat_set<std::string>>)->Apply(SizeArgs);
BENCHMARK_MAIN();
