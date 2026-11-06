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
#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "boost/unordered/unordered_flat_map.hpp"

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

// Stands in for a storage object such as an Edge; only its address is used as a key
struct Node {
  std::array<std::byte, 64> payload;
};

// absl::Hash marked as already well mixed, so boost skips its own mixing step
template <typename T>
struct AvalanchingAbslHash : absl::Hash<T> {
  using is_avalanching = std::true_type;
};

// Nodes sit 64 bytes apart, so the low 6 bits of their addresses never vary; drop them and let boost mix the rest
struct ShiftedPtrHash {
  std::size_t operator()(Node const *ptr) const noexcept { return reinterpret_cast<std::uintptr_t>(ptr) >> 6; }
};

std::vector<Node const *> ShuffledPointers(std::vector<Node> const &nodes) {
  std::vector<Node const *> ptrs;
  ptrs.reserve(nodes.size());
  for (auto const &node : nodes) {
    ptrs.push_back(&node);
  }
  std::ranges::shuffle(ptrs, std::mt19937_64(kSeed));
  return ptrs;
}

// As many lookups as there are present keys; hit_pct of them are present, the rest come from absent
template <typename K>
std::vector<K> LookupKeys(std::vector<K> const &present, std::vector<K> const &absent, int64_t hit_pct) {
  auto const n = static_cast<int64_t>(present.size());
  auto const hits = n * hit_pct / 100;
  auto lookups = RandomSample(present, hits);
  lookups.insert(lookups.end(), absent.begin(), absent.begin() + (n - hits));
  std::ranges::shuffle(lookups, std::mt19937_64(kSeed + 2));
  return lookups;
}

template <typename T, typename K>
void FillWithKeys(T &cont, const std::vector<K> &keys) {
  cont.reserve(cont.size() + keys.size());
  for (const auto &key : keys) {
    cont[key] = key;
  }
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
static void BM_FlatHashMapInt64_Insert(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = RandomInts(num_items);
  for (auto _ : state) {
    T cont;
    cont.reserve(num_items);
    for (auto key : keys) {
      cont[key] = key;
    }
  }
  state.SetItemsProcessed(state.iterations() * num_items);
}

template <typename T>
static void BM_FlatHashMapString_Insert(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = ShuffledStrings(num_items);
  for (auto _ : state) {
    T cont;
    cont.reserve(num_items);
    for (const auto &key : keys) {
      cont[key] = key;
    }
  }
  state.SetItemsProcessed(state.iterations() * num_items);
}

// Look up as many keys as the map holds, in random order, with hit_pct of them present
template <typename T>
static void BM_FlatHashMapString_Find(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = ShuffledStrings(num_items);
  T cont;
  FillWithKeys(cont, keys);
  auto const lookups = LookupKeys(keys, ShuffledStrings(num_items, "miss"), state.range(1));

  for (auto _ : state) {
    int64_t found = 0;
    for (const auto &key : lookups) {
      found += cont.find(key) != cont.end();
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lookups.size()));
}

template <typename T>
static void BM_FlatHashMapPtr_Insert(benchmark::State &state) {
  auto const num_items = state.range(0);
  std::vector<Node> const nodes(num_items);
  auto const keys = ShuffledPointers(nodes);
  for (auto _ : state) {
    T cont;
    cont.reserve(num_items);
    for (auto key : keys) {
      cont[key] = key;
    }
  }
  state.SetItemsProcessed(state.iterations() * num_items);
}

// Look up as many pointers as the map holds, in random order; misses point to nodes that were never inserted
template <typename T>
static void BM_FlatHashMapPtr_Find(benchmark::State &state) {
  auto const num_items = state.range(0);
  std::vector<Node> const nodes(num_items);
  std::vector<Node> const absent_nodes(num_items);
  auto const keys = ShuffledPointers(nodes);
  T cont;
  FillWithKeys(cont, keys);
  auto const lookups = LookupKeys(keys, ShuffledPointers(absent_nodes), state.range(1));

  for (auto _ : state) {
    int64_t found = 0;
    for (auto key : lookups) {
      found += cont.find(key) != cont.end();
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lookups.size()));
}

// Erase a random tenth of the map; erased elements are re-inserted outside of the timed region
template <typename T>
static void BM_FlatHashMapString_Erase(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const keys = ShuffledStrings(num_items);
  T cont;
  FillWithKeys(cont, keys);
  auto const to_erase = RandomSample(keys, num_items / 10);

  for (auto _ : state) {
    for (const auto &key : to_erase) {
      auto erased = cont.erase(key);
      benchmark::DoNotOptimize(erased);
    }
    state.PauseTiming();
    FillWithKeys(cont, to_erase);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(to_erase.size()));
}

BENCHMARK(BM_FlatHashMapInt64_Insert<absl::flat_hash_map<uint64_t, uint64_t>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapInt64_Insert<boost::unordered_flat_map<uint64_t, uint64_t>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapString_Insert<absl::flat_hash_map<std::string, std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapString_Insert<boost::unordered_flat_map<std::string, std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapString_Find<absl::flat_hash_map<std::string, std::string>>)->Apply(FindArgs);
BENCHMARK(BM_FlatHashMapString_Find<boost::unordered_flat_map<std::string, std::string>>)->Apply(FindArgs);
BENCHMARK(BM_FlatHashMapString_Erase<absl::flat_hash_map<std::string, std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapString_Erase<boost::unordered_flat_map<std::string, std::string>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapPtr_Insert<absl::flat_hash_map<Node const *, Node const *>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapPtr_Insert<boost::unordered_flat_map<Node const *, Node const *>>)->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapPtr_Insert<boost::unordered_flat_map<Node const *, Node const *, absl::Hash<Node const *>>>)
    ->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapPtr_Find<absl::flat_hash_map<Node const *, Node const *>>)->Apply(FindArgs);
BENCHMARK(BM_FlatHashMapPtr_Find<boost::unordered_flat_map<Node const *, Node const *>>)->Apply(FindArgs);
BENCHMARK(BM_FlatHashMapPtr_Find<boost::unordered_flat_map<Node const *, Node const *, absl::Hash<Node const *>>>)
    ->Apply(FindArgs);
BENCHMARK(
    BM_FlatHashMapPtr_Insert<boost::unordered_flat_map<Node const *, Node const *, AvalanchingAbslHash<Node const *>>>)
    ->Apply(SizeArgs);
BENCHMARK(BM_FlatHashMapPtr_Insert<boost::unordered_flat_map<Node const *, Node const *, ShiftedPtrHash>>)
    ->Apply(SizeArgs);
BENCHMARK(
    BM_FlatHashMapPtr_Find<boost::unordered_flat_map<Node const *, Node const *, AvalanchingAbslHash<Node const *>>>)
    ->Apply(FindArgs);
BENCHMARK(BM_FlatHashMapPtr_Find<boost::unordered_flat_map<Node const *, Node const *, ShiftedPtrHash>>)
    ->Apply(FindArgs);
BENCHMARK_MAIN();
