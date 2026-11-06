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

// Lookups into a set of TypedValues, the shape of the cached set an `IN` list filter probes once per row.

#include <benchmark/benchmark.h>
#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "boost/unordered/unordered_flat_set.hpp"
#include "query/typed_value.hpp"

namespace {

using memgraph::query::TypedValue;

// Fixed seed so every container sees the same keys in the same order
constexpr uint64_t kSeed = 42;

constexpr int64_t kMinSize = 1 << 10;
constexpr int64_t kMaxSize = 1 << 22;
constexpr int64_t kSizeMultiplier = 8;

std::vector<TypedValue> RandomIntValues(int64_t n, uint64_t seed) {
  std::mt19937_64 rng(seed);
  std::vector<TypedValue> values;
  values.reserve(n);
  for (int64_t i = 0; i < n; i++) {
    values.emplace_back(static_cast<int64_t>(rng()));
  }
  return values;
}

std::vector<TypedValue> ShuffledStringValues(int64_t n, std::string const &prefix) {
  std::vector<TypedValue> values;
  values.reserve(n);
  for (int64_t i = 0; i < n; i++) {
    values.emplace_back(prefix + std::to_string(i));
  }
  std::ranges::shuffle(values, std::mt19937_64(kSeed));
  return values;
}

struct IntValues {
  static auto Present(int64_t n) { return RandomIntValues(n, kSeed); }

  static auto Absent(int64_t n) { return RandomIntValues(n, kSeed + 3); }
};

struct StringValues {
  static auto Present(int64_t n) { return ShuffledStringValues(n, "str"); }

  static auto Absent(int64_t n) { return ShuffledStringValues(n, "miss"); }
};

// As many lookups as there are present values; hit_pct of them are present, the rest come from absent
std::vector<TypedValue> LookupValues(std::vector<TypedValue> present, std::vector<TypedValue> const &absent,
                                     int64_t hit_pct) {
  auto const n = static_cast<int64_t>(present.size());
  auto const hits = n * hit_pct / 100;
  std::ranges::shuffle(present, std::mt19937_64(kSeed + 1));
  present.resize(hits);
  present.insert(present.end(), absent.begin(), absent.begin() + (n - hits));
  std::ranges::shuffle(present, std::mt19937_64(kSeed + 2));
  return present;
}

void FindArgs(benchmark::Benchmark *b) {
  b->ArgsProduct({benchmark::CreateRange(kMinSize, kMaxSize, kSizeMultiplier), {100, 50, 0}})
      ->ArgNames({"size", "hit_pct"})
      ->Unit(benchmark::kMicrosecond);
}

// The hasher and equality used by the `IN` list cache
using AbslSet = absl::flat_hash_set<TypedValue, absl::DefaultHashContainerHash<TypedValue>, TypedValue::BoolEqual>;
using BoostSet = boost::unordered_flat_set<TypedValue, TypedValue::Hash, TypedValue::BoolEqual>;
using BoostAbslHashSet = boost::unordered_flat_set<TypedValue, absl::Hash<TypedValue>, TypedValue::BoolEqual>;

}  // namespace

// Look up as many values as the set holds, in random order, with hit_pct of them present
template <typename T, typename Values>
static void BM_TypedValueSet_Find(benchmark::State &state) {
  auto const num_items = state.range(0);
  auto const values = Values::Present(num_items);
  T cont(values.begin(), values.end());
  auto const lookups = LookupValues(values, Values::Absent(num_items), state.range(1));

  for (auto _ : state) {
    int64_t found = 0;
    for (auto const &value : lookups) {
      found += cont.find(value) != cont.end();
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lookups.size()));
}

BENCHMARK(BM_TypedValueSet_Find<AbslSet, IntValues>)->Apply(FindArgs);
BENCHMARK(BM_TypedValueSet_Find<BoostSet, IntValues>)->Apply(FindArgs);
BENCHMARK(BM_TypedValueSet_Find<BoostAbslHashSet, IntValues>)->Apply(FindArgs);
BENCHMARK(BM_TypedValueSet_Find<AbslSet, StringValues>)->Apply(FindArgs);
BENCHMARK(BM_TypedValueSet_Find<BoostSet, StringValues>)->Apply(FindArgs);
BENCHMARK(BM_TypedValueSet_Find<BoostAbslHashSet, StringValues>)->Apply(FindArgs);
BENCHMARK_MAIN();
