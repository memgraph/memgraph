// Copyright 2022 Memgraph Ltd.
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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

#include "storage/v3/key_store.hpp"
#include "storage/v3/lexicographically_ordered_vertex.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::benchmark {

///////////////////////////////////////////////////////////////////////////////
// Testing Insert Operation
///////////////////////////////////////////////////////////////////////////////
static void BM_BenchmarkInsertSkipList(::benchmark::State &state) {
  utils::SkipList<storage::v3::PrimaryKey> skip_list;
  coordinator::Hlc start_timestamp;
  storage::v3::IsolationLevel isolation_level{storage::v3::IsolationLevel::SNAPSHOT_ISOLATION};
  storage::v3::Transaction transaction{start_timestamp, isolation_level};
  auto *delta = storage::v3::CreateDeleteObjectDelta(&transaction);

  for (auto _ : state) {
    for (auto i{0}; i < state.range(0); ++i) {
      auto acc = skip_list.access();
      acc.insert({storage::v3::PrimaryKey{storage::v3::PropertyValue{i}}});
    }
  }
}

static void BM_BenchmarkInsertStdMap(::benchmark::State &state) {
  std::map<storage::v3::PrimaryKey, storage::v3::LexicographicallyOrderedVertex> std_map;
  coordinator::Hlc start_timestamp;
  storage::v3::IsolationLevel isolation_level{storage::v3::IsolationLevel::SNAPSHOT_ISOLATION};
  storage::v3::Transaction transaction{start_timestamp, isolation_level};
  auto *delta = storage::v3::CreateDeleteObjectDelta(&transaction);

  for (auto _ : state) {
    for (auto i{0}; i < state.range(0); ++i) {
      std_map.insert({storage::v3::PrimaryKey{storage::v3::PropertyValue{i}},
                      storage::v3::LexicographicallyOrderedVertex{storage::v3::Vertex{
                          delta, std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue{i}}}}});
    }
  }
}

static void BM_BenchmarkInsertStdSet(::benchmark::State &state) {
  std::set<storage::v3::PrimaryKey> std_set;
  coordinator::Hlc start_timestamp;
  storage::v3::IsolationLevel isolation_level{storage::v3::IsolationLevel::SNAPSHOT_ISOLATION};
  storage::v3::Transaction transaction{start_timestamp, isolation_level};
  auto *delta = storage::v3::CreateDeleteObjectDelta(&transaction);

  for (auto _ : state) {
    for (auto i{0}; i < state.range(0); ++i) {
      std_set.insert(storage::v3::PrimaryKey{std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue{i}}});
    }
  }
}

BENCHMARK(BM_BenchmarkInsertSkipList)->RangeMultiplier(10)->Range(1000, 10000000)->Unit(::benchmark::kMillisecond);

BENCHMARK(BM_BenchmarkInsertStdMap)->RangeMultiplier(10)->Range(1000, 10000000)->Unit(::benchmark::kMillisecond);

BENCHMARK(BM_BenchmarkInsertStdSet)->RangeMultiplier(10)->Range(1000, 10000000)->Unit(::benchmark::kMillisecond);

}  // namespace memgraph::benchmark

BENCHMARK_MAIN();
