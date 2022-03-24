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

#include <chrono>
#include <iostream>
#include <map>
#include <random>

#include <benchmark/benchmark.h>

#include "storage/v2/property_store.hpp"

///////////////////////////////////////////////////////////////////////////////
// PropertyStore Set
///////////////////////////////////////////////////////////////////////////////

// NOLINTNEXTLINE(google-runtime-references)
static void PropertyStoreSet(benchmark::State &state) {
  memgraph::storage::PropertyStore store;
  std::mt19937 gen(state.thread_index());
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = memgraph::storage::PropertyId::FromUint(dist(gen));
    store.SetProperty(prop, memgraph::storage::PropertyValue(42));
    ++counter;
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK(PropertyStoreSet)->RangeMultiplier(2)->Range(1, 1024)->Unit(benchmark::kNanosecond)->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// std::map Set
///////////////////////////////////////////////////////////////////////////////

// NOLINTNEXTLINE(google-runtime-references)
static void StdMapSet(benchmark::State &state) {
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> store;
  std::mt19937 gen(state.thread_index());
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = memgraph::storage::PropertyId::FromUint(dist(gen));
    store.emplace(prop, memgraph::storage::PropertyValue(42));
    ++counter;
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK(StdMapSet)->RangeMultiplier(2)->Range(1, 1024)->Unit(benchmark::kNanosecond)->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// PropertyStore Get
///////////////////////////////////////////////////////////////////////////////

// NOLINTNEXTLINE(google-runtime-references)
static void PropertyStoreGet(benchmark::State &state) {
  memgraph::storage::PropertyStore store;
  for (uint64_t i = 0; i < state.range(0); ++i) {
    auto prop = memgraph::storage::PropertyId::FromUint(i);
    store.SetProperty(prop, memgraph::storage::PropertyValue(0));
  }
  std::mt19937 gen(state.thread_index());
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = memgraph::storage::PropertyId::FromUint(dist(gen));
    store.GetProperty(prop);
    ++counter;
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK(PropertyStoreGet)->RangeMultiplier(2)->Range(1, 1024)->Unit(benchmark::kNanosecond)->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// std::map Get
///////////////////////////////////////////////////////////////////////////////

// NOLINTNEXTLINE(google-runtime-references)
static void StdMapGet(benchmark::State &state) {
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> store;
  for (uint64_t i = 0; i < state.range(0); ++i) {
    auto prop = memgraph::storage::PropertyId::FromUint(i);
    store.emplace(prop, memgraph::storage::PropertyValue(0));
  }
  std::mt19937 gen(state.thread_index());
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = memgraph::storage::PropertyId::FromUint(dist(gen));
    store.find(prop);
    ++counter;
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK(StdMapGet)->RangeMultiplier(2)->Range(1, 1024)->Unit(benchmark::kNanosecond)->UseRealTime();

BENCHMARK_MAIN();
