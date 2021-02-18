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
  storage::PropertyStore store;
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = storage::PropertyId::FromUint(dist(gen));
    store.SetProperty(prop, storage::PropertyValue(42));
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
  std::map<storage::PropertyId, storage::PropertyValue> store;
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = storage::PropertyId::FromUint(dist(gen));
    store.emplace(prop, storage::PropertyValue(42));
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
  storage::PropertyStore store;
  for (uint64_t i = 0; i < state.range(0); ++i) {
    auto prop = storage::PropertyId::FromUint(i);
    store.SetProperty(prop, storage::PropertyValue(0));
  }
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = storage::PropertyId::FromUint(dist(gen));
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
  std::map<storage::PropertyId, storage::PropertyValue> store;
  for (uint64_t i = 0; i < state.range(0); ++i) {
    auto prop = storage::PropertyId::FromUint(i);
    store.emplace(prop, storage::PropertyValue(0));
  }
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, state.range(0) - 1);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto prop = storage::PropertyId::FromUint(dist(gen));
    store.find(prop);
    ++counter;
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK(StdMapGet)->RangeMultiplier(2)->Range(1, 1024)->Unit(benchmark::kNanosecond)->UseRealTime();

BENCHMARK_MAIN();
