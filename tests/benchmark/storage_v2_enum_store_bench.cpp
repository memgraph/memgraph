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

#include "storage/v2/enum_store.hpp"

#include <benchmark/benchmark.h>
#include <random>
#include <ranges>

using namespace memgraph::storage;

/// HELPERS

template <std::size_t lb, std::size_t ub>
auto getRandomString() -> std::string {
  thread_local auto engine = std::default_random_engine(std::random_device{}());
  thread_local auto len_dist = std::uniform_int_distribution<size_t>{lb, ub};
  return std::string(len_dist(engine), 'A');
  ;
}

static void BM_ConvertToEnum(benchmark::State &state) {
  // Setup extream usage:
  // - 100 enum types
  // - 1000 enum values each
  // - values string length 5 to 20
  auto sut = EnumStore{};
  for (auto i : rv::iota(0, 100)) {
    auto random_value = [](auto i) { return std::format("{}{}", getRandomString<5, 20>(), i); };
    auto values = rv::iota(0, 1000) | rv::transform(random_value);
    auto e_type = std::format("enum_type_{}", i);
    auto e_values = std::vector(values.begin(), values.end());
    [[maybe_unused]] auto res = sut.RegisterEnum(std::move(e_type), std::move(e_values));
  }

  auto const &c_sut = sut;
  // Lookup potential worst case, the last type + last value
  auto all = c_sut.AllRegistered();
  assert(r::size(all) != 0);
  auto const &[e_type, e_values] = *r::begin(all | rv::reverse);
  auto lookup_type = e_type;
  auto lookup_value = e_values.back();

  for (auto _ : state) {
    benchmark::ClobberMemory();
    benchmark::DoNotOptimize(lookup_type);
    benchmark::DoNotOptimize(lookup_value);
    auto e = c_sut.ToEnum(lookup_type, lookup_value);
    benchmark::DoNotOptimize(*e);
  }
}
// Register the function as a benchmark
BENCHMARK(BM_ConvertToEnum);
// Run the benchmark
BENCHMARK_MAIN();
