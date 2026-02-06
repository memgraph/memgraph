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

/**
 * Benchmarks usearch index_dense with different metric + scalar combinations.
 * Two binary variants: usearch_index_add_builtin (SimSIMD=0, FP16=0) and usearch_index_add_simsimd
 * (SimSIMD=1, FP16=1). Run via ctest -R memgraph__benchmark__usearch_index_add or run the binaries
 * to compare; builtin is typically ~30–40% faster for L2 f32.
 *
 * All benchmarks use float vectors; the index scalar type (f32/f16) is set by the metric. For f16
 * indices, usearch converts f32 -> f16 on add internally.
 */

#include <benchmark/benchmark.h>
#include <usearch/index_dense.hpp>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <random>
#include <vector>

namespace {

namespace unum_usearch = unum::usearch;
using index_t = unum_usearch::index_dense_gt<>;

constexpr std::size_t kDimension = 512;
constexpr std::size_t kDefaultCapacity = 10000;

std::vector<float> MakeRandomVector(std::size_t dim, unsigned seed) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<float> dist(-1.0F, 1.0F);
  std::vector<float> v(dim);
  for (auto &x : v) x = dist(gen);
  return v;
}

// All metrics use float vectors; index scalar (f32/f16) is from metric.
void BM_IndexAdd(benchmark::State &state, unum_usearch::metric_kind_t metric_kind,
                 unum_usearch::scalar_kind_t scalar_kind) {
  const std::size_t n = static_cast<std::size_t>(state.range(0));
  const std::size_t capacity = (std::max)(n, kDefaultCapacity);

  unum_usearch::metric_punned_t metric(kDimension, metric_kind, scalar_kind);
  auto result = index_t::make(metric);
  if (!result) {
    state.SkipWithError("index_t::make failed");
    return;
  }
  index_t index = std::move(result.index);
  if (!index.try_reserve({capacity, 1})) {
    state.SkipWithError("try_reserve failed");
    return;
  }

  std::vector<std::vector<float>> vectors;
  vectors.reserve(n);
  for (std::size_t i = 0; i < n; ++i) vectors.push_back(MakeRandomVector(kDimension, static_cast<unsigned>(i)));

  for (auto _ : state) {
    state.PauseTiming();
    auto res = index_t::make(metric);
    if (!res) {
      state.SkipWithError("index_t::make failed in loop");
      return;
    }
    index = std::move(res.index);
    if (!index.try_reserve({capacity, 1})) {
      state.SkipWithError("try_reserve failed in loop");
      return;
    }
    state.ResumeTiming();

    for (std::size_t i = 0; i < n; ++i) {
      auto add_res = index.add(static_cast<std::uint64_t>(i), vectors[i].data());
      if (!add_res) {
        state.SkipWithError("add failed");
        return;
      }
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * n));
}

void PrintConfig() {
#if defined(USEARCH_USE_SIMSIMD) && USEARCH_USE_SIMSIMD
  std::cout << "Config: USEARCH_USE_SIMSIMD=1, USEARCH_USE_FP16LIB=" << USEARCH_USE_FP16LIB << " (simsimd path)\n";
#else
  std::cout << "Config: USEARCH_USE_SIMSIMD=0, USEARCH_USE_FP16LIB=" << USEARCH_USE_FP16LIB << " (builtin path)\n";
#endif
}

}  // namespace

// L2 squared + f32 (Memgraph vector index default)
BENCHMARK_CAPTURE(BM_IndexAdd, L2_F32, unum_usearch::metric_kind_t::l2sq_k, unum_usearch::scalar_kind_t::f32_k)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

// Cosine + f32 (common for embeddings)
BENCHMARK_CAPTURE(BM_IndexAdd, Cos_F32, unum_usearch::metric_kind_t::cos_k, unum_usearch::scalar_kind_t::f32_k)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

// Inner product (dot) + f32
BENCHMARK_CAPTURE(BM_IndexAdd, IP_F32, unum_usearch::metric_kind_t::ip_k, unum_usearch::scalar_kind_t::f32_k)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

// L2 squared + f16 (index stores f16; we add floats, usearch converts)
BENCHMARK_CAPTURE(BM_IndexAdd, L2_F16, unum_usearch::metric_kind_t::l2sq_k, unum_usearch::scalar_kind_t::f16_k)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

// Cosine + f16
BENCHMARK_CAPTURE(BM_IndexAdd, Cos_F16, unum_usearch::metric_kind_t::cos_k, unum_usearch::scalar_kind_t::f16_k)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

// Inner product + f16
BENCHMARK_CAPTURE(BM_IndexAdd, IP_F16, unum_usearch::metric_kind_t::ip_k, unum_usearch::scalar_kind_t::f16_k)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

int main(int argc, char **argv) {
  PrintConfig();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
