#include "benchmark/benchmark_api.h"

#include <set>
#include <vector>

static void BM_VectorInsert(benchmark::State &state)
{
  while (state.KeepRunning()) {
    std::vector<int> insertion_test;
    for (int i = 0, i_end = state.range_x(); i < i_end; i++) {
      insertion_test.push_back(i);
    }
  }
}

// Register the function as a benchmark
BENCHMARK(BM_VectorInsert)->Range(8, 8 << 10);

//~~~~~~~~~~~~~~~~

// Define another benchmark
static void BM_SetInsert(benchmark::State &state)
{
  while (state.KeepRunning()) {
    std::set<int> insertion_test;
    for (int i = 0, i_end = state.range_x(); i < i_end; i++) {
      insertion_test.insert(i);
    }
  }
}

BENCHMARK(BM_SetInsert)->Range(8, 8 << 10);

BENCHMARK_MAIN();
