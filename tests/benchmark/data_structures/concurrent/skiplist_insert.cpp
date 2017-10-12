#ifndef NDEBUG
#define NDEBUG
#endif

#include <algorithm>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>

#include "data_structures/concurrent/skiplist.hpp"
#include "skiplist_helper.hpp"

void Insert(benchmark::State &state) {
  SkipList<int> skiplist;
  while (state.KeepRunning()) {
    const int count = SkipListHelper::InsertConcurrentSkiplistTimed(
        &skiplist, 0, 10000000, state.range(1),
        std::chrono::microseconds(state.range(0)));
    state.SetItemsProcessed(count);  // Number of processed items in one
                                     // iteration - useful for items/per s.
    state.SetIterationTime(state.range(0) * 1.0 /
                           1000000);  // Time the iteration took - since ideally
                                      // all threads should run and stop at the
                                      // same time we set the time manually.
    auto sl_access = skiplist.access();
    while (sl_access.size()) sl_access.remove(*sl_access.begin());
  }
}

/**
 * Invokes the test function with two arguments, time and number of threads.
 * Time is specified in microseconds.
 */
static void CustomArguments(benchmark::internal::Benchmark *b) {
  for (int i = (1 << 18); i <= (1 << 20); i *= 2)
    for (int j = 1; j <= 8; ++j) b->Args({i, j});
}

/**
 * This benchmark represents a use case of benchmarking one multi-threaded
 * concurrent structure. This test assumes that all threads will start and end
 * at the exact same time and will compete with each other.
 */
BENCHMARK(Insert)
    ->Apply(CustomArguments)  // Set custom arguments.
    ->Unit(benchmark::kMicrosecond)
    ->UseManualTime()  // Don't calculate real-time but depend on function
                       // providing the execution time.
    ->Repetitions(3)
    ->ReportAggregatesOnly(1);

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
