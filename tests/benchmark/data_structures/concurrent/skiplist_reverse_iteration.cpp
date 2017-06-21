/**
  @date: 2017-01-31
  @authors: Sandi Fatic

  These tests are used to benchmark the ReverseIterator vs the Find function
  while iterating the whole skiplist in reverse.
*/

#include <algorithm>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "benchmark/benchmark_api.h"
#include "data_structures/concurrent/skiplist.hpp"
#include "skiplist_helper.hpp"
#include "utils/random/random_generator.hpp"

using utils::random::NumberGenerator;
using IntegerGenerator = NumberGenerator<std::uniform_int_distribution<int>,
                                         std::default_random_engine, int>;

static void ReverseFromRBegin(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();

    SkipList<int> skiplist;
    SkipListHelper::InsertConcurrentSkiplist(&skiplist, 0, state.range(0));
    int counter = 10;

    auto accessor = skiplist.access();
    auto rbegin = accessor.rbegin();
    auto rend = accessor.rend();

    state.ResumeTiming();

    for (int i = 0; i < counter; i++) {
      if (rbegin != rend) {
        rbegin++;
      }
    }
  }
}

static void FindFromRBegin(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();

    SkipList<int> skiplist;
    SkipListHelper::InsertConcurrentSkiplist(&skiplist, 0, state.range(0));
    int counter = 10;

    auto accessor = skiplist.access();
    auto rbegin = accessor.rbegin();

    state.ResumeTiming();

    for (int i = 0; i < counter; i++) {
      accessor.find(*rbegin - i);
    }
  }
}

auto BM_ReverseFromRBegin = [](benchmark::State &state) {
  ReverseFromRBegin(state);
};

auto BM_FindFromRBegin = [](benchmark::State &state) { FindFromRBegin(state); };

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  benchmark::RegisterBenchmark("ReverseFromRBegin", BM_ReverseFromRBegin)
      ->RangeMultiplier(2)
      ->Range(1 << 10, 1 << 16);

  benchmark::RegisterBenchmark("FindFromRBegin", BM_FindFromRBegin)
      ->RangeMultiplier(2)
      ->Range(1 << 10, 1 << 16);

  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
