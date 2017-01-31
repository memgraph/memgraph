/**
  @date: 2017-01-31
  @authors: Sandi Fatic

  These tests are used to benchmark the ReverseIterator vs the Find function
  while iterating the whole skiplist in reverse.
*/

#include <algorithm>
#include <thread>
#include <vector>

#include "benchmark/benchmark_api.h"
#include "data_structures/concurrent/skiplist.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/random/generator.h"

using utils::random::NumberGenerator;
using IntegerGenerator = NumberGenerator<std::uniform_int_distribution<int>,
                                         std::default_random_engine, int>;

void InsertSkiplist(SkipList<int> *skiplist, int start, int end) {
  auto accessor = skiplist->access();

  for (int start = 0; start < end; start++) {
    accessor.insert(std::move(start));
  }
}

void InsertConcurrentSkiplist(SkipList<int> *skiplist, int start, int end) {
  int number_od_threads = std::thread::hardware_concurrency();
  std::vector<std::thread> threads(number_od_threads);

  for (int i = 0; i < number_od_threads; i++) {
    int part = (end - start) / number_od_threads;
    threads[i] = std::thread(InsertSkiplist, skiplist, start + i * part,
                             start + (i + 1) * part);
  }

  for (int i = 0; i < number_od_threads; i++) {
    threads[i].join();
  }
}

static void ReverseFromRBegin(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();

    SkipList<int> skiplist;
    InsertConcurrentSkiplist(&skiplist, 0, state.range(0));
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
    InsertConcurrentSkiplist(&skiplist, 0, state.range(0));
    int counter = 10;

    auto accessor = skiplist.access();
    auto rbegin = accessor.rbegin();
    auto rend = accessor.rend();

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
  logging::init_async();
  logging::log->pipe(std::make_unique<Stdout>());

  benchmark::RegisterBenchmark("ReverseFromRBegin", BM_ReverseFromRBegin)
      ->RangeMultiplier(2)
      ->Range(1 << 10, 1 << 16);

  benchmark::RegisterBenchmark("FindFromRBegin", BM_FindFromRBegin)
      ->RangeMultiplier(2)
      ->Range(1 << 10, 1 << 16);

  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
