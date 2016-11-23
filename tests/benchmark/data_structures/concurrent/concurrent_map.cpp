#include <random>
#include <thread>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/random/generator.h"

#include "benchmark/benchmark_api.h"

/*
  ConcurrentMap Benchmark Test:
    - tests time of Insertion, Contain and Delete operations

    - benchmarking time per operation

    - test run ConcurrentMap with the following keys and values:
        - <int,int>
        - <int, string>
        - <string, int>
        - <string, string>

    - tests run single and multi threaded in range (1, Max_Threads_Per_Cpu)

    TODO(sale) implements configurable command line arguments on start
*/

using utils::random::NumberGenerator;
using utils::random::PairGenerator;
using utils::random::StringGenerator;

using IntegerGenerator = NumberGenerator<std::uniform_int_distribution<int>,
                                         std::default_random_engine, int>;

template <class K, class V>
static void InsertValue(benchmark::State& state, ConcurrentMap<K, V>* map,
                        const std::vector<std::pair<K, V>>& elements) {
  while (state.KeepRunning()) {
    auto accessor = map->access();
    for (int start = 0; start < state.range(0); start++) {
      accessor.insert(elements[start].first, elements[start].second);
    }
  }
  state.SetComplexityN(state.range(0));
}

template <class K, class V>
static void DeleteValue(benchmark::State& state, ConcurrentMap<K, V>* map,
                        const std::vector<std::pair<K, V>> elements) {
  while (state.KeepRunning()) {
    auto accessor = map->access();
    for (int start = 0; start < state.range(0); start++) {
      accessor.remove(elements[start].first);
    }
  }
  state.SetComplexityN(state.range(0));
}

template <class K, class V>
static void ContainsValue(benchmark::State& state, ConcurrentMap<K, V>* map,
                          const std::vector<std::pair<K, V>> elements) {
  while (state.KeepRunning()) {
    auto accessor = map->access();
    for (int start = 0; start < state.range(0); start++) {
      accessor.contains(elements[start].first);
    }
  }
  state.SetComplexityN(state.range(0));
}

auto BM_InsertValue = [](benchmark::State& state, auto* map, auto& elements) {
  InsertValue(state, map, elements);
};

auto BM_DeleteValue = [](benchmark::State& state, auto* map, auto elements) {
  DeleteValue(state, map, elements);
};

auto BM_ContainsValue = [](benchmark::State& state, auto* map, auto elements) {
  ContainsValue(state, map, elements);
};

int main(int argc, char** argv) {
  int MAX_ELEMENTS = 1 << 14;
  int MULTIPLIER = 2;
  int MAX_THREADS = (int)std::thread::hardware_concurrency();

  logging::init_async();
  logging::log->pipe(std::make_unique<Stdout>());

  StringGenerator sg(128);
  IntegerGenerator ig(0, 1000000);

  /*
    Creates RandomGenerators, ConcurentMaps and Random Element Vectors for the
    following use cases:

      Map elements contain keys and value for:
        <int, int>,
        <int, string>
        <string, int>
        <string, string>
  */

  PairGenerator<IntegerGenerator, IntegerGenerator> piig(&ig, &ig);
  PairGenerator<StringGenerator, StringGenerator> pssg(&sg, &sg);
  PairGenerator<StringGenerator, IntegerGenerator> psig(&sg, &ig);
  PairGenerator<IntegerGenerator, StringGenerator> pisg(&ig, &sg);

  ConcurrentMap<int, int> ii_map;
  ConcurrentMap<int, std::string> is_map;
  ConcurrentMap<std::string, int> si_map;
  ConcurrentMap<std::string, std::string> ss_map;

  auto ii_elems = utils::random::generate_vector(piig, MAX_ELEMENTS);
  auto is_elems = utils::random::generate_vector(pisg, MAX_ELEMENTS);
  auto si_elems = utils::random::generate_vector(psig, MAX_ELEMENTS);
  auto ss_elems = utils::random::generate_vector(pssg, MAX_ELEMENTS);

  /* insertion Tests */

  for (int t = 1; t <= MAX_THREADS; t *= 2) {
    benchmark::RegisterBenchmark("InsertValue[Int, Int]", BM_InsertValue,
                                 &ii_map, ii_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("InsertValue[Int, String] (size:128 chars)",
                                 BM_InsertValue, &is_map, is_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("InsertValue[String, Int] (size:128 chars)",
                                 BM_InsertValue, &si_map, si_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("InsertValue[String, String] (size:128 chars)",
                                 BM_InsertValue, &ss_map, ss_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);
  }

  // Contains Benchmark Tests

  for (int t = 1; t <= MAX_THREADS; t *= 2) {
    benchmark::RegisterBenchmark("ContainsValue[Int, Int]", BM_ContainsValue,
                                 &ii_map, ii_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("ContainsValue[Int, String] (size:128 chars)",
                                 BM_ContainsValue, &is_map, is_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("ContainsValue[String, Int] (size:128 chars)",
                                 BM_ContainsValue, &si_map, si_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark(
        "ContainsValue[String, String] (size:128 chars)", BM_ContainsValue,
        &ss_map, ss_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);
  }

  // Deletion Banchamark Tests

  for (int t = 1; t <= MAX_THREADS; t *= 2) {
    benchmark::RegisterBenchmark("DeleteValue[Int, Int]", BM_DeleteValue,
                                 &ii_map, ii_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("DeleteValue[Int, String] (size:128 chars)",
                                 BM_DeleteValue, &is_map, is_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("DeleteValue[String, Int] (size:128 chars)",
                                 BM_DeleteValue, &si_map, si_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);

    benchmark::RegisterBenchmark("DeleteValue[String, String] (size:128 chars)",
                                 BM_DeleteValue, &ss_map, ss_elems)
        ->RangeMultiplier(MULTIPLIER)
        ->Range(1, MAX_ELEMENTS)
        ->Complexity(benchmark::oN)
        ->Threads(t);
  }

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
