#include <random>
#include <thread>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"
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
*/

using utils::random::NumberGenerator;
using utils::random::PairGenerator;
using utils::random::StringGenerator;

using IntegerGenerator = NumberGenerator<std::uniform_int_distribution<int>,
                                         std::default_random_engine, int>;

// Global arguments
int MAX_ELEMENTS = 1 << 20, MULTIPLIER = 2;
int THREADS, RANGE_START, RANGE_END, STRING_LENGTH;

/*
  ConcurrentMap Insertion Benchmark Test
*/
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

/*
  ConcurrentMap Deletion Benchmark Test
*/
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

/*
  ConcurrentMap Contains Benchmark Test
*/
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

/*
  Commandline Argument Parsing

  Arguments:
   * Integer Range Minimum
      -start number

   *  Integer Range Maximum
      - end number

   * Number of threads
      - threads number

   * Random String lenght
      -string-length number
*/
void parse_arguments(int argc, char** argv) {
  ProgramArguments::instance().register_args(argc, argv);

  RANGE_START =
      ProgramArguments::instance().get_arg("-start", "0").GetInteger();
  RANGE_END =
      ProgramArguments::instance().get_arg("-end", "1000000000").GetInteger();

  THREADS = std::min(
      ProgramArguments::instance().get_arg("-threads", "1").GetInteger(),
      (int)std::thread::hardware_concurrency());

  STRING_LENGTH = ProgramArguments::instance()
                      .get_arg("-string-length", "128")
                      .GetInteger();
}

int main(int argc, char** argv) {
  logging::init_async();
  logging::log->pipe(std::make_unique<Stdout>());

  parse_arguments(argc, argv);

  StringGenerator sg(STRING_LENGTH);
  IntegerGenerator ig(RANGE_START, RANGE_END);

  /*
    Creates RandomGenerators, ConcurentMaps and Random Element Vectors for the
    following use cases:

      Map elements contain keys and value for:
        <int, int>,
        <int, string>
        <string, int>
        <string, string>
  */

  // random generators for tests
  PairGenerator<IntegerGenerator, IntegerGenerator> piig(&ig, &ig);
  PairGenerator<StringGenerator, StringGenerator> pssg(&sg, &sg);
  PairGenerator<StringGenerator, IntegerGenerator> psig(&sg, &ig);
  PairGenerator<IntegerGenerator, StringGenerator> pisg(&ig, &sg);

  // maps used for testing
  ConcurrentMap<int, int> ii_map;
  ConcurrentMap<int, std::string> is_map;
  ConcurrentMap<std::string, int> si_map;
  ConcurrentMap<std::string, std::string> ss_map;

  // random elements for testing
  auto ii_elems = utils::random::generate_vector(piig, MAX_ELEMENTS);
  auto is_elems = utils::random::generate_vector(pisg, MAX_ELEMENTS);
  auto si_elems = utils::random::generate_vector(psig, MAX_ELEMENTS);
  auto ss_elems = utils::random::generate_vector(pssg, MAX_ELEMENTS);

  /* insertion Tests */

  benchmark::RegisterBenchmark("InsertValue[Int, Int]", BM_InsertValue, &ii_map,
                               ii_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("InsertValue[Int, String]", BM_InsertValue,
                               &is_map, is_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("InsertValue[String, Int]", BM_InsertValue,
                               &si_map, si_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("InsertValue[String, String]", BM_InsertValue,
                               &ss_map, ss_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  // Contains Benchmark Tests

  benchmark::RegisterBenchmark("ContainsValue[Int, Int]", BM_ContainsValue,
                               &ii_map, ii_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("ContainsValue[Int, String]", BM_ContainsValue,
                               &is_map, is_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("ContainsValue[String, Int]", BM_ContainsValue,
                               &si_map, si_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("ContainsValue[String, String]",
                               BM_ContainsValue, &ss_map, ss_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  // Deletion Banchamark Tests

  benchmark::RegisterBenchmark("DeleteValue[Int, Int]", BM_DeleteValue, &ii_map,
                               ii_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("DeleteValue[Int, String]", BM_DeleteValue,
                               &is_map, is_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("DeleteValue[String, Int]", BM_DeleteValue,
                               &si_map, si_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RegisterBenchmark("DeleteValue[String, String]", BM_DeleteValue,
                               &ss_map, ss_elems)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
