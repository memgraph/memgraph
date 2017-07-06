#include <random>
#include <thread>

#include <benchmark/benchmark_api.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "utils/random/random_generator.hpp"

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

DEFINE_int32(start, 0, "Range start");
DEFINE_int32(end, 1000000000, "Range end");
DEFINE_int32(threads, 1, "Number of threads");
DEFINE_int32(string_length, 128, "String length");

// Global arguments
int MAX_ELEMENTS = 1 << 18, MULTIPLIER = 2;
int THREADS, RANGE_START, RANGE_END, STRING_LENGTH;

/*
  ConcurrentMap Insertion Benchmark Test
*/
template <class K, class V>
static void InsertValue(benchmark::State &state, ConcurrentMap<K, V> *map,
                        const std::vector<std::pair<K, V>> &elements) {
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
static void DeleteValue(benchmark::State &state, ConcurrentMap<K, V> *map,
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
static void ContainsValue(benchmark::State &state, ConcurrentMap<K, V> *map,
                          const std::vector<std::pair<K, V>> elements) {
  while (state.KeepRunning()) {
    auto accessor = map->access();
    for (int start = 0; start < state.range(0); start++) {
      accessor.contains(elements[start].first);
    }
  }
  state.SetComplexityN(state.range(0));
}

auto BM_InsertValue = [](benchmark::State &state, auto *map, auto &elements) {
  InsertValue(state, map, elements);
};

auto BM_DeleteValue = [](benchmark::State &state, auto *map, auto elements) {
  DeleteValue(state, map, elements);
};

auto BM_ContainsValue = [](benchmark::State &state, auto *map, auto elements) {
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
void parse_arguments() {
  RANGE_START = FLAGS_start;
  RANGE_END = FLAGS_end;

  THREADS = std::min(FLAGS_threads,
                     static_cast<int>(std::thread::hardware_concurrency()));
  STRING_LENGTH = FLAGS_string_length;
}

int main(int argc, char **argv) {
  benchmark::Initialize(&argc, argv);
  parse_arguments();
  google::InitGoogleLogging(argv[0]);

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

  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
