#include <random>
#include <thread>

#include <benchmark/benchmark_api.h>
#include <glog/logging.h>
#include "gflags/gflags.h"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "utils/random/random_generator.hpp"

/*
  ConcurrentMap Benchmark Test:
    - tests time of Insertion, Deletion and Find

   - benchmarks time for total execution with operation percentages
*/

using utils::random::NumberGenerator;
using utils::random::PairGenerator;

using IntegerGenerator = NumberGenerator<std::uniform_int_distribution<int>,
                                         std::default_random_engine, int>;

// Global Arguments
int MAX_ELEMENTS = 1 << 20, MULTIPLIER = 2;
int THREADS, INSERT_PERC, DELETE_PERC, CONTAINS_PERC, RANGE_START, RANGE_END;

// ConcurrentMap Becnhmark Test using percentages for Insert, Delete, Find
template <class K, class V>
static void Rape(benchmark::State &state, ConcurrentMap<int, int> *map,
                 const std::vector<std::pair<K, V>> &elements) {
  int number_of_elements = state.range(0);

  while (state.KeepRunning()) {
    auto accessor = map->access();

    for (int start = 0; start < state.range(0); start++) {
      float current_percentage = (float)start / (float)number_of_elements * 100;
      if (current_percentage < (float)INSERT_PERC) {
        accessor.insert(elements[start].first, elements[start].second);
      } else if (current_percentage < (float)CONTAINS_PERC + INSERT_PERC) {
        accessor.contains(elements[start].first);
      } else {
        accessor.remove(elements[start].first);
      }
    }
  }

  state.SetComplexityN(state.range(0));
}

auto BM_Rape = [](benchmark::State &state, auto *map, auto &elements) {
  Rape(state, map, elements);
};

DEFINE_int32(insert, 50, "Insertions percentage");
DEFINE_int32(delete, 20, "Deletions percentage");
DEFINE_int32(find, 30, "Find percentage");
DEFINE_int32(start, 0, "Range start");
DEFINE_int32(end, 1000000000, "Range end");
DEFINE_int32(threads, 1, "Number of threads");

/*
  Commandline Arguments Parsing

  Arguments:
    * Insertion percentage (0-100)
        -insert number(int)

    * Deletion percentage (0-100)
        -delete number(int)

    * Find percentage (0-100)
        -find number(int)

    * Integer Range Minimum
        -start number

    * Integer Range Maximum
        - end number

    * Number of threads
        -threads number
*/
void parse_arguments() {
  INSERT_PERC = FLAGS_insert;
  DELETE_PERC = FLAGS_delete;
  CONTAINS_PERC = FLAGS_find;

  if (INSERT_PERC + DELETE_PERC + CONTAINS_PERC != 100) {
    std::cout << "Invalid percentage" << std::endl;
    std::cout << "Percentage must sum to 100" << std::endl;
    exit(-1);
  }

  RANGE_START = FLAGS_start;
  RANGE_END = FLAGS_end;

  THREADS = std::min(FLAGS_threads,
                     static_cast<int>(std::thread::hardware_concurrency()));
}

int main(int argc, char **argv) {
  benchmark::Initialize(&argc, argv);
  parse_arguments();
  google::InitGoogleLogging(argv[0]);

  IntegerGenerator int_gen(RANGE_START, RANGE_END);
  PairGenerator<IntegerGenerator, IntegerGenerator> pair_gen(&int_gen,
                                                             &int_gen);

  ConcurrentMap<int, int> map;
  auto elements = utils::random::generate_vector(pair_gen, MAX_ELEMENTS);

  benchmark::RegisterBenchmark("Rape", BM_Rape, &map, elements)
      ->RangeMultiplier(MULTIPLIER)
      ->Range(1, MAX_ELEMENTS)
      ->Complexity(benchmark::oN)
      ->Threads(THREADS);

  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
