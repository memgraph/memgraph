#include <random>
#include <thread>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/random/random_generator.hpp"

#include "benchmark/benchmark_api.h"

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
void parse_arguments(int argc, char **argv) {
  REGISTER_ARGS(argc, argv);

  INSERT_PERC = GET_ARG("-insert", "50").get_int();
  DELETE_PERC = GET_ARG("-delete", "20").get_int();
  CONTAINS_PERC = GET_ARG("-find", "30").get_int();

  if (INSERT_PERC + DELETE_PERC + CONTAINS_PERC != 100) {
    std::cout << "Invalid percentage" << std::endl;
    std::cout << "Percentage must sum to 100" << std::endl;
    exit(-1);
  }

  RANGE_START = GET_ARG("-start", "0").get_int();

  RANGE_END = GET_ARG("-end", "1000000000").get_int();

  THREADS = std::min(GET_ARG("-threads", "1").get_int(),
                     (int)std::thread::hardware_concurrency());
}

int main(int argc, char **argv) {
  logging::init_async();
  logging::log->pipe(std::make_unique<Stdout>());

  parse_arguments(argc, argv);

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

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
