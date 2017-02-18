#define LOG_NO_INFO 1

#include "benchmark/benchmark_api.h"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/preprocessor.hpp"
#include "yaml-cpp/yaml.h"

auto BM_Strip = [](benchmark::State &state, auto &function, std::string query) {
  while (state.KeepRunning()) {
    for (int start = 0; start < state.range(0); start++) {
      function(query);
    }
  }
  state.SetComplexityN(state.range(0));
};

int main(int argc, char **argv) {
  logging::init_async();
  logging::log->pipe(std::make_unique<Stdout>());

  YAML::Node dataset = YAML::LoadFile(
      "../../tests/data/cypher_queries/stripper/query_dict.yaml");

  QueryPreprocessor processor;
  using std::placeholders::_1;
  std::function<StrippedQuery(const std::string &query)> preprocess =
      std::bind(&QueryPreprocessor::preprocess, &processor, _1);

  auto tests = dataset["benchmark_queries"].as<std::vector<std::string>>();
  for (auto &test : tests) {
    auto *benchmark =
        benchmark::RegisterBenchmark(test.c_str(), BM_Strip, preprocess, test)
            ->RangeMultiplier(2)
            ->Range(1, 8 << 10)
            ->Complexity(benchmark::oN);
  }

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
