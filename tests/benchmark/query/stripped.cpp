#define LOG_NO_INFO 1

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <benchmark/benchmark_api.h>
#include <glog/logging.h>

#include "query/frontend/stripped.hpp"

auto BM_Strip = [](benchmark::State &state, auto &function, std::string query) {
  while (state.KeepRunning()) {
    for (int start = 0; start < state.range(0); start++) {
      function(query);
    }
  }
  state.SetComplexityN(state.range(0));
};

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);

  auto preprocess = [](const std::string &query) {
    return query::StrippedQuery(query);
  };

  std::string path = "../../tests/data/cypher_queries/stripper/query_dict.yaml";
  std::fstream queries_file(path);

  std::string test;
  while (std::getline(queries_file, test)) {
    benchmark::RegisterBenchmark(test.c_str(), BM_Strip, preprocess, test)
        ->Range(1, 1)
        ->Complexity(benchmark::oN);
  }

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
