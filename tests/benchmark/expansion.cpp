#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>

#include "communication/result_stream_faker.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"

class ExpansionBenchFixture : public benchmark::Fixture {
 protected:
  // GraphDb shouldn't be global constructed/destructed. See documentation in
  // database/graph_db.hpp for details.
  std::experimental::optional<GraphDb> db_;
  query::Interpreter interpeter_;

  void SetUp(const benchmark::State &state) override {
    db_.emplace();
    GraphDbAccessor dba(*db_);
    for (int i = 0; i < state.range(0); i++) dba.InsertVertex();

    // the fixed part is one vertex expanding to 1000 others
    auto start = dba.InsertVertex();
    start.add_label(dba.Label("Start"));
    auto edge_type = dba.EdgeType("edge_type");
    for (int i = 0; i < 1000; i++) {
      auto dest = dba.InsertVertex();
      dba.InsertEdge(start, dest, edge_type);
    }
    dba.Commit();
  }

  void TearDown(const benchmark::State &) override {
    GraphDbAccessor dba(*db_);
    for (auto vertex : dba.Vertices(false)) dba.DetachRemoveVertex(vertex);
    dba.Commit();
    db_ = std::experimental::nullopt;
  }
};

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Match)(benchmark::State &state) {
  auto query = "MATCH (s:Start) return s";
  GraphDbAccessor dba(*db_);
  while (state.KeepRunning()) {
    ResultStreamFaker results;
    interpeter_(query, dba, {}, false).PullAll(results);
  }
}

BENCHMARK_REGISTER_F(ExpansionBenchFixture, Match)
    ->RangeMultiplier(1024)
    ->Range(1, 1 << 20)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Expand)(benchmark::State &state) {
  auto query = "MATCH (s:Start) WITH s MATCH (s)--(d) RETURN count(d)";
  GraphDbAccessor dba(*db_);
  while (state.KeepRunning()) {
    ResultStreamFaker results;
    interpeter_(query, dba, {}, false).PullAll(results);
  }
}

BENCHMARK_REGISTER_F(ExpansionBenchFixture, Expand)
    ->RangeMultiplier(1024)
    ->Range(1, 1 << 20)
    ->Unit(benchmark::kMillisecond);

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
