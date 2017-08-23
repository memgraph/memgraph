#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>

#include "communication/result_stream_faker.hpp"
#include "database/dbms.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"

class ExpansionBenchFixture : public benchmark::Fixture {
 protected:
  std::experimental::optional<Dbms> dbms_;
  query::Interpreter interpeter_;

  void SetUp(const benchmark::State &state) override {
    if (!dbms_)
      dbms_.emplace();
    auto dba = dbms_->active();
    for (int i = 0; i < state.range(0); i++) dba->InsertVertex();

    // the fixed part is one vertex expanding to 1000 others
    auto start = dba->InsertVertex();
    start.add_label(dba->Label("Start"));
    auto edge_type = dba->EdgeType("edge_type");
    for (int i = 0; i < 1000; i++) {
      auto dest = dba->InsertVertex();
      dba->InsertEdge(start, dest, edge_type);
    }
    dba->Commit();
  }

  void TearDown(const benchmark::State &) override {
    auto dba = dbms_->active();
    for (auto vertex : dba->Vertices(false)) dba->DetachRemoveVertex(vertex);
    dba->Commit();
  }
};

// BENCHMARK_DEFINE_F(ExpansionBenchFixture, Match)(benchmark::State &state) {
//   auto query = "MATCH (s:Start) return s";
//   auto dba = dbms_->active();
//   while (state.KeepRunning()) {
//     ResultStreamFaker results;
//     interpeter_.Interpret(query, *dba, results, {});
//   }
// }
// 
// BENCHMARK_REGISTER_F(ExpansionBenchFixture, Match)
//     ->RangeMultiplier(1024)
//     ->Range(1, 1 << 20)
//     ->Unit(benchmark::kMillisecond);

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Expand)(benchmark::State &state) {
  auto query = "MATCH (s:Start) WITH s MATCH (s)--(d) RETURN count(d)";
  auto dba = dbms_->active();
  while (state.KeepRunning()) {
    ResultStreamFaker results;
    interpeter_.Interpret(query, *dba, results, {});
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
