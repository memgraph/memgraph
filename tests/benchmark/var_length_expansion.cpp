// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Hot-path benchmark for the variable-length expansion family (DFS / BFS / weighted shortest path)
// over the real graph. Used to confirm the traversal-policy unification introduces no regression on
// the real path (ADR 0005 / issue 43 Stage E). The graph is a deterministic circulant graph so
// every run traverses identical topology.

#include <benchmark/benchmark.h>
#include <memory>

#include "communication/result_stream_faker.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "var-length-expansion-benchmark"};

// A circulant graph: N vertices, each vertex i has out-edges to (i+1)..(i+kDegree) mod N. Uniform
// branching gives a dense, deterministic multi-hop workload.
static constexpr int kNumVertices = 4000;
static constexpr int kDegree = 5;

class VarLengthBenchFixture : public benchmark::Fixture {
 protected:
  std::optional<memgraph::system::System> system;
  std::optional<memgraph::query::AllowEverythingAuthChecker> auth_checker;
  std::optional<memgraph::query::InterpreterContext> interpreter_context;
  std::optional<memgraph::query::Interpreter> interpreter;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;
  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state;

  void SetUp(const benchmark::State & /*state*/) override {
    repl_state.emplace(std::nullopt);
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.disk.main_storage_directory = data_directory / "disk";
    db_gk.emplace(std::move(config));
    auto db_acc_opt = db_gk->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;

    system.emplace();
    auth_checker.emplace();
    interpreter_context.emplace(memgraph::query::InterpreterConfig{},
                                nullptr,
                                nullptr,
                                nullptr,
                                &repl_state.value(),
                                *system,
                                nullptr
#ifdef MG_ENTERPRISE
                                ,
                                nullptr,
                                nullptr
#endif
    );

    auto label = db_acc->storage()->NameToLabel("Start");
    {
      auto dba = db_acc->Access(memgraph::storage::WRITE);
      auto edge_type = dba->NameToEdgeType("E");
      std::vector<memgraph::storage::VertexAccessor> vertices;
      vertices.reserve(kNumVertices);
      for (int i = 0; i < kNumVertices; i++) vertices.push_back(dba->CreateVertex());
      MG_ASSERT(vertices[0].AddLabel(label).has_value());
      for (int i = 0; i < kNumVertices; i++) {
        for (int d = 1; d <= kDegree; d++) {
          auto &dest = vertices[(i + d) % kNumVertices];
          MG_ASSERT(dba->CreateEdge(&vertices[i], &dest, edge_type).has_value());
        }
      }
      MG_ASSERT(dba->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db_acc->UniqueAccess();
      MG_ASSERT(unique_acc->CreateIndex(label).has_value());
      MG_ASSERT(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    interpreter.emplace(&*interpreter_context, std::move(db_acc));
    interpreter->SetUser(auth_checker->GenQueryUser(std::nullopt, {}));
  }

  void TearDown(const benchmark::State & /*state*/) override {
    interpreter = std::nullopt;
    interpreter_context = std::nullopt;
    db_gk.reset();
    auth_checker.reset();
    system.reset();
    std::filesystem::remove_all(data_directory);
  }

  void Run(const char *query, benchmark::State &state) {
    while (state.KeepRunning()) {
      ResultStreamFaker results(interpreter->current_db_.db_acc_->get()->storage());
      interpreter->Prepare(query, memgraph::query::no_params_fn, {});
      interpreter->PullAll(&results);
    }
  }
};

// Depth-first variable-length: explores every distinct-edge path within the bound.
BENCHMARK_DEFINE_F(VarLengthBenchFixture, VarLengthDFS)(benchmark::State &state) {
  Run("MATCH (s:Start)-[*1..4]->(d) RETURN count(d)", state);
}

BENCHMARK_REGISTER_F(VarLengthBenchFixture, VarLengthDFS)->Unit(benchmark::kMillisecond);

// Single-source breadth-first: reaches every node at its shortest depth once.
BENCHMARK_DEFINE_F(VarLengthBenchFixture, BFS)(benchmark::State &state) {
  Run("MATCH (s:Start)-[*BFS 1..10]->(d) RETURN count(d)", state);
}

BENCHMARK_REGISTER_F(VarLengthBenchFixture, BFS)->Unit(benchmark::kMillisecond);

// Weighted shortest path with a unit-weight lambda: Dijkstra over the whole reachable set.
BENCHMARK_DEFINE_F(VarLengthBenchFixture, WeightedShortestPath)(benchmark::State &state) {
  Run("MATCH (s:Start)-[e *wShortest 10 (r, n | 1) w]->(d) RETURN count(d)", state);
}

BENCHMARK_REGISTER_F(VarLengthBenchFixture, WeightedShortestPath)->Unit(benchmark::kMillisecond);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
