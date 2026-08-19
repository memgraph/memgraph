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

std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "expansion-benchmark"};

class ExpansionBenchFixture : public benchmark::Fixture {
 protected:
  std::optional<memgraph::system::System> system;
  std::optional<memgraph::query::AllowEverythingAuthChecker> auth_checker;
  std::optional<memgraph::query::InterpreterContext> interpreter_context;
  std::optional<memgraph::query::Interpreter> interpreter;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;
  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state;

  void SetUp(const benchmark::State &state) override {
    repl_state.emplace(std::nullopt);  // No need for a storage directory, since we are not replicating or restoring
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

    auto label = db_acc->storage()->NameToLabel("Starting");

    {
      auto dba = db_acc->Access(memgraph::storage::WRITE);
      for (int i = 0; i < state.range(0); i++) dba->CreateVertex();

      // the fixed part is one vertex expanding to 1000 others
      auto start = dba->CreateVertex();
      MG_ASSERT(start.AddLabel(label).has_value());
      auto edge_type = dba->NameToEdgeType("edge_type");
      for (int i = 0; i < 1000; i++) {
        auto dest = dba->CreateVertex();
        MG_ASSERT(dba->CreateEdge(&start, &dest, edge_type).has_value());
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

  void TearDown(const benchmark::State &) override {
    interpreter = std::nullopt;
    interpreter_context = std::nullopt;
    db_gk.reset();
    auth_checker.reset();
    system.reset();
    std::filesystem::remove_all(data_directory);
  }
};

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Match)(benchmark::State &state) {
  auto query = "MATCH (s:Starting) return s";

  while (state.KeepRunning()) {
    ResultStreamFaker results(interpreter->current_db_.db_acc_->get()->storage());
    interpreter->Prepare(query, memgraph::query::no_params_fn, {});
    interpreter->PullAll(&results);
  }
}

BENCHMARK_REGISTER_F(ExpansionBenchFixture, Match)
    ->RangeMultiplier(1024)
    ->Range(1, 1 << 20)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Expand)(benchmark::State &state) {
  auto query = "MATCH (s:Starting) WITH s MATCH (s)--(d) RETURN count(d)";

  while (state.KeepRunning()) {
    ResultStreamFaker results(interpreter->current_db_.db_acc_->get()->storage());
    interpreter->Prepare(query, memgraph::query::no_params_fn, {});
    interpreter->PullAll(&results);
  }
}

BENCHMARK_REGISTER_F(ExpansionBenchFixture, Expand)
    ->RangeMultiplier(1024)
    ->Range(1, 1 << 20)
    ->Unit(benchmark::kMillisecond);

// Layers fully connected to the next, so `width ^ kLayers` distinct paths - plenty of deviations.
class KShortestBenchFixture : public benchmark::Fixture {
 protected:
  static constexpr int kLayers = 3;

  // One component means a single input row, measuring the inner Yen search alone; more than one also
  // prices the per-row work - clearing the memo and reusing the adjacency cache.
  virtual int PairCount() const { return 1; }

  std::optional<memgraph::system::System> system;
  std::optional<memgraph::query::AllowEverythingAuthChecker> auth_checker;
  std::optional<memgraph::query::InterpreterContext> interpreter_context;
  std::optional<memgraph::query::Interpreter> interpreter;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;
  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state;

  void SetUp(const benchmark::State &state) override {
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

    auto source_label = db_acc->storage()->NameToLabel("Source");
    auto target_label = db_acc->storage()->NameToLabel("Target");

    {
      auto dba = db_acc->Access(memgraph::storage::WRITE);
      auto edge_type = dba->NameToEdgeType("edge_type");
      auto id_property = dba->NameToProperty("id");
      auto pair_property = dba->NameToProperty("pair");
      int64_t next_id = 0;
      auto make_vertex = [&] {
        auto vertex = dba->CreateVertex();
        MG_ASSERT(vertex.SetProperty(id_property, memgraph::storage::PropertyValue(next_id++)).has_value());
        return vertex;
      };

      // The components are disjoint, so `pair` correlates each source with its own target.
      for (int pair = 0; pair < PairCount(); ++pair) {
        auto tag_pair = [&](memgraph::storage::VertexAccessor &vertex) {
          MG_ASSERT(vertex.SetProperty(pair_property, memgraph::storage::PropertyValue(pair)).has_value());
        };

        auto source = make_vertex();
        MG_ASSERT(source.AddLabel(source_label).has_value());
        tag_pair(source);
        std::vector<memgraph::storage::VertexAccessor> previous_layer{source};

        for (int layer = 0; layer < kLayers; ++layer) {
          std::vector<memgraph::storage::VertexAccessor> current_layer;
          for (int i = 0; i < state.range(0); ++i) current_layer.push_back(make_vertex());
          for (auto &from : previous_layer) {
            for (auto &to : current_layer) MG_ASSERT(dba->CreateEdge(&from, &to, edge_type).has_value());
          }
          previous_layer = std::move(current_layer);
        }

        auto target = make_vertex();
        MG_ASSERT(target.AddLabel(target_label).has_value());
        tag_pair(target);
        for (auto &from : previous_layer) MG_ASSERT(dba->CreateEdge(&from, &target, edge_type).has_value());
      }

      MG_ASSERT(dba->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    for (auto label : {source_label, target_label}) {
      auto unique_acc = db_acc->UniqueAccess();
      MG_ASSERT(unique_acc->CreateIndex(label).has_value());
      MG_ASSERT(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    interpreter.emplace(&*interpreter_context, std::move(db_acc));
    interpreter->SetUser(auth_checker->GenQueryUser(std::nullopt, {}));
  }

  void TearDown(const benchmark::State &) override {
    interpreter = std::nullopt;
    interpreter_context = std::nullopt;
    db_gk.reset();
    auth_checker.reset();
    system.reset();
    std::filesystem::remove_all(data_directory);
  }

  void RunQuery(benchmark::State &state, const char *query) {
    while (state.KeepRunning()) {
      ResultStreamFaker results(interpreter->current_db_.db_acc_->get()->storage());
      interpreter->Prepare(query, memgraph::query::no_params_fn, {});
      interpreter->PullAll(&results);
    }
  }
};

BENCHMARK_DEFINE_F(KShortestBenchFixture, KShortest)(benchmark::State &state) {
  RunQuery(state, "MATCH (s:Source), (t:Target) WITH s, t MATCH (s)-[*KSHORTEST |20]->(t) RETURN count(*)");
}

BENCHMARK_REGISTER_F(KShortestBenchFixture, KShortest)->RangeMultiplier(2)->Range(2, 8)->Unit(benchmark::kMillisecond);

// The same search with an always-true lambda: the delta against `KShortest` is the predicate's cost.
BENCHMARK_DEFINE_F(KShortestBenchFixture, KShortestFiltered)(benchmark::State &state) {
  RunQuery(state,
           "MATCH (s:Source), (t:Target) WITH s, t MATCH (s)-[*KSHORTEST |20 (r, n | n.id >= 0)]->(t) RETURN "
           "count(*)");
}

BENCHMARK_REGISTER_F(KShortestBenchFixture, KShortestFiltered)
    ->RangeMultiplier(2)
    ->Range(2, 8)
    ->Unit(benchmark::kMillisecond);

// Several pairs through one cursor - the shape a real query takes, and the only way to price the
// per-row work: the memo is cleared per row while the adjacency cache is not.
class KShortestMultiRowBenchFixture : public KShortestBenchFixture {
 protected:
  int PairCount() const override { return 6; }
};

// No `|k`, so master and this branch do the same search; with a limit they would not, since it is
// now per row.
BENCHMARK_DEFINE_F(KShortestMultiRowBenchFixture, KShortestMultiRow)(benchmark::State &state) {
  RunQuery(state,
           "MATCH (s:Source), (t:Target) WHERE s.pair = t.pair WITH s, t "
           "MATCH (s)-[*KSHORTEST]->(t) RETURN count(*)");
}

BENCHMARK_REGISTER_F(KShortestMultiRowBenchFixture, KShortestMultiRow)->Arg(2)->Arg(4)->Unit(benchmark::kMillisecond);

// The delta against `KShortestMultiRow` is the predicate plus the memo, once per input row.
BENCHMARK_DEFINE_F(KShortestMultiRowBenchFixture, KShortestMultiRowFiltered)(benchmark::State &state) {
  RunQuery(state,
           "MATCH (s:Source), (t:Target) WHERE s.pair = t.pair WITH s, t "
           "MATCH (s)-[*KSHORTEST (r, n | n.id >= 0)]->(t) RETURN count(*)");
}

BENCHMARK_REGISTER_F(KShortestMultiRowBenchFixture, KShortestMultiRowFiltered)
    ->Arg(2)
    ->Arg(4)
    ->Unit(benchmark::kMillisecond);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
