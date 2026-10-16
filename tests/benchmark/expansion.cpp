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
#include <sys/resource.h>
#include <algorithm>
#include <memory>

#include "communication/result_stream_faker.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
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

/// What an expansion costs in memory rather than in time.
///
/// Minor faults are the direct measure: a collection built, read once and discarded still costs
/// the kernel a page supplied and zeroed for every page of it, and that does not show up in a
/// latency figure because the work is the kernel's.
///
/// The peak is read above the level the graph itself rests at, so it reports what the query added
/// rather than what the fixture built. It is a high-water mark over the process, so run one arm per
/// process to attribute it to that arm.
class ExpansionCost {
 public:
  static ExpansionCost Take() {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    return ExpansionCost{memgraph::utils::total_memory_tracker.Amount(), static_cast<int64_t>(usage.ru_minflt)};
  }

  void ReportInto(benchmark::State &state) const {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    state.counters["minor_faults"] =
        benchmark::Counter(static_cast<double>(static_cast<int64_t>(usage.ru_minflt) - faults_at_rest_),
                           benchmark::Counter::kAvgIterations);
    auto const over_resting = memgraph::utils::total_memory_tracker.Peak() - bytes_at_rest_;
    state.counters["peak_bytes_over_resting"] =
        benchmark::Counter(static_cast<double>(std::max<int64_t>(over_resting, 0)));
  }

 private:
  ExpansionCost(int64_t bytes_at_rest, int64_t faults_at_rest)
      : bytes_at_rest_{bytes_at_rest}, faults_at_rest_{faults_at_rest} {}

  int64_t bytes_at_rest_;
  int64_t faults_at_rest_;
};

/// One vertex of a high degree, which is the shape that makes the expansion the whole query: every
/// row it returns is a relationship of the same vertex, so what the query costs is what traversing
/// that vertex costs and nothing else.
class HighDegreeExpansionFixture : public benchmark::Fixture {
 protected:
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

    auto hub_label = db_acc->storage()->NameToLabel("Hub");

    {
      auto dba = db_acc->Access(memgraph::storage::WRITE);
      auto hub = dba->CreateVertex();
      MG_ASSERT(hub.AddLabel(hub_label).has_value());
      auto edge_type = dba->NameToEdgeType("HAS");
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto destination = dba->CreateVertex();
        MG_ASSERT(dba->CreateEdge(&hub, &destination, edge_type).has_value());
      }
      MG_ASSERT(dba->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    {
      auto unique_acc = db_acc->UniqueAccess();
      MG_ASSERT(unique_acc->CreateIndex(hub_label).has_value());
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
    auto const at_rest = ExpansionCost::Take();
    while (state.KeepRunning()) {
      ResultStreamFaker results(interpreter->current_db_.db_acc_->get()->storage());
      interpreter->Prepare(query, memgraph::query::no_params_fn, {});
      interpreter->PullAll(&results);
    }
    at_rest.ReportInto(state);
  }
};

/// Counting the relationships returns one row, so everything the query allocates is the traversal.
BENCHMARK_DEFINE_F(HighDegreeExpansionFixture, CountRelationships)(benchmark::State &state) {
  RunQuery(state, "MATCH (:Hub)-[r:HAS]->() RETURN count(r)");
}

BENCHMARK_REGISTER_F(HighDegreeExpansionFixture, CountRelationships)
    ->RangeMultiplier(8)
    ->Range(1 << 12, 1 << 18)
    ->Unit(benchmark::kMillisecond);

/// The control. It reaches the same vertex through the same index and expands nothing, so a change
/// to how an expansion is built has to leave it where it is.
BENCHMARK_DEFINE_F(HighDegreeExpansionFixture, CountHubsWithoutExpanding)(benchmark::State &state) {
  RunQuery(state, "MATCH (h:Hub) RETURN count(h)");
}

BENCHMARK_REGISTER_F(HighDegreeExpansionFixture, CountHubsWithoutExpanding)
    ->RangeMultiplier(8)
    ->Range(1 << 12, 1 << 18)
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
