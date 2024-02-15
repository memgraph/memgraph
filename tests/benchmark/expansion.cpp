// Copyright 2024 Memgraph Ltd.
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
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/status.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "utils/logging.hpp"

std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "expansion-benchmark"};

class ExpansionBenchFixture : public benchmark::Fixture {
 protected:
  std::optional<memgraph::system::System> system;
  std::optional<memgraph::query::InterpreterContext> interpreter_context;
  std::optional<memgraph::query::Interpreter> interpreter;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;
  std::optional<memgraph::replication::ReplicationState> repl_state;

  void SetUp(const benchmark::State &state) override {
    repl_state.emplace(std::nullopt);  // No need for a storage directory, since we are not replicating or restoring
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.disk.main_storage_directory = data_directory / "disk";
    db_gk.emplace(std::move(config), *repl_state);
    auto db_acc_opt = db_gk->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;

    system.emplace();
    interpreter_context.emplace(memgraph::query::InterpreterConfig{}, nullptr, &repl_state.value(), *system
#ifdef MG_ENTERPRISE
                                ,
                                nullptr
#endif
    );

    auto label = db_acc->storage()->NameToLabel("Starting");

    {
      auto dba = db_acc->Access();
      for (int i = 0; i < state.range(0); i++) dba->CreateVertex();

      // the fixed part is one vertex expanding to 1000 others
      auto start = dba->CreateVertex();
      MG_ASSERT(start.AddLabel(label).HasValue());
      auto edge_type = dba->NameToEdgeType("edge_type");
      for (int i = 0; i < 1000; i++) {
        auto dest = dba->CreateVertex();
        MG_ASSERT(dba->CreateEdge(&start, &dest, edge_type).HasValue());
      }
      MG_ASSERT(!dba->Commit().HasError());
    }

    {
      auto unique_acc = db_acc->UniqueAccess();
      MG_ASSERT(!unique_acc->CreateIndex(label).HasError());
    }

    interpreter.emplace(&*interpreter_context, std::move(db_acc));
  }

  void TearDown(const benchmark::State &) override {
    interpreter = std::nullopt;
    interpreter_context = std::nullopt;
    system.reset();
    db_gk.reset();
    std::filesystem::remove_all(data_directory);
  }
};

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Match)(benchmark::State &state) {
  auto query = "MATCH (s:Starting) return s";

  while (state.KeepRunning()) {
    ResultStreamFaker results(interpreter->current_db_.db_acc_->get()->storage());
    interpreter->Prepare(query, {}, {});
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
    interpreter->Prepare(query, {}, {});
    interpreter->PullAll(&results);
  }
}

BENCHMARK_REGISTER_F(ExpansionBenchFixture, Expand)
    ->RangeMultiplier(1024)
    ->Range(1, 1 << 20)
    ->Unit(benchmark::kMillisecond);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
