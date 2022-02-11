// Copyright 2021 Memgraph Ltd.
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

#include "communication/result_stream_faker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "utils/memory.hpp"

class ExpansionBenchFixture : public benchmark::Fixture {
 protected:
  std::optional<storage::Storage> db;
  std::optional<query::InterpreterContext> interpreter_context;
  std::optional<query::Interpreter> interpreter;
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "expansion-benchmark"};

  void SetUp(const benchmark::State &state) override {
    db.emplace();

    auto label = db->NameToLabel("Starting");

    {
      auto dba = db->Access();
      for (int i = 0; i < state.range(0); i++) dba.CreateVertex();

      // the fixed part is one vertex expanding to 1000 others
      auto start = dba.CreateVertex();
      MG_ASSERT(start.AddLabel(label).HasValue());
      auto edge_type = dba.NameToEdgeType("edge_type");
      for (int i = 0; i < 1000; i++) {
        auto dest = dba.CreateVertex();
        MG_ASSERT(dba.CreateEdge(&start, &dest, edge_type).HasValue());
      }
      MG_ASSERT(!dba.Commit().HasError());
    }

    MG_ASSERT(db->CreateIndex(label));

    interpreter_context.emplace(&*db, query::InterpreterConfig{}, data_directory);
    interpreter.emplace(&*interpreter_context);
  }

  void TearDown(const benchmark::State &) override {
    interpreter = std::nullopt;
    interpreter_context = std::nullopt;
    db = std::nullopt;
    std::filesystem::remove_all(data_directory);
  }
};

BENCHMARK_DEFINE_F(ExpansionBenchFixture, Match)(benchmark::State &state) {
  auto query = "MATCH (s:Starting) return s";

  while (state.KeepRunning()) {
    ResultStreamFaker results(&*db);
    interpreter->Prepare(query, storage::PropertyValue::TMap{utils::NewDeleteResource()}, nullptr);
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
    ResultStreamFaker results(&*db);
    interpreter->Prepare(query, storage::PropertyValue::TMap{utils::NewDeleteResource()}, nullptr);
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
