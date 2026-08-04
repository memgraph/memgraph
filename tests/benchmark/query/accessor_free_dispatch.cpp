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

// Measures the cost of the accessor-free classification that Interpreter::Prepare runs for every
// CypherQuery.
//
// The interesting number is NOT how fast the accessor-free queries are -- those skip a whole storage
// transaction and are obviously cheaper. It is whether the added classification slows down the
// queries that do NOT participate: every normal Cypher query now runs the recognizers and is rejected
// by them. `NormalMatch` and `NormalCallProcedure` are the regression guards. `NormalCallProcedure` is
// the worst case for rejection depth: it is a CallProcedure that passes every cheap AST check and is
// only rejected by the trailing-clause count, so it probes how far a non-participating query gets.

#include <benchmark/benchmark.h>

#include <filesystem>
#include <string>

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/synchronized.hpp"

namespace {

constexpr auto kNoHandler = nullptr;

// Owns a real in-memory database plus an Interpreter, mirroring the InterpreterTest fixture so the
// measured path is the production Prepare/Pull path rather than a stub.
class InterpreterHarness {
 public:
  InterpreterHarness() = default;

  ~InterpreterHarness() { std::filesystem::remove_all(data_directory_); }

  InterpreterHarness(const InterpreterHarness &) = delete;
  InterpreterHarness &operator=(const InterpreterHarness &) = delete;
  InterpreterHarness(InterpreterHarness &&) = delete;
  InterpreterHarness &operator=(InterpreterHarness &&) = delete;

  void Run(const std::string &query) { interpreter_.Interpret(query); }

 private:
  std::filesystem::path data_directory_{std::filesystem::temp_directory_path() /
                                        "MG_benchmark_query_accessor_free_dispatch"};

  memgraph::storage::Config config_{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory_;
    config.disk.main_storage_directory = config.durability.storage_directory / "disk";
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state_{
      memgraph::storage::ReplicationStateRootPath(config_)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk_{config_};
  memgraph::dbms::DatabaseAccess db_{[&] {
    auto db_acc_opt = db_gk_.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    return *db_acc_opt;
  }()};

  memgraph::system::System system_state_;
  memgraph::query::InterpreterContext interpreter_context_{{},
                                                           nullptr,
                                                           nullptr,
                                                           kNoHandler,
                                                           &repl_state_,
                                                           system_state_,
                                                           nullptr
#ifdef MG_ENTERPRISE
                                                           ,
                                                           nullptr,
                                                           nullptr
#endif
  };

  InterpreterFaker interpreter_{&interpreter_context_, db_};
};

void RunQuery(benchmark::State &state, const std::string &query) {
  InterpreterHarness harness;
  // Warm the AST cache so the steady-state measurement reflects dispatch, not parsing.
  harness.Run(query);
  while (state.KeepRunning()) {
    harness.Run(query);
  }
}

// --- accessor-free (participating) shapes ---------------------------------------------------------

void ConstantReturn(benchmark::State &state) { RunQuery(state, "RETURN 1"); }

void BuiltinIntrospection(benchmark::State &state) { RunQuery(state, "CALL mg.procedures() YIELD name"); }

// --- normal (non-participating) shapes: the regression guards -------------------------------------

void NormalMatch(benchmark::State &state) { RunQuery(state, "MATCH (n) RETURN n"); }

void NormalArithmeticReturn(benchmark::State &state) { RunQuery(state, "RETURN 1 + 1 AS x"); }

void NormalCallProcedure(benchmark::State &state) { RunQuery(state, "CALL mg.procedures() YIELD name RETURN name"); }

}  // namespace

BENCHMARK(ConstantReturn)->Unit(benchmark::kMicrosecond);
BENCHMARK(BuiltinIntrospection)->Unit(benchmark::kMicrosecond);
BENCHMARK(NormalMatch)->Unit(benchmark::kMicrosecond);
BENCHMARK(NormalArithmeticReturn)->Unit(benchmark::kMicrosecond);
BENCHMARK(NormalCallProcedure)->Unit(benchmark::kMicrosecond);

int main(int argc, char **argv) {
  memgraph::license::global_license_checker.EnableTesting();
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
