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

// Measures both halves of the graph-free fast path: what a query saves by skipping its storage
// transaction, and what every other query pays for the analysis that decides it.
//
// The second half is the one that needs watching. Every implicit-transaction Cypher query is now walked
// by the graph-access analysis and, for most queries, rejected by it. `Match` is the common case, where
// the walk stops at the first pattern. `MatchWithLongProjection` is the expensive case: the analysis
// has to walk a wide projection of expressions before the pattern rejects the query anyway.

#include <benchmark/benchmark.h>

#include <filesystem>
#include <string>

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/graph_free.hpp"
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

// Graph-free: skips the storage transaction.
void ConstantReturn(benchmark::State &state) { RunQuery(state, "RETURN 1"); }

void ArithmeticReturn(benchmark::State &state) { RunQuery(state, "RETURN 1 + 1 AS x"); }

void BuiltinIntrospection(benchmark::State &state) { RunQuery(state, "CALL mg.procedures() YIELD name"); }

void BuiltinIntrospectionWithProjection(benchmark::State &state) {
  RunQuery(state, "CALL mg.procedures() YIELD name RETURN count(name) AS c");
}

void Unwind(benchmark::State &state) { RunQuery(state, "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"); }

// The same projections, forced onto the transaction path by a query-level memory limit, which the
// analysis rejects. The difference against the pair above is what skipping the transaction buys.
void ConstantReturnWithTransaction(benchmark::State &state) { RunQuery(state, "RETURN 1 QUERY MEMORY LIMIT 1024 MB"); }

void UnwindWithTransaction(benchmark::State &state) {
  RunQuery(state, "UNWIND [1, 2, 3, 4, 5] AS x RETURN x QUERY MEMORY LIMIT 1024 MB");
}

// Needs the graph: pays for the analysis and is rejected by it.
void Match(benchmark::State &state) { RunQuery(state, "MATCH (n) RETURN n"); }

void MatchWithLongProjection(benchmark::State &state) {
  RunQuery(state, "MATCH (n) RETURN 1 + 1, 2 * 2, 3 - 3, 4 / 4, 5 % 5, 'a', true, null, [1, 2], {k: 1}");
}

// The analysis on its own, against an already-parsed query, so its cost can be read without the rest of
// query execution around it. This is what every query that does not take the fast path pays.
void RunAnalysis(benchmark::State &state, const std::string &query_string) {
  memgraph::query::AstStorage storage;
  memgraph::query::frontend::opencypher::Parser parser{query_string};
  memgraph::query::Parameters parameters;
  memgraph::query::frontend::ParsingContext context{.is_query_cached = true};
  memgraph::query::frontend::CypherMainVisitor visitor{context, &storage, &parameters};
  visitor.visit(parser.tree());
  auto *query = memgraph::utils::Downcast<memgraph::query::CypherQuery>(visitor.query());
  MG_ASSERT(query, "Expected a Cypher query");
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(memgraph::query::IsGraphFree(*query));
  }
}

void AnalysisOfMatch(benchmark::State &state) { RunAnalysis(state, "MATCH (n) RETURN n"); }

void AnalysisOfMatchWithLongProjection(benchmark::State &state) {
  RunAnalysis(state, "MATCH (n) RETURN 1 + 1, 2 * 2, 3 - 3, 4 / 4, 5 % 5, 'a', true, null, [1, 2], {k: 1}");
}

void AnalysisOfConstantReturn(benchmark::State &state) { RunAnalysis(state, "RETURN 1"); }

}  // namespace

BENCHMARK(ConstantReturn)->Unit(benchmark::kMicrosecond);
BENCHMARK(ArithmeticReturn)->Unit(benchmark::kMicrosecond);
BENCHMARK(BuiltinIntrospection)->Unit(benchmark::kMicrosecond);
BENCHMARK(BuiltinIntrospectionWithProjection)->Unit(benchmark::kMicrosecond);
BENCHMARK(Unwind)->Unit(benchmark::kMicrosecond);
BENCHMARK(ConstantReturnWithTransaction)->Unit(benchmark::kMicrosecond);
BENCHMARK(UnwindWithTransaction)->Unit(benchmark::kMicrosecond);
BENCHMARK(Match)->Unit(benchmark::kMicrosecond);
BENCHMARK(MatchWithLongProjection)->Unit(benchmark::kMicrosecond);
BENCHMARK(AnalysisOfMatch)->Unit(benchmark::kNanosecond);
BENCHMARK(AnalysisOfMatchWithLongProjection)->Unit(benchmark::kNanosecond);
BENCHMARK(AnalysisOfConstantReturn)->Unit(benchmark::kNanosecond);

int main(int argc, char **argv) {
  memgraph::license::global_license_checker.EnableTesting();
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
