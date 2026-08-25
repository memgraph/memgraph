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

/// What a Distinct costs per column of its key.
///
/// A column whose value follows from another column the key holds cannot separate two rows the rest
/// of the key does not, so a plan may deduplicate without it. What that is worth is the copying and
/// hashing of one value per row, which is what these measure.
///
/// Both widths run in one process against one graph, so the pair is not separated by which build ran
/// them, what the page cache held, or what else the machine was doing.

#include <cstdlib>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/interpreter.hpp"
#include "query/parameters.hpp"
#include "query/plan/planner.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"

namespace {

namespace ms = memgraph::storage;
namespace mq = memgraph::query;

memgraph::metrics::DatabaseMetricHandles &BenchmarkMetricHandles() {
  static memgraph::metrics::DatabaseMetricHandles handles;
  return handles;
}

/// Rows through the Distinct. Large enough that the per-row cost dominates the fixed cost of a pull.
int64_t ItemCount() {
  static int64_t const count = [] {
    auto const *env = std::getenv("MG_DISTINCT_BENCH_ITEMS");
    return env != nullptr ? std::strtoll(env, nullptr, 10) : 200'000;
  }();
  return count;
}

/// Every vertex distinct, so the Distinct keeps every row and pays the full cost of every one. A
/// graph of duplicates would measure the set rejecting rows instead, which is the cheaper path.
ms::Storage &TheGraph() {
  static auto const db = [] {
    auto config = ms::Config{};
    auto storage = std::make_unique<ms::InMemoryStorage>(config);
    storage->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);
    auto dba = storage->Access(ms::WRITE);
    auto const label = dba->NameToLabel("Item");
    auto const p_id = dba->NameToProperty("id");
    auto const p_name = dba->NameToProperty("name");
    for (int64_t i = 0; i != ItemCount(); ++i) {
      auto vertex = dba->CreateVertex();
      MG_ASSERT(vertex.AddLabel(label).has_value());
      MG_ASSERT(vertex.SetProperty(p_id, ms::PropertyValue(i)).has_value());
      MG_ASSERT(vertex.SetProperty(p_name, ms::PropertyValue("item_" + std::to_string(i))).has_value());
    }
    MG_ASSERT(dba->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    return storage;
  }();
  return *db;
}

mq::CypherQuery *ParseCypherQuery(std::string const &query_string, mq::AstStorage *ast) {
  mq::frontend::ParsingContext parsing_context;
  mq::Parameters parameters;
  parsing_context.is_query_cached = false;
  mq::frontend::opencypher::Parser parser(query_string);
  mq::frontend::CypherMainVisitor cypher_visitor(parsing_context, ast, &parameters);
  cypher_visitor.visit(parser.tree());
  return memgraph::utils::Downcast<mq::CypherQuery>(cypher_visitor.query());
}

/// Runs `query`, first forcing the Distinct's key to every column the projection below it produces
/// where `keep_every_column`, which is the key a plan carries before a column is dropped from it.
void RunDistinct(benchmark::State &state, std::string const &query, bool keep_every_column) {
  auto storage_dba = TheGraph().Access(ms::READ);
  mq::DbAccessor dba(storage_dba.get());

  mq::AstStorage ast;
  mq::Parameters parameters;
  auto *cypher_query = ParseCypherQuery(query, &ast);
  auto symbol_table = mq::MakeSymbolTable(cypher_query);
  auto planning_context = mq::plan::MakePlanningContext(&ast, &symbol_table, cypher_query, &dba);
  auto plan_and_cost = mq::plan::MakeLogicalPlan(&planning_context, parameters, false);

  auto *distinct = memgraph::utils::Downcast<mq::plan::Distinct>(plan_and_cost.plan.get());
  MG_ASSERT(distinct, "The plan for this query is expected to be topped by a Distinct");
  auto *produce = memgraph::utils::Downcast<mq::plan::Produce>(distinct->input_.get());
  MG_ASSERT(produce, "The Distinct is expected to sit on the projection");
  if (keep_every_column) distinct->value_symbols_ = produce->OutputSymbols(symbol_table);
  auto const key_width = distinct->value_symbols_.size();

  memgraph::utils::MonotonicBufferResource per_pull_memory{mq::kExecutionMemoryBlockSize};
  mq::EvaluationContext evaluation_context{&per_pull_memory};
  evaluation_context.properties = mq::NamesToProperties(ast.properties_, &dba);
  evaluation_context.labels = mq::NamesToLabels(ast.labels_, &dba);

  int64_t rows = 0;
  for (auto _ : state) {
    mq::ExecutionContext execution_context{.db_accessor = &dba,
                                           .symbol_table = symbol_table,
                                           .evaluation_context = evaluation_context,
                                           .metric_handles = &BenchmarkMetricHandles()};
    memgraph::utils::MonotonicBufferResource memory{mq::kExecutionMemoryBlockSize};
    mq::Frame frame(symbol_table.max_position(), &memory);
    auto cursor = plan_and_cost.plan->MakeCursor(&memory, BenchmarkMetricHandles());
    while (cursor->Pull(frame, execution_context)) {
      ++rows;
      per_pull_memory.Release();
    }
  }
  state.SetItemsProcessed(state.iterations() * ItemCount());
  state.counters["key_width"] = static_cast<double>(key_width);
  state.counters["out_rows"] = static_cast<double>(rows) / static_cast<double>(state.iterations());
}

// ---------------------------------------------------------------- a vertex and its properties ----

constexpr auto kVertexAndProperty = "MATCH (n:Item) RETURN DISTINCT n.id, n";
constexpr auto kVertexAndTwoProperties = "MATCH (n:Item) RETURN DISTINCT n.id, n.name, n";

void VertexAndPropertyWide(benchmark::State &state) { RunDistinct(state, kVertexAndProperty, true); }

void VertexAndPropertyNarrow(benchmark::State &state) { RunDistinct(state, kVertexAndProperty, false); }

void VertexAndTwoPropertiesWide(benchmark::State &state) { RunDistinct(state, kVertexAndTwoProperties, true); }

void VertexAndTwoPropertiesNarrow(benchmark::State &state) { RunDistinct(state, kVertexAndTwoProperties, false); }

// The control: nothing about this key follows from anything else in it, so the two arms plan the
// same key and any gap between them is the measurement rather than the change.
constexpr auto kVertexAlone = "MATCH (n:Item) RETURN DISTINCT n";

void VertexAloneWide(benchmark::State &state) { RunDistinct(state, kVertexAlone, true); }

void VertexAloneNarrow(benchmark::State &state) { RunDistinct(state, kVertexAlone, false); }

BENCHMARK(VertexAndPropertyWide)->Unit(benchmark::kMillisecond);
BENCHMARK(VertexAndPropertyNarrow)->Unit(benchmark::kMillisecond);
BENCHMARK(VertexAndTwoPropertiesWide)->Unit(benchmark::kMillisecond);
BENCHMARK(VertexAndTwoPropertiesNarrow)->Unit(benchmark::kMillisecond);
BENCHMARK(VertexAloneWide)->Unit(benchmark::kMillisecond);
BENCHMARK(VertexAloneNarrow)->Unit(benchmark::kMillisecond);

}  // namespace

int main(int argc, char **argv) {
  benchmark::Initialize(&argc, argv);
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
