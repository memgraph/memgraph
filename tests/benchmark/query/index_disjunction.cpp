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

// An indexed label disjunction, one benchmark per branch kind: label branches read once, and
// label-property branches sought for every input row. Parsing and planning happen once, outside the loop.

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>

#include "query/frontend/ast/cypher_main_visitor.hpp"

#include <benchmark/benchmark.h>

// planner.hpp has to come before the parser headers: json.hpp needs libc's EOF macro, which antlr hides.
#include "query/interpret/frame.hpp"
#include "query/parameters.hpp"
#include "query/plan/planner.hpp"

#include "metrics/metric_handles.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

namespace {

namespace ms = memgraph::storage;
namespace mq = memgraph::query;

constexpr int64_t kPerLabel = 200'000;
constexpr int64_t kBoth = 40'000;
constexpr int64_t kValues = 1000;

memgraph::metrics::DatabaseMetricHandles &BenchmarkMetricHandles() {
  static memgraph::metrics::DatabaseMetricHandles handles;
  return handles;
}

/// `kPerLabel` vertices :A, `kPerLabel` :B and `kBoth` :A:B, each with p = i % kValues, indexed on :A, :B, :A(p)
/// and :B(p). Built once for the whole run.
ms::Storage &TheGraph() {
  static std::unique_ptr<ms::Storage> const db = [] {
    std::unique_ptr<ms::Storage> db = std::make_unique<ms::InMemoryStorage>();
    auto label_a = db->NameToLabel("A");
    auto label_b = db->NameToLabel("B");
    auto prop_p = db->NameToProperty("p");
    {
      auto acc = db->Access(ms::WRITE);
      auto add = [&](std::initializer_list<ms::LabelId> labels, int64_t count) {
        for (int64_t i = 0; i < count; ++i) {
          auto vertex = acc->CreateVertex();
          for (auto label : labels) {
            if (!vertex.AddLabel(label).has_value()) std::abort();
          }
          if (!vertex.SetProperty(prop_p, ms::PropertyValue(i % kValues)).has_value()) std::abort();
        }
      };
      add({label_a}, kPerLabel);
      add({label_b}, kPerLabel);
      add({label_a, label_b}, kBoth);
      if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) std::abort();
    }
    for (auto label : {label_a, label_b}) {
      auto unique_acc = db->UniqueAccess();
      if (!unique_acc->CreateIndex(label).has_value()) std::abort();
      if (!unique_acc->CreateIndex(label, {prop_p}).has_value()) std::abort();
      if (!unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) std::abort();
    }
    db->FreeMemory();
    return db;
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

void RunQuery(benchmark::State &state, std::string const &query) {
  auto storage_dba = TheGraph().Access(ms::READ);
  mq::DbAccessor dba(storage_dba.get());

  mq::AstStorage ast;
  mq::Parameters parameters;
  auto *cypher_query = ParseCypherQuery(query, &ast);
  auto symbol_table = mq::MakeSymbolTable(cypher_query);
  auto planning_context = mq::plan::MakePlanningContext(&ast, &symbol_table, cypher_query, &dba);
  auto plan_and_cost = mq::plan::MakeLogicalPlan(&planning_context, parameters, false);

  memgraph::utils::MonotonicBufferResource per_pull_memory{mq::kExecutionMemoryBlockSize};
  mq::EvaluationContext evaluation_context{&per_pull_memory};
  evaluation_context.properties = mq::NamesToProperties(ast.properties_, &dba);
  evaluation_context.labels = mq::NamesToLabels(ast.labels_, &dba);

  for (auto _ : state) {
    mq::ExecutionContext execution_context{.db_accessor = &dba,
                                           .symbol_table = symbol_table,
                                           .evaluation_context = evaluation_context,
                                           .metric_handles = &BenchmarkMetricHandles()};
    memgraph::utils::MonotonicBufferResource memory{mq::kExecutionMemoryBlockSize};
    mq::Frame frame(symbol_table.max_position(), &memory);
    auto cursor = plan_and_cost.plan->MakeCursor(&memory, BenchmarkMetricHandles());
    while (cursor->Pull(frame, execution_context)) per_pull_memory.Release();
  }
}

void LabelBranches(benchmark::State &state) { RunQuery(state, "MATCH (n:A|B) RETURN count(*) AS c"); }

void PropertyBranchesPerInputRow(benchmark::State &state) {
  RunQuery(state, "UNWIND range(1, 2000) AS x MATCH (n:A|B {p: x % 1000}) RETURN count(*) AS c");
}

BENCHMARK(LabelBranches)->Unit(benchmark::kMillisecond);
BENCHMARK(PropertyBranchesPerInputRow)->Unit(benchmark::kMillisecond);

}  // namespace

BENCHMARK_MAIN();
