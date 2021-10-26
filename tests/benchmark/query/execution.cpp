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

#include <random>
#include <string>

#include <benchmark/benchmark.h>

//////////////////////////////////////////////////////
// THIS INCLUDE SHOULD ALWAYS COME BEFORE THE
// OTHER INCLUDES
// "planner.hpp" includes json.hpp which uses libc's
// EOF macro while in the other includes
// <antlr4-runtime.h> is included which contains a static
// variable of the same name, EOF.
// This hides the definition of the macro which causes
// the compilation to fail.
#include "query/plan/planner.hpp"
//////////////////////////////////////////////////////
#include "communication/result_stream_faker.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

// The following classes are wrappers for utils::MemoryResource, so that we can
// use BENCHMARK_TEMPLATE

class MonotonicBufferResource final {
  utils::MonotonicBufferResource memory_{query::kExecutionMemoryBlockSize};

 public:
  utils::MemoryResource *get() { return &memory_; }

  void Reset() { memory_.Release(); }
};

class NewDeleteResource final {
 public:
  utils::MemoryResource *get() { return utils::NewDeleteResource(); }

  void Reset() {}
};

class PoolResource final {
  utils::PoolResource memory_{128, 4 * 1024};

 public:
  utils::MemoryResource *get() { return &memory_; }

  void Reset() { memory_.Release(); }
};

static void AddVertices(storage::Storage *db, int vertex_count) {
  auto dba = db->Access();
  for (int i = 0; i < vertex_count; i++) dba.CreateVertex();
  MG_ASSERT(!dba.Commit().HasError());
}

static const char *kStartLabel = "start";

static void AddStarGraph(storage::Storage *db, int spoke_count, int depth) {
  {
    auto dba = db->Access();
    auto center_vertex = dba.CreateVertex();
    MG_ASSERT(center_vertex.AddLabel(dba.NameToLabel(kStartLabel)).HasValue());
    for (int i = 0; i < spoke_count; ++i) {
      auto prev_vertex = center_vertex;
      for (int j = 0; j < depth; ++j) {
        auto dest = dba.CreateVertex();
        MG_ASSERT(dba.CreateEdge(&prev_vertex, &dest, dba.NameToEdgeType("Type")).HasValue());
        prev_vertex = dest;
      }
    }
    MG_ASSERT(!dba.Commit().HasError());
  }
  MG_ASSERT(db->CreateIndex(db->NameToLabel(kStartLabel)));
}

static void AddTree(storage::Storage *db, int vertex_count) {
  {
    auto dba = db->Access();
    std::vector<storage::VertexAccessor> vertices;
    vertices.reserve(vertex_count);
    auto root = dba.CreateVertex();
    MG_ASSERT(root.AddLabel(dba.NameToLabel(kStartLabel)).HasValue());
    vertices.push_back(root);
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
    std::mt19937_64 rg(42);
    for (int i = 1; i < vertex_count; ++i) {
      auto v = dba.CreateVertex();
      std::uniform_int_distribution<> dis(0U, vertices.size() - 1U);
      auto &parent = vertices.at(dis(rg));
      MG_ASSERT(dba.CreateEdge(&parent, &v, dba.NameToEdgeType("Type")).HasValue());
      vertices.push_back(v);
    }
    MG_ASSERT(!dba.Commit().HasError());
  }
  MG_ASSERT(db->CreateIndex(db->NameToLabel(kStartLabel)));
}

static query::CypherQuery *ParseCypherQuery(const std::string &query_string, query::AstStorage *ast) {
  query::frontend::ParsingContext parsing_context;
  parsing_context.is_query_cached = false;
  query::frontend::opencypher::Parser parser(query_string);
  // Convert antlr4 AST into Memgraph AST.
  query::frontend::CypherMainVisitor cypher_visitor(parsing_context, ast);
  cypher_visitor.visit(parser.tree());
  return utils::Downcast<query::CypherQuery>(cypher_visitor.query());
};

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void Distinct(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddVertices(&db, state.range(0));
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto query_string = "MATCH (s) RETURN DISTINCT s";
  auto *cypher_query = ParseCypherQuery(query_string, &ast);
  auto symbol_table = query::MakeSymbolTable(cypher_query);
  auto context = query::plan::MakePlanningContext(&ast, &symbol_table, cypher_query, &dba);
  auto plan_and_cost = query::plan::MakeLogicalPlan(&context, parameters, false);
  ResultStreamFaker results(&db);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = plan_and_cost.first->MakeCursor(memory.get());
    while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(Distinct, NewDeleteResource)->Range(1024, 1U << 21U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Distinct, MonotonicBufferResource)->Range(1024, 1U << 21U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Distinct, PoolResource)->Range(1024, 1U << 21U)->Unit(benchmark::kMicrosecond);

static query::plan::ExpandVariable MakeExpandVariable(query::EdgeAtom::Type expand_type,
                                                      query::SymbolTable *symbol_table) {
  auto input_symbol = symbol_table->CreateSymbol("input", false);
  auto dest_symbol = symbol_table->CreateSymbol("dest", false);
  auto edge_symbol = symbol_table->CreateSymbol("edge", false);
  auto lambda_node_symbol = symbol_table->CreateSymbol("n", false);
  auto lambda_edge_symbol = symbol_table->CreateSymbol("e", false);
  query::plan::ExpansionLambda filter_lambda;
  filter_lambda.inner_node_symbol = lambda_node_symbol;
  filter_lambda.inner_edge_symbol = lambda_edge_symbol;
  filter_lambda.expression = nullptr;
  return query::plan::ExpandVariable(nullptr, input_symbol, dest_symbol, edge_symbol, expand_type,
                                     query::EdgeAtom::Direction::OUT, {}, false, nullptr, nullptr, false, filter_lambda,
                                     std::nullopt, std::nullopt);
}

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void ExpandVariable(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddStarGraph(&db, state.range(0), state.range(1));
  query::SymbolTable symbol_table;
  auto expand_variable = MakeExpandVariable(query::EdgeAtom::Type::DEPTH_FIRST, &symbol_table);
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = expand_variable.MakeCursor(memory.get());
    for (const auto &v : dba.Vertices(storage::View::OLD, dba.NameToLabel(kStartLabel))) {
      frame[expand_variable.input_symbol_] = query::TypedValue(query::VertexAccessor(v));
      while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(ExpandVariable, NewDeleteResource)
    ->Ranges({{1, 1U << 5U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandVariable, MonotonicBufferResource)
    ->Ranges({{1, 1U << 5U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandVariable, PoolResource)
    ->Ranges({{1, 1U << 5U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void ExpandBfs(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddTree(&db, state.range(0));
  query::SymbolTable symbol_table;
  auto expand_variable = MakeExpandVariable(query::EdgeAtom::Type::BREADTH_FIRST, &symbol_table);
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = expand_variable.MakeCursor(memory.get());
    for (const auto &v : dba.Vertices(storage::View::OLD, dba.NameToLabel(kStartLabel))) {
      frame[expand_variable.input_symbol_] = query::TypedValue(query::VertexAccessor(v));
      while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(ExpandBfs, NewDeleteResource)->Range(512, 1U << 19U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandBfs, MonotonicBufferResource)->Range(512, 1U << 19U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandBfs, PoolResource)->Range(512, 1U << 19U)->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void ExpandShortest(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddTree(&db, state.range(0));
  query::SymbolTable symbol_table;
  auto expand_variable = MakeExpandVariable(query::EdgeAtom::Type::BREADTH_FIRST, &symbol_table);
  expand_variable.common_.existing_node = true;
  auto dest_symbol = expand_variable.common_.node_symbol;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = expand_variable.MakeCursor(memory.get());
    for (const auto &v : dba.Vertices(storage::View::OLD, dba.NameToLabel(kStartLabel))) {
      frame[expand_variable.input_symbol_] = query::TypedValue(query::VertexAccessor(v));
      for (const auto &dest : dba.Vertices(storage::View::OLD, dba.NameToLabel(kStartLabel))) {
        frame[dest_symbol] = query::TypedValue(query::VertexAccessor(dest));
        while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
      }
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(ExpandShortest, NewDeleteResource)->Range(512, 1U << 20U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandShortest, MonotonicBufferResource)->Range(512, 1U << 20U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandShortest, PoolResource)->Range(512, 1U << 20U)->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void ExpandWeightedShortest(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddTree(&db, state.range(0));
  query::SymbolTable symbol_table;
  auto expand_variable = MakeExpandVariable(query::EdgeAtom::Type::WEIGHTED_SHORTEST_PATH, &symbol_table);
  expand_variable.common_.existing_node = true;
  expand_variable.weight_lambda_ =
      query::plan::ExpansionLambda{symbol_table.CreateSymbol("edge", false), symbol_table.CreateSymbol("vertex", false),
                                   ast.Create<query::PrimitiveLiteral>(1)};
  auto dest_symbol = expand_variable.common_.node_symbol;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = expand_variable.MakeCursor(memory.get());
    for (const auto &v : dba.Vertices(storage::View::OLD, dba.NameToLabel(kStartLabel))) {
      frame[expand_variable.input_symbol_] = query::TypedValue(query::VertexAccessor(v));
      for (const auto &dest : dba.Vertices(storage::View::OLD, dba.NameToLabel(kStartLabel))) {
        frame[dest_symbol] = query::TypedValue(query::VertexAccessor(dest));
        while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
      }
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(ExpandWeightedShortest, NewDeleteResource)->Range(512, 1U << 20U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandWeightedShortest, MonotonicBufferResource)
    ->Range(512, 1U << 20U)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(ExpandWeightedShortest, PoolResource)->Range(512, 1U << 20U)->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void Accumulate(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddVertices(&db, state.range(1));
  query::SymbolTable symbol_table;
  auto scan_all = std::make_shared<query::plan::ScanAll>(nullptr, symbol_table.CreateSymbol("v", false));
  std::vector<query::Symbol> symbols;
  symbols.reserve(state.range(0));
  for (int i = 0; i < state.range(0); ++i) {
    symbols.push_back(symbol_table.CreateSymbol(std::to_string(i), false));
  }
  query::plan::Accumulate accumulate(scan_all, symbols,
                                     /* advance_command= */ false);
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = accumulate.MakeCursor(memory.get());
    while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(Accumulate, NewDeleteResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Accumulate, MonotonicBufferResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Accumulate, PoolResource)->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void Aggregate(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddVertices(&db, state.range(1));
  query::SymbolTable symbol_table;
  auto scan_all = std::make_shared<query::plan::ScanAll>(nullptr, symbol_table.CreateSymbol("v", false));
  std::vector<query::Symbol> symbols;
  symbols.reserve(state.range(0));
  std::vector<query::Expression *> group_by;
  group_by.reserve(state.range(0));
  std::vector<query::plan::Aggregate::Element> aggregations;
  aggregations.reserve(state.range(0));
  for (int i = 0; i < state.range(0); ++i) {
    auto sym = symbol_table.CreateSymbol(std::to_string(i), false);
    symbols.push_back(sym);
    group_by.push_back(ast.Create<query::Identifier>(sym.name())->MapTo(sym));
    aggregations.push_back({ast.Create<query::PrimitiveLiteral>(i), nullptr, query::Aggregation::Op::SUM,
                            symbol_table.CreateSymbol("out" + std::to_string(i), false)});
  }
  query::plan::Aggregate aggregate(scan_all, aggregations, group_by, symbols);
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = aggregate.MakeCursor(memory.get());
    frame[symbols.front()] = query::TypedValue(0);  // initial group_by value
    while (cursor->Pull(frame, execution_context)) {
      frame[symbols.front()].ValueInt()++;  // new group_by value
      per_pull_memory.Reset();
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(Aggregate, NewDeleteResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Aggregate, MonotonicBufferResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Aggregate, PoolResource)->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void OrderBy(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddVertices(&db, state.range(1));
  query::SymbolTable symbol_table;
  auto scan_all = std::make_shared<query::plan::ScanAll>(nullptr, symbol_table.CreateSymbol("v", false));
  std::vector<query::Symbol> symbols;
  symbols.reserve(state.range(0));
  // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
  std::mt19937_64 rg(42);
  std::vector<query::SortItem> sort_items;
  sort_items.reserve(state.range(0));
  for (int i = 0; i < state.range(0); ++i) {
    symbols.push_back(symbol_table.CreateSymbol(std::to_string(i), false));
    auto rand_value = utils::MemcpyCast<int64_t>(rg());
    sort_items.push_back({query::Ordering::ASC, ast.Create<query::PrimitiveLiteral>(rand_value)});
  }
  query::plan::OrderBy order_by(scan_all, sort_items, symbols);
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    auto cursor = order_by.MakeCursor(memory.get());
    while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(OrderBy, NewDeleteResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(OrderBy, MonotonicBufferResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(OrderBy, PoolResource)->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void Unwind(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  storage::Storage db;
  AddVertices(&db, state.range(0));
  query::SymbolTable symbol_table;
  auto scan_all = std::make_shared<query::plan::ScanAll>(nullptr, symbol_table.CreateSymbol("v", false));
  auto list_sym = symbol_table.CreateSymbol("list", false);
  auto *list_expr = ast.Create<query::Identifier>("list")->MapTo(list_sym);
  auto out_sym = symbol_table.CreateSymbol("out", false);
  query::plan::Unwind unwind(scan_all, list_expr, out_sym);
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // We need to only set the memory for temporary (per pull) evaluations
  TMemory per_pull_memory;
  query::EvaluationContext evaluation_context{per_pull_memory.get()};
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table, evaluation_context};
    TMemory memory;
    query::Frame frame(symbol_table.max_position(), memory.get());
    frame[list_sym] = query::TypedValue(std::vector<query::TypedValue>(state.range(1)));
    auto cursor = unwind.MakeCursor(memory.get());
    while (cursor->Pull(frame, execution_context)) per_pull_memory.Reset();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(Unwind, NewDeleteResource)->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Unwind, MonotonicBufferResource)
    ->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(Unwind, PoolResource)->Ranges({{4, 1U << 7U}, {512, 1U << 13U}})->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
