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

// What the frontend costs for one query, which is what a cache miss pays. Measured in two parts: the ANTLR
// parse alone, and the parse plus the walk that builds the AST.
//
// The shapes below isolate the label-expression grammar, which parses a disjunction with a non-greedy loop
// over '|'. What that costs depends on the parser's prediction strategy, so read these against the strategy
// in use: under a single LL pass a label test beside a comprehension's projection is the expensive shape,
// while a parser that tries SLL first parses that one outright and instead pays twice for the shapes SLL
// cannot parse, which are the ones naming a top-level disjunction.

#include <benchmark/benchmark.h>

#include <string>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/stripped.hpp"
#include "query/parameters.hpp"
#include "storage/v2/property_value.hpp"

namespace {

struct Shape {
  const char *name;
  const char *query;
  /// A label named by `$p`, which the parser resolves while parsing. The shapes that use one are the ones a
  /// query cache never holds, so they pay everything below on every execution rather than once.
  bool names_a_parameter{false};
};

// clang-format off
const Shape kShapes[] = {
    {"plain_label",            "MATCH (n:A) RETURN n"},
    {"label_chain",            "MATCH (n:A:B:C) RETURN n"},
    {"label_conjunction",      "MATCH (n:A&B&C) RETURN n"},
    {"label_disjunction",      "MATCH (n:A|B|C) RETURN n"},
    {"label_mixed",            "MATCH (n:A&!B|C) RETURN n"},
    {"label_wildcard",         "MATCH (n:%) RETURN n"},
    // A pipe means two things here, so the pair says what telling them apart costs.
    {"comprehension_plain",    "MATCH (n) RETURN [x IN [n] WHERE x.p | x] AS r"},
    {"comprehension_label",    "MATCH (n) RETURN [x IN [n] WHERE x:A|B | x] AS r"},
    // Which part of a label expression costs: a conjunction leaves the pipe unambiguous, and parenthesising
    // the disjunction settles it before the comprehension's own pipe is reached. The parenthesised one is the
    // shape an SLL-first parser is least able to parse in one pass.
    {"comprehension_label_and",   "MATCH (n) RETURN [x IN [n] WHERE x:A&B | x] AS r"},
    {"comprehension_label_paren", "MATCH (n) RETURN [x IN [n] WHERE x:(A|B) | x] AS r"},
    {"comprehension_label_bare",  "MATCH (n) RETURN [x IN [n] WHERE x:A | x] AS r"},
    // Without a projection there is no second meaning for a pipe to have, and outside a comprehension there
    // is no pipe at all. Together these say whether the cost is the ambiguity or the label test itself.
    {"comprehension_no_pipe",     "MATCH (n) RETURN [x IN [n] WHERE x:A] AS r"},
    {"label_test_expression",     "MATCH (n) RETURN n:A AS r"},
    // The same work, for a label named by a parameter. A query cache never holds these, so what the rows
    // above pay once, these pay on every execution.
    {"param_label",            "MATCH (n:$p) RETURN n",      true},
    {"param_label_mixed",      "MATCH (n:A&!$p|C) RETURN n", true},
    // A body big enough to say whether the cost of copying an AST keeps pace with the cost of building it.
    {"long_body",
     "MATCH (n:A)-[e]->(m) WHERE n.a > n.b AND m.c IN n.list "
     "WITH n, m, n.a + n.b AS s, count(*) AS c ORDER BY n.a + n.b, m.c "
     "RETURN n.a, n.b, m.c, s, c ORDER BY s"},
    // A node reached by two paths, which is what the copy below has to arrive at once.
    {"shared_bound",           "MATCH ()-[r*42]->() RETURN r"},
};
// clang-format on

/// Which token index the parser hands `$p` is the lexer's business, so every position carries the same value.
memgraph::query::Parameters LabelParameters() {
  memgraph::query::Parameters parameters;
  for (int position = 0; position < 64; ++position) {
    parameters.Add(position, memgraph::storage::ExternalPropertyValue("B"));
  }
  return parameters;
}

/// The ANTLR parse alone. A parameter costs nothing here: it is resolved by the walk below.
void Parse(benchmark::State &state, std::string query) {
  for (auto _ : state) {
    memgraph::query::frontend::opencypher::Parser parser(query);
    benchmark::DoNotOptimize(parser.tree());
  }
  state.SetItemsProcessed(state.iterations());
}

/// The parse plus the walk that turns the parse tree into Memgraph's AST, which is what one cache miss costs.
void ParseAndBuildAst(benchmark::State &state, std::string query, bool names_a_parameter) {
  for (auto _ : state) {
    memgraph::query::frontend::ParsingContext context;
    context.is_query_cached = false;
    memgraph::query::AstStorage storage;
    auto parameters = names_a_parameter ? LabelParameters() : memgraph::query::Parameters{};
    memgraph::query::frontend::opencypher::Parser parser(query);
    memgraph::query::frontend::CypherMainVisitor visitor(context, &storage, &parameters);
    visitor.visit(parser.tree());
    benchmark::DoNotOptimize(visitor.query());
  }
  state.SetItemsProcessed(state.iterations());
}

/// Copying the built AST into a fresh storage. A query the cache holds pays this on every execution to hand
/// the caller its own copy; read it against the parse above to see what it costs a query the cache never holds.
void CopyAst(benchmark::State &state, std::string query, bool names_a_parameter) {
  memgraph::query::frontend::ParsingContext context;
  context.is_query_cached = false;
  memgraph::query::AstStorage storage;
  auto parameters = names_a_parameter ? LabelParameters() : memgraph::query::Parameters{};
  memgraph::query::frontend::opencypher::Parser parser(query);
  memgraph::query::frontend::CypherMainVisitor visitor(context, &storage, &parameters);
  visitor.visit(parser.tree());
  auto *parsed = visitor.query();
  for (auto _ : state) {
    memgraph::query::AstStorage copy;
    benchmark::DoNotOptimize(copy.Copy(parsed));
  }
  state.SetItemsProcessed(state.iterations());
}

/// Stripping runs before either, on every execution, and is what the cache key is built from.
void Strip(benchmark::State &state, std::string query) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(memgraph::query::frontend::StrippedQuery(query));
  }
  state.SetItemsProcessed(state.iterations());
}

}  // namespace

int main(int argc, char **argv) {
  for (const auto &shape : kShapes) {
    benchmark::RegisterBenchmark((std::string{"Parse/"} + shape.name).c_str(), Parse, shape.query)
        ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark(
        (std::string{"ParseAndBuildAst/"} + shape.name).c_str(), ParseAndBuildAst, shape.query, shape.names_a_parameter)
        ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark(
        (std::string{"CopyAst/"} + shape.name).c_str(), CopyAst, shape.query, shape.names_a_parameter)
        ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark((std::string{"Strip/"} + shape.name).c_str(), Strip, shape.query)
        ->Unit(benchmark::kMicrosecond);
  }
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
