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

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/rewrite/pruning_bfs.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

namespace {

// Plan shapes that no Cypher query produces today, so that the rewriter is
// pinned to its own contract rather than to what the planner happens to emit.
class PruningBFSRewriteTest : public ::testing::Test {
 protected:
  AstStorage storage;
  SymbolTable symbol_table;
  Parameters parameters;
  bool read_parameters = false;

  Symbol source_sym = symbol_table.CreateSymbol("source", true);
  Symbol target_sym = symbol_table.CreateSymbol("target", true);
  Symbol edge_sym = symbol_table.CreateSymbol("edges", true);
  Symbol inner_edge_sym = symbol_table.CreateSymbol("inner_edge", false);
  Symbol inner_node_sym = symbol_table.CreateSymbol("inner_node", false);

  std::shared_ptr<ExpandVariable> expand;

  /// Knobs the tests vary; defaults describe a plan the rewrite accepts.
  Expression *lower_bound = nullptr;
  bool deduplicates = true;

  /// Once -> ScanAll -> ExpandVariable -> `above` -> Distinct -> Produce, where
  /// `above` is built from the expansion by the caller.
  std::unique_ptr<LogicalOperator> MakePlan(
      std::function<std::shared_ptr<LogicalOperator>(std::shared_ptr<LogicalOperator>)> const &above) {
    auto scan = std::make_shared<ScanAll>(nullptr, source_sym);
    expand = std::make_shared<ExpandVariable>(scan,
                                              source_sym,
                                              target_sym,
                                              edge_sym,
                                              EdgeAtom::Type::DEPTH_FIRST,
                                              EdgeAtom::Direction::OUT,
                                              std::vector<memgraph::storage::EdgeTypeId>{},
                                              false,
                                              lower_bound,
                                              nullptr,
                                              false,
                                              ExpansionLambda{inner_edge_sym, inner_node_sym, nullptr},
                                              std::nullopt,
                                              std::nullopt,
                                              nullptr);
    std::shared_ptr<LogicalOperator> top = above(expand);
    if (deduplicates) {
      top = std::make_shared<Distinct>(std::move(top), std::vector<Symbol>{target_sym});
    }
    auto *named = storage.Create<NamedExpression>("target", storage.Create<Identifier>("target")->MapTo(target_sym));
    return std::make_unique<Produce>(std::move(top), std::vector<NamedExpression *>{named});
  }

  /// A bound that only the parameters of a particular execution settle, as query
  /// stripping produces for a written-out bound.
  Expression *SuppliedBound(int64_t value) {
    constexpr int kTokenPosition = 0;
    parameters.Add(kTokenPosition, memgraph::storage::ExternalPropertyValue{value});
    return storage.Create<ParameterLookup>(kTokenPosition);
  }

  EdgeAtom::Type RewrittenType(
      std::function<std::shared_ptr<LogicalOperator>(std::shared_ptr<LogicalOperator>)> const &above) {
    auto plan = RewriteWithPruningBFS(MakePlan(above), &symbol_table, parameters, &read_parameters);
    return expand->type_;
  }
};

TEST_F(PruningBFSRewriteTest, RewritesWhenNothingAboveReadsTheEdges) {
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
}

TEST_F(PruningBFSRewriteTest, DoesNotRewriteWhenAFilterExpressionReadsTheEdges) {
  // A Filter carrying only `expression_` still evaluates it, so the symbols it
  // names are read whether or not `all_filters_` describes them.
  auto const type = RewrittenType([this](auto input) {
    auto *reads_edges = storage.Create<Identifier>("edges")->MapTo(edge_sym);
    return std::static_pointer_cast<LogicalOperator>(
        std::make_shared<Filter>(input, std::vector<std::shared_ptr<LogicalOperator>>{}, reads_edges));
  });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, DoesNotRewriteBelowAWriteProcedure) {
  // A write procedure runs once per row, so collapsing duplicate rows changes
  // how many times its side effects happen.
  auto const type = RewrittenType([this](auto input) {
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<plan::CallProcedure>(input,
                                                                                           "mock.write",
                                                                                           std::vector<Expression *>{},
                                                                                           std::vector<std::string>{},
                                                                                           std::vector<Symbol>{},
                                                                                           nullptr,
                                                                                           1UL,
                                                                                           /*is_write=*/true,
                                                                                           /*procedure_id=*/0));
  });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, RewritesBelowAReadProcedure) {
  auto const type = RewrittenType([this](auto input) {
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<plan::CallProcedure>(input,
                                                                                           "mock.read",
                                                                                           std::vector<Expression *>{},
                                                                                           std::vector<std::string>{},
                                                                                           std::vector<Symbol>{},
                                                                                           nullptr,
                                                                                           1UL,
                                                                                           /*is_write=*/false,
                                                                                           /*procedure_id=*/0));
  });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
}

TEST_F(PruningBFSRewriteTest, ASuppliedBoundThatPermitsPruningTiesThePlanToIt) {
  lower_bound = SuppliedBound(1);
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
  EXPECT_TRUE(read_parameters);
}

TEST_F(PruningBFSRewriteTest, ASuppliedBoundThatDeniesPruningAlsoTiesThePlanToIt) {
  // The plan is depth-first either way, but caching it would serve it to the
  // bounds that do permit pruning.
  lower_bound = SuppliedBound(2);
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_TRUE(read_parameters);
}

TEST_F(PruningBFSRewriteTest, ABoundIsNotReadWhenNothingElseAllowsPruning) {
  // Nothing deduplicates, so the bound never decides anything and the plan is
  // the one every execution of this query would get.
  deduplicates = false;
  lower_bound = SuppliedBound(1);
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_FALSE(read_parameters);
}

TEST_F(PruningBFSRewriteTest, AnAbsentBoundLeavesThePlanFitForTheCache) {
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
  EXPECT_FALSE(read_parameters);
}

}  // namespace
