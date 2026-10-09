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

  Symbol source_sym = symbol_table.CreateSymbol("source", true);
  Symbol target_sym = symbol_table.CreateSymbol("target", true);
  Symbol edge_sym = symbol_table.CreateSymbol("edges", true);
  Symbol inner_edge_sym = symbol_table.CreateSymbol("inner_edge", false);
  Symbol inner_node_sym = symbol_table.CreateSymbol("inner_node", false);

  std::shared_ptr<ExpandVariable> expand;
  std::shared_ptr<Distinct> distinct;

  /// Knobs the tests vary; defaults describe a plan the rewrite accepts, and
  /// one whose input rows it lets share a single search.
  Expression *lower_bound = nullptr;
  Expression *upper_bound = nullptr;
  Expression *filter_lambda = nullptr;
  EdgeAtom::Direction direction = EdgeAtom::Direction::OUT;
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
                                              direction,
                                              std::vector<memgraph::storage::EdgeTypeId>{},
                                              false,
                                              lower_bound,
                                              upper_bound,
                                              false,
                                              ExpansionLambda{inner_edge_sym, inner_node_sym, filter_lambda},
                                              std::nullopt,
                                              std::nullopt,
                                              nullptr);
    std::shared_ptr<LogicalOperator> top = above(expand);
    distinct = nullptr;
    if (deduplicates) {
      distinct = std::make_shared<Distinct>(std::move(top), std::vector<Symbol>{target_sym});
      top = distinct;
    }
    auto *named = storage.Create<NamedExpression>("target", storage.Create<Identifier>("target")->MapTo(target_sym));
    return std::make_unique<Produce>(std::move(top), std::vector<NamedExpression *>{named});
  }

  Expression *Edges() { return storage.Create<Identifier>("edges")->MapTo(edge_sym); }

  /// `WHEN predicate THEN RETURN column AS x` over @p input, with @p folds as the predicate's folds.
  std::shared_ptr<LogicalOperator> ConditionalOver(std::shared_ptr<LogicalOperator> input, Expression *predicate,
                                                   Expression *column,
                                                   std::vector<std::shared_ptr<LogicalOperator>> folds = {}) {
    auto x = symbol_table.CreateSymbol("x", true);
    auto *named = storage.Create<NamedExpression>("x", column)->MapTo(x);
    return std::make_shared<Conditional>(
        std::move(input),
        std::vector<Conditional::Branch>{
            {.predicate = predicate,
             .pattern_filters = std::move(folds),
             .plan = std::make_shared<Produce>(nullptr, std::vector<NamedExpression *>{named})}},
        std::vector<Symbol>{x});
  }

  /// A bound that only the parameters of a particular execution settle, as query
  /// stripping produces for a written-out bound. Its value is out of the plan's
  /// reach, but it reads no symbol, so the cursor can settle it.
  Expression *SuppliedBound() { return storage.Create<ParameterLookup>(0); }

  /// A bound read from the row being expanded, which no plan can settle.
  Expression *BoundFromTheRow() {
    auto *identifier = storage.Create<Identifier>("source")->MapTo(source_sym);
    return storage.Create<PropertyLookup>(identifier, storage.GetPropertyIx("depth"));
  }

  EdgeAtom::Type RewrittenType(
      std::function<std::shared_ptr<LogicalOperator>(std::shared_ptr<LogicalOperator>)> const &above) {
    auto plan = RewriteWithPruningBFS(MakePlan(above), &symbol_table);
    return expand->type_;
  }

  /// Whether the rewrite let the expansion's input rows share one search.
  bool SharesOneSearch(std::function<std::shared_ptr<LogicalOperator>(std::shared_ptr<LogicalOperator>)> const &above) {
    auto plan = RewriteWithPruningBFS(MakePlan(above), &symbol_table);
    EXPECT_EQ(expand->type_, EdgeAtom::Type::PRUNING_BFS) << "sharing is only ever offered to a pruning BFS";
    return expand->group_sources_;
  }

  /// Whether the rewrite found the Distinct above the expansion to have nothing
  /// left to deduplicate.
  bool DistinctIsSpentOn(
      std::function<std::shared_ptr<LogicalOperator>(std::shared_ptr<LogicalOperator>)> const &above) {
    auto plan = RewriteWithPruningBFS(MakePlan(above), &symbol_table);
    EXPECT_TRUE(distinct) << "the plan under test is built without one";
    return distinct && distinct->input_is_distinct_;
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

TEST_F(PruningBFSRewriteTest, DoesNotRewriteWhenAConditionalPredicateReadsTheEdges) {
  // CALL (edges) { WHEN edges IS NULL THEN RETURN 1 AS x }
  auto const type = RewrittenType([this](auto input) {
    return ConditionalOver(input, storage.Create<IsNullOperator>(Edges()), storage.Create<PrimitiveLiteral>(1));
  });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, DoesNotRewriteWhenAConditionalBranchReadsTheEdges) {
  // CALL (edges) { WHEN true THEN RETURN edges AS x }
  auto const type = RewrittenType(
      [this](auto input) { return ConditionalOver(input, storage.Create<PrimitiveLiteral>(true), Edges()); });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, DoesNotRewriteWhenAConditionalFoldReadsTheEdges) {
  // CALL (edges) { WHEN EXISTS { WITH edges WHERE edges IS NULL } THEN RETURN 1 AS x }: only the fold reads the edges.
  auto const type = RewrittenType([this](auto input) {
    auto fold_input = std::make_shared<Filter>(std::make_shared<Once>(),
                                               std::vector<std::shared_ptr<LogicalOperator>>{},
                                               storage.Create<IsNullOperator>(Edges()));
    auto exists_sym = symbol_table.CreateAnonymousSymbol();
    auto fold = std::make_shared<EvaluatePatternFilter>(fold_input, exists_sym, Fold::kBool);
    return ConditionalOver(
        input, storage.Create<Identifier>("exists")->MapTo(exists_sym), storage.Create<PrimitiveLiteral>(1), {fold});
  });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, RewritesBelowAConditionalThatReadsNoEdges) {
  // CALL () { WHEN true THEN RETURN 1 AS x }
  auto const type = RewrittenType([this](auto input) {
    return ConditionalOver(input, storage.Create<PrimitiveLiteral>(true), storage.Create<PrimitiveLiteral>(1));
  });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
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
                                                                                           GraphAccess::Write,
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
                                                                                           GraphAccess::Read,
                                                                                           /*procedure_id=*/0));
  });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
}

// === The bound belongs to the cursor, not the plan ===

TEST_F(PruningBFSRewriteTest, MarksAnExpansionWhoseBoundOnlyTheParametersSettle) {
  // The value is out of reach here, and does not need to be: the cursor reads it
  // before it pulls a row, so leaving the plan free of it costs nothing.
  lower_bound = SuppliedBound();
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
}

TEST_F(PruningBFSRewriteTest, LeavesAnExpansionWhoseBoundTheRowSupplies) {
  // A property lookup is neither a literal nor a parameter, so the bound is not
  // resolvable and the expansion stays depth-first.
  lower_bound = BoundFromTheRow();
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, LeavesAnExpansionWhoseBoundIsANonTrivialExpression) {
  // An addition of two literals reads no symbol but is neither a literal nor a
  // parameter, so ConstExternalPropertyValue cannot resolve it.
  auto *lhs = storage.Create<PrimitiveLiteral>(memgraph::storage::ExternalPropertyValue{static_cast<int64_t>(0)}, 0);
  auto *rhs = storage.Create<PrimitiveLiteral>(memgraph::storage::ExternalPropertyValue{static_cast<int64_t>(1)}, 0);
  auto *add = storage.Create<AdditionOperator>();
  add->expression1_ = lhs;
  add->expression2_ = rhs;
  lower_bound = add;
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::DEPTH_FIRST);
}

TEST_F(PruningBFSRewriteTest, MarksAnExpansionWithNoBoundAtAll) {
  auto const type = RewrittenType([](auto input) { return input; });
  EXPECT_EQ(type, EdgeAtom::Type::PRUNING_BFS);
}

// === One search shared across the input rows ===

TEST_F(PruningBFSRewriteTest, SharesOneSearchWhenTheSourceIsDeadAbove) {
  // Only `target` is read above, so which source reached a vertex cannot be told
  // from the rows, and the searches need not be kept apart.
  EXPECT_TRUE(SharesOneSearch([](auto input) { return input; }));
}

TEST_F(PruningBFSRewriteTest, DoesNotShareOneSearchUnderAnUpperBound) {
  // A vertex first reached deep would keep a later source that reaches it early
  // from walking what that source still has the bound to walk.
  upper_bound = SuppliedBound();
  EXPECT_FALSE(SharesOneSearch([](auto input) { return input; }));
}

TEST_F(PruningBFSRewriteTest, DoesNotShareOneSearchWhenEdgesCrossEitherWay) {
  // Ruling out a walk that retreads the edge it left a source by takes the
  // branch it was found on, and a branch belongs to the source it leaves.
  direction = EdgeAtom::Direction::BOTH;
  EXPECT_FALSE(SharesOneSearch([](auto input) { return input; }));
}

TEST_F(PruningBFSRewriteTest, DoesNotShareOneSearchWhenTheSourceIsReadAbove) {
  // The rows a shared search drops are ones this filter could have told apart.
  EXPECT_FALSE(SharesOneSearch([this](auto input) {
    auto *reads_source = storage.Create<Identifier>("source")->MapTo(source_sym);
    return std::static_pointer_cast<LogicalOperator>(
        std::make_shared<Filter>(input, std::vector<std::shared_ptr<LogicalOperator>>{}, reads_source));
  }));
}

TEST_F(PruningBFSRewriteTest, DoesNotShareOneSearchWhenTheLambdaReadsTheOuterRow) {
  // A vertex let through under one row's filter would be passed over under the
  // next one's, but a shared search settles it for every row at once.
  auto *identifier = storage.Create<Identifier>("source")->MapTo(source_sym);
  filter_lambda = storage.Create<PropertyLookup>(identifier, storage.GetPropertyIx("keep"));
  EXPECT_FALSE(SharesOneSearch([](auto input) { return input; }));
}

TEST_F(PruningBFSRewriteTest, SharesOneSearchWhenTheLambdaReadsOnlyItsOwnRow) {
  auto *identifier = storage.Create<Identifier>("inner_node")->MapTo(inner_node_sym);
  filter_lambda = storage.Create<PropertyLookup>(identifier, storage.GetPropertyIx("keep"));
  EXPECT_TRUE(SharesOneSearch([](auto input) { return input; }));
}

// === The Distinct that licensed the rewrite is spent by it ===

TEST_F(PruningBFSRewriteTest, SpendsTheDistinctOnASharedSearch) {
  // A shared search emits each vertex once over the whole operator, so the
  // Distinct that permitted it has nothing of its own left to catch.
  EXPECT_TRUE(DistinctIsSpentOn([](auto input) { return input; }));
}

TEST_F(PruningBFSRewriteTest, SpendsTheDistinctThroughAFilter) {
  // A filter only ever drops rows, so it cannot put back a duplicate.
  EXPECT_TRUE(DistinctIsSpentOn([this](auto input) {
    auto *reads_target = storage.Create<Identifier>("target")->MapTo(target_sym);
    return std::static_pointer_cast<LogicalOperator>(
        std::make_shared<Filter>(input, std::vector<std::shared_ptr<LogicalOperator>>{}, reads_target));
  }));
}

TEST_F(PruningBFSRewriteTest, KeepsTheDistinctWhereTheSearchIsNotShared) {
  // A per-source pruning BFS repeats a vertex once for every source reaching
  // it, which is the whole of what this Distinct is there for.
  upper_bound = SuppliedBound();
  EXPECT_FALSE(DistinctIsSpentOn([](auto input) { return input; }));
}

TEST_F(PruningBFSRewriteTest, KeepsTheDistinctWhereNoRewriteFired) {
  lower_bound = BoundFromTheRow();
  EXPECT_FALSE(DistinctIsSpentOn([](auto input) { return input; }));
}

}  // namespace
