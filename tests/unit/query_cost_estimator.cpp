// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <memory>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/storage.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

using CardParam = CostEstimator<memgraph::query::DbAccessor>::CardParam;
using CostParam = CostEstimator<memgraph::query::DbAccessor>::CostParam;
using MiscParam = CostEstimator<memgraph::query::DbAccessor>::MiscParam;

/** A fixture for cost estimation. Sets up the database
 * and accessor (adds some vertices). Provides convenience
 * functions for creating the logical plan. Note that the
 * resulting plan is NOT fit for execution, only for cost
 * estimation testing. */
class QueryCostEstimator : public ::testing::Test {
 protected:
  memgraph::storage::Storage db;
  std::optional<memgraph::storage::Storage::Accessor> storage_dba;
  std::optional<memgraph::query::DbAccessor> dba;
  memgraph::storage::LabelId label = db.NameToLabel("label");
  memgraph::storage::PropertyId property = db.NameToProperty("property");

  // we incrementally build the logical operator plan
  // start it off with Once
  std::shared_ptr<LogicalOperator> last_op_ = std::make_shared<Once>();

  AstStorage storage_;
  SymbolTable symbol_table_;
  Parameters parameters_;
  int symbol_count = 0;

  void SetUp() {
    ASSERT_FALSE(db.CreateIndex(label).HasError());
    ASSERT_FALSE(db.CreateIndex(label, property).HasError());
    storage_dba.emplace(db.Access());
    dba.emplace(&*storage_dba);
  }

  Symbol NextSymbol() { return symbol_table_.CreateSymbol("Symbol" + std::to_string(symbol_count++), true); }

  /** Adds the given number of vertices to the DB, of which
   * the given numbers are labeled and have a property set. */
  void AddVertices(int vertex_count, int labeled_count, int property_count = 0) {
    for (int i = 0; i < vertex_count; i++) {
      auto vertex = dba->InsertVertex();
      if (i < labeled_count) {
        ASSERT_TRUE(vertex.AddLabel(label).HasValue());
      }
      if (i < property_count) {
        ASSERT_TRUE(vertex.SetProperty(property, memgraph::storage::PropertyValue(i)).HasValue());
      }
    }

    dba->AdvanceCommand();
  }

  auto Cost() {
    CostEstimator<memgraph::query::DbAccessor> cost_estimator(&*dba, parameters_);
    last_op_->Accept(cost_estimator);
    return cost_estimator.cost();
  }

  template <typename TLogicalOperator, typename... TArgs>
  void MakeOp(TArgs... args) {
    last_op_ = std::make_shared<TLogicalOperator>(args...);
  }

  template <typename TValue>
  Expression *Literal(TValue value) {
    return storage_.Create<PrimitiveLiteral>(value);
  }

  template <typename TValue>
  Expression *Parameter(TValue value) {
    int token_position = parameters_.size();
    parameters_.Add(token_position, memgraph::storage::PropertyValue(value));
    return storage_.Create<ParameterLookup>(token_position);
  }

  auto InclusiveBound(Expression *expression) {
    return std::make_optional(memgraph::utils::MakeBoundInclusive(expression));
  };

  const std::nullopt_t nullopt = std::nullopt;
};

// multiply with 1 to avoid linker error (possibly fixed in CLang >= 3.81)
#define EXPECT_COST(COST) EXPECT_FLOAT_EQ(Cost(), 1 * COST)

TEST_F(QueryCostEstimator, Once) { EXPECT_COST(0); }

TEST_F(QueryCostEstimator, ScanAll) {
  AddVertices(100, 30, 20);
  MakeOp<ScanAll>(last_op_, NextSymbol());
  EXPECT_COST(100 * CostParam::kScanAll);
}

TEST_F(QueryCostEstimator, ScanAllByLabelCardinality) {
  AddVertices(100, 30, 20);
  MakeOp<ScanAllByLabel>(last_op_, NextSymbol(), label);
  EXPECT_COST(30 * CostParam::kScanAllByLabel);
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertyValueConstant) {
  AddVertices(100, 30, 20);
  for (auto const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelPropertyValue>(nullptr, NextSymbol(), label, property, "property", const_val);
    EXPECT_COST(1 * CostParam::MakeScanAllByLabelPropertyValue);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertyValueConstExpr) {
  AddVertices(100, 30, 20);
  for (auto const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelPropertyValue>(nullptr, NextSymbol(), label, property, "property",
                                        // once we make expression const-folding this test case will fail
                                        storage_.Create<UnaryPlusOperator>(const_val));
    EXPECT_COST(20 * CardParam::kFilter * CostParam::MakeScanAllByLabelPropertyValue);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertyRangeUpperConstant) {
  AddVertices(100, 30, 20);
  for (auto const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelPropertyRange>(nullptr, NextSymbol(), label, property, "property", nullopt,
                                        InclusiveBound(const_val));
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(13 * CostParam::MakeScanAllByLabelPropertyRange);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertyRangeLowerConstant) {
  AddVertices(100, 30, 20);
  for (auto const_val : {Literal(17), Parameter(17)}) {
    MakeOp<ScanAllByLabelPropertyRange>(nullptr, NextSymbol(), label, property, "property", InclusiveBound(const_val),
                                        nullopt);
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(3 * CostParam::MakeScanAllByLabelPropertyRange);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertyRangeConstExpr) {
  AddVertices(100, 30, 20);
  for (auto const_val : {Literal(12), Parameter(12)}) {
    auto bound = std::make_optional(
        memgraph::utils::MakeBoundInclusive(static_cast<Expression *>(storage_.Create<UnaryPlusOperator>(const_val))));
    MakeOp<ScanAllByLabelPropertyRange>(nullptr, NextSymbol(), label, property, "property", bound, nullopt);
    EXPECT_COST(20 * CardParam::kFilter * CostParam::MakeScanAllByLabelPropertyRange);
  }
}

TEST_F(QueryCostEstimator, Expand) {
  MakeOp<Expand>(last_op_, NextSymbol(), NextSymbol(), NextSymbol(), EdgeAtom::Direction::IN,
                 std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  EXPECT_COST(CardParam::kExpand * CostParam::kExpand);
}

TEST_F(QueryCostEstimator, ExpandVariable) {
  MakeOp<ExpandVariable>(last_op_, NextSymbol(), NextSymbol(), NextSymbol(), EdgeAtom::Type::DEPTH_FIRST,
                         EdgeAtom::Direction::IN, std::vector<memgraph::storage::EdgeTypeId>{}, false, nullptr, nullptr,
                         false, ExpansionLambda{NextSymbol(), NextSymbol(), nullptr}, std::nullopt, std::nullopt);
  EXPECT_COST(CardParam::kExpandVariable * CostParam::kExpandVariable);
}

TEST_F(QueryCostEstimator, ForeachListLiteral) {
  constexpr size_t list_expr_sz = 10;
  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(std::make_shared<Once>(), NodeCreationInfo{});
  MakeOp<memgraph::query::plan::Foreach>(
      last_op_, create, storage_.Create<ListLiteral>(std::vector<Expression *>(list_expr_sz, nullptr)), NextSymbol());
  EXPECT_COST(CostParam::kForeach * list_expr_sz);
}

TEST_F(QueryCostEstimator, Foreach) {
  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(std::make_shared<Once>(), NodeCreationInfo{});
  MakeOp<memgraph::query::plan::Foreach>(last_op_, create, storage_.Create<Identifier>(), NextSymbol());
  EXPECT_COST(CostParam::kForeach * MiscParam::kForeachNoLiteral);
}

TEST_F(QueryCostEstimator, SubqueryCartesian) {
  auto no_vertices = 4;
  AddVertices(no_vertices, 0, 0);
  std::shared_ptr<LogicalOperator> input = std::make_shared<ScanAll>(std::make_shared<Once>(), NextSymbol());
  std::shared_ptr<LogicalOperator> subquery = std::make_shared<ScanAll>(std::make_shared<Once>(), NextSymbol());
  MakeOp<memgraph::query::plan::Apply>(input, subquery, true);
  EXPECT_COST(CostParam::kSubquery * no_vertices * no_vertices);
}

TEST_F(QueryCostEstimator, UnitSubquery) {
  auto no_vertices = 4;
  AddVertices(no_vertices, 0, 0);
  std::shared_ptr<LogicalOperator> input = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> subquery = std::make_shared<ScanAll>(std::make_shared<Once>(), NextSymbol());
  MakeOp<memgraph::query::plan::Apply>(input, subquery, true);
  EXPECT_COST(CostParam::kSubquery * no_vertices);
}

TEST_F(QueryCostEstimator, Union) {
  auto no_vertices = 4;
  AddVertices(no_vertices, 0, 0);

  std::vector<Symbol> union_symbols{NextSymbol()};
  std::shared_ptr<LogicalOperator> left_op = std::make_shared<ScanAll>(std::make_shared<Once>(), NextSymbol());
  std::shared_ptr<LogicalOperator> right_op = std::make_shared<ScanAll>(std::make_shared<Once>(), NextSymbol());
  MakeOp<memgraph::query::plan::Union>(left_op, right_op, union_symbols, left_op->OutputSymbols(symbol_table_),
                                       right_op->OutputSymbols(symbol_table_));
  EXPECT_COST(CostParam::kUnion * (no_vertices + no_vertices));
}

// Helper for testing an operations cost and cardinality.
// Only for operations that first increment cost, then modify cardinality.
// Intentionally a macro (instead of function) for better test feedback.
#define TEST_OP(OP, OP_COST_PARAM, OP_CARD_PARAM) \
  OP;                                             \
  EXPECT_COST(OP_COST_PARAM);                     \
  OP;                                             \
  EXPECT_COST(OP_COST_PARAM + OP_CARD_PARAM * OP_COST_PARAM);

TEST_F(QueryCostEstimator, Filter) {
  TEST_OP(MakeOp<Filter>(last_op_, std::vector<std::shared_ptr<LogicalOperator>>{}, Literal(true)), CostParam::kFilter,
          CardParam::kFilter);
}

TEST_F(QueryCostEstimator, EdgeUniquenessFilter) {
  TEST_OP(MakeOp<EdgeUniquenessFilter>(last_op_, NextSymbol(), std::vector<Symbol>()), CostParam::kEdgeUniquenessFilter,
          CardParam::kEdgeUniquenessFilter);
}

TEST_F(QueryCostEstimator, UnwindLiteral) {
  TEST_OP(MakeOp<memgraph::query::plan::Unwind>(
              last_op_, storage_.Create<ListLiteral>(std::vector<Expression *>(7, nullptr)), NextSymbol()),
          CostParam::kUnwind, 7);
}

TEST_F(QueryCostEstimator, UnwindNoLiteral) {
  TEST_OP(MakeOp<memgraph::query::plan::Unwind>(last_op_, nullptr, NextSymbol()), CostParam::kUnwind,
          MiscParam::kUnwindNoLiteral);
}

#undef TEST_OP
#undef EXPECT_COST
//
// TODO test cost when ScanAll, Expand, Accumulate, Limit
// vs cost for SA, Expand, Limit
