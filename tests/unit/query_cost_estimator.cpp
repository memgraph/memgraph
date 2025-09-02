// Copyright 2025 Memgraph Ltd.
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
#include "query/plan/rewrite/index_lookup.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::replication_coordination_glue::ReplicationRole;
using CardParam = CostEstimator<memgraph::query::DbAccessor>::CardParam;
using CostParam = CostEstimator<memgraph::query::DbAccessor>::CostParam;
using MiscParam = CostEstimator<memgraph::query::DbAccessor>::MiscParam;
namespace ms = memgraph::storage;

/** A fixture for cost estimation. Sets up the database
 * and accessor (adds some vertices). Provides convenience
 * functions for creating the logical plan. Note that the
 * resulting plan is NOT fit for execution, only for cost
 * estimation testing. */
class QueryCostEstimator : public ::testing::Test {
 protected:
  std::unique_ptr<ms::Storage> db = std::make_unique<ms::InMemoryStorage>();
  std::optional<std::unique_ptr<ms::Storage::Accessor>> storage_dba;
  std::optional<memgraph::query::DbAccessor> dba;
  ms::LabelId label = db->NameToLabel("label");
  ms::PropertyId prop_a = db->NameToProperty("a");
  ms::PropertyId prop_b = db->NameToProperty("b");
  ms::PropertyId prop_c = db->NameToProperty("c");
  ms::PropertyId prop_d = db->NameToProperty("d");

  // we incrementally build the logical operator plan
  // start it off with Once
  std::shared_ptr<LogicalOperator> last_op_ = std::make_shared<Once>();

  AstStorage storage_;
  SymbolTable symbol_table_;
  Parameters parameters_;
  int symbol_count = 0;

  void SetUp() override {
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label, {prop_a}).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label, {prop_c, prop_a, prop_b}).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label, {ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}})
                       .HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    storage_dba.emplace(db->Access());
    dba.emplace(storage_dba->get());
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
        ASSERT_TRUE(vertex.SetProperty(prop_a, ms::PropertyValue(i)).HasValue());
        ASSERT_TRUE(vertex.SetProperty(prop_b, ms::PropertyValue(i)).HasValue());
        ASSERT_TRUE(vertex.SetProperty(prop_c, ms::PropertyValue(i)).HasValue());

        ASSERT_TRUE(vertex
                        .SetProperty(prop_d, ms::PropertyValue{ms::PropertyValue::map_t{
                                                 {prop_a, ms::PropertyValue(i)}, {prop_b, ms::PropertyValue(i)}}})
                        .HasValue());
      }
    }

    dba->AdvanceCommand();
  }

  auto Cost() {
    CostEstimator<memgraph::query::DbAccessor> cost_estimator(&*dba, symbol_table_, parameters_,
                                                              memgraph::query::plan::IndexHints());
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
    parameters_.Add(token_position, ms::ExternalPropertyValue(value));
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

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesConstant) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelProperties>(nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Equal(const_val)});
    EXPECT_COST(1 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesConstExpr) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_a}},
        std::vector{ExpressionRange::Equal(storage_.Create<UnaryPlusOperator>(const_val))});
    // once we make expression const-folding this test case will fail
    EXPECT_COST(20 * CardParam::kFilter * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesUpperConstant) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelProperties>(nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Range(std::nullopt, InclusiveBound(const_val))});
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(13 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesLowerConstant) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(17), Parameter(17)}) {
    MakeOp<ScanAllByLabelProperties>(nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Range(InclusiveBound(const_val), std::nullopt)});
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(3 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertieRangeConstExpr) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = std::make_optional(
        memgraph::utils::MakeBoundInclusive(static_cast<Expression *>(storage_.Create<UnaryPlusOperator>(const_val))));

    MakeOp<ScanAllByLabelProperties>(nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Range(bound, std::nullopt)});

    EXPECT_COST(20 * CardParam::kFilter * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesComposite) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);

    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label,
        std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(bound, bound),
                    ExpressionRange::Range(bound, bound)});

    EXPECT_COST(1 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesComposite_CostIs0IfPropIsNotInRange) {
  AddVertices(100, 30, 20);
  auto b_bound = InclusiveBound(Literal(1000));
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);

    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label,
        std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(b_bound, b_bound),
                    ExpressionRange::Range(bound, bound)});

    EXPECT_COST(CostParam::kMinimumCost);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesComposite_EstimateCostWhenOpCannotUseExactValues) {
  AddVertices(100, 30, 20);

  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);
    auto b_bound = std::make_optional(
        memgraph::utils::MakeBoundInclusive(static_cast<Expression *>(storage_.Create<UnaryPlusOperator>(const_val))));

    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label,
        std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(b_bound, b_bound),
                    ExpressionRange::Range(bound, bound)});

    // This computation is based on having 60 vertices in the index. Technically
    // incorrect, as we only have 20, but each time we set a property `a`, `b`,
    // or `c`, an new index entry is created. We over-estimate the cost by
    // a factor or 3. We could account for this in the costing?
    EXPECT_COST(15 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesCompositeNested) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);

    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(bound, bound)});

    EXPECT_COST(1 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesCompositeNested_CostIs0IfPropIsNotInRange) {
  AddVertices(100, 30, 20);
  auto b_bound = InclusiveBound(Literal(1000));
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);

    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(b_bound, b_bound)});

    EXPECT_COST(CostParam::kMinimumCost);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesCompositeNested_EstimateCostWhenOpCannotUseExactValues) {
  AddVertices(100, 30, 20);

  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);
    auto b_bound = std::make_optional(
        memgraph::utils::MakeBoundInclusive(static_cast<Expression *>(storage_.Create<UnaryPlusOperator>(const_val))));

    MakeOp<ScanAllByLabelProperties>(
        nullptr, NextSymbol(), label, std::vector{ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(b_bound, b_bound)});

    EXPECT_COST(5 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, Expand) {
  MakeOp<Expand>(last_op_, NextSymbol(), NextSymbol(), NextSymbol(), EdgeAtom::Direction::IN,
                 std::vector<ms::EdgeTypeId>{}, false, ms::View::OLD);
  EXPECT_COST(CardParam::kExpand * CostParam::kExpand);
}

TEST_F(QueryCostEstimator, ExpandVariable) {
  MakeOp<ExpandVariable>(last_op_, NextSymbol(), NextSymbol(), NextSymbol(), EdgeAtom::Type::DEPTH_FIRST,
                         EdgeAtom::Direction::IN, std::vector<ms::EdgeTypeId>{}, false, nullptr, nullptr, false,
                         ExpansionLambda{NextSymbol(), NextSymbol(), nullptr}, std::nullopt, std::nullopt, nullptr);
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
  EXPECT_COST(CostParam::kSubquery * no_vertices * no_vertices + no_vertices);
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
// Intentially a macro (instead of function) for better test feedback.
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
