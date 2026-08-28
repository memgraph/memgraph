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
      ASSERT_TRUE(unique_acc->CreateIndex(label).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(label, {prop_a}).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(label, {prop_c, prop_a, prop_b}).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(label, {ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}})
                      .has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    storage_dba.emplace(db->Access(memgraph::storage::WRITE));
    dba.emplace(storage_dba->get());
  }

  Symbol NextSymbol() { return symbol_table_.CreateSymbol("Symbol" + std::to_string(symbol_count++), true); }

  /** Adds the given number of vertices to the DB, of which
   * the given numbers are labeled and have a property set. */
  void AddVertices(int vertex_count, int labeled_count, int property_count = 0) {
    for (int i = 0; i < vertex_count; i++) {
      auto vertex = dba->InsertVertex();
      if (i < labeled_count) {
        ASSERT_TRUE(vertex.AddLabel(label).has_value());
      }
      if (i < property_count) {
        ASSERT_TRUE(vertex.SetProperty(prop_a, ms::PropertyValue(i)).has_value());
        ASSERT_TRUE(vertex.SetProperty(prop_b, ms::PropertyValue(i)).has_value());
        ASSERT_TRUE(vertex.SetProperty(prop_c, ms::PropertyValue(i)).has_value());

        ASSERT_TRUE(vertex
                        .SetProperty(prop_d,
                                     ms::PropertyValue{ms::PropertyValue::map_t{{prop_a, ms::PropertyValue(i)},
                                                                                {prop_b, ms::PropertyValue(i)}}})
                        .has_value());
      }
    }

    dba->AdvanceCommand();
  }

  auto Cost() {
    CostEstimator<memgraph::query::DbAccessor> cost_estimator(
        &*dba, symbol_table_, parameters_, memgraph::query::plan::IndexHints());
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
    MakeOp<ScanAllByLabelProperties>(nullptr,
                                     NextSymbol(),
                                     label,
                                     std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Equal(const_val)});
    EXPECT_COST(1 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesConstExpr) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelProperties>(
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_a}},
        std::vector{ExpressionRange::Equal(storage_.Create<UnaryPlusOperator>(const_val))});
    // once we make expression const-folding this test case will fail
    EXPECT_COST(20 * CardParam::kFilter * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesUpperConstant) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    MakeOp<ScanAllByLabelProperties>(nullptr,
                                     NextSymbol(),
                                     label,
                                     std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Range(std::nullopt, InclusiveBound(const_val))});
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(13 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesLowerConstant) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(17), Parameter(17)}) {
    MakeOp<ScanAllByLabelProperties>(nullptr,
                                     NextSymbol(),
                                     label,
                                     std::vector{ms::PropertyPath{prop_a}},
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

    MakeOp<ScanAllByLabelProperties>(nullptr,
                                     NextSymbol(),
                                     label,
                                     std::vector{ms::PropertyPath{prop_a}},
                                     std::vector{ExpressionRange::Range(bound, std::nullopt)});

    EXPECT_COST(20 * CardParam::kFilter * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesComposite) {
  AddVertices(100, 30, 20);
  for (auto *const_val : {Literal(12), Parameter(12)}) {
    auto bound = InclusiveBound(const_val);

    MakeOp<ScanAllByLabelProperties>(
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
        std::vector{ExpressionRange::Range(bound, bound),
                    ExpressionRange::Range(bound, bound),
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
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
        std::vector{ExpressionRange::Range(bound, bound),
                    ExpressionRange::Range(b_bound, b_bound),
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
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
        std::vector{ExpressionRange::Range(bound, bound),
                    ExpressionRange::Range(b_bound, b_bound),
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
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}},
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
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}},
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
        nullptr,
        NextSymbol(),
        label,
        std::vector{ms::PropertyPath{prop_d, prop_a}, ms::PropertyPath{prop_d, prop_b}},
        std::vector{ExpressionRange::Range(bound, bound), ExpressionRange::Range(b_bound, b_bound)});

    EXPECT_COST(5 * CostParam::kScanAllByLabelProperties);
  }
}

TEST_F(QueryCostEstimator, Expand) {
  MakeOp<Expand>(last_op_,
                 NextSymbol(),
                 NextSymbol(),
                 NextSymbol(),
                 EdgeAtom::Direction::IN,
                 std::vector<ms::EdgeTypeId>{},
                 false,
                 ms::View::OLD);
  EXPECT_COST(CardParam::kExpand * CostParam::kExpand);
}

TEST_F(QueryCostEstimator, ExpandVariable) {
  MakeOp<ExpandVariable>(last_op_,
                         NextSymbol(),
                         NextSymbol(),
                         NextSymbol(),
                         EdgeAtom::Type::DEPTH_FIRST,
                         EdgeAtom::Direction::IN,
                         std::vector<ms::EdgeTypeId>{},
                         false,
                         nullptr,
                         nullptr,
                         false,
                         ExpansionLambda{NextSymbol(), NextSymbol(), nullptr},
                         std::nullopt,
                         std::nullopt,
                         nullptr);
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
  MakeOp<memgraph::query::plan::Union>(
      left_op, right_op, union_symbols, left_op->OutputSymbols(symbol_table_), right_op->OutputSymbols(symbol_table_));
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
  TEST_OP(MakeOp<Filter>(last_op_, std::vector<std::shared_ptr<LogicalOperator>>{}, Literal(true)),
          CostParam::kFilter,
          CardParam::kFilter);
}

TEST_F(QueryCostEstimator, EdgeUniquenessFilter) {
  TEST_OP(MakeOp<EdgeUniquenessFilter>(last_op_, NextSymbol(), std::vector<Symbol>()),
          CostParam::kEdgeUniquenessFilter,
          CardParam::kEdgeUniquenessFilter);
}

TEST_F(QueryCostEstimator, UnwindLiteral) {
  TEST_OP(MakeOp<memgraph::query::plan::Unwind>(
              last_op_, storage_.Create<ListLiteral>(std::vector<Expression *>(7, nullptr)), NextSymbol()),
          CostParam::kUnwind,
          7);
}

TEST_F(QueryCostEstimator, UnwindNoLiteral) {
  TEST_OP(MakeOp<memgraph::query::plan::Unwind>(last_op_, nullptr, NextSymbol()),
          CostParam::kUnwind,
          MiscParam::kUnwindNoLiteral);
}

namespace {
// Helper to build the toSet(coalesce(list, [])) AST pattern produced by IN-to-Unwind lowering.
Expression *MakeInUnwindExpression(AstStorage &storage, std::vector<Expression *> elements) {
  auto *inner_list = storage.Create<ListLiteral>(std::move(elements));
  auto *empty_list = storage.Create<ListLiteral>(std::vector<Expression *>{});
  auto *coalesced = storage.Create<Coalesce>(std::vector<Expression *>{inner_list, empty_list});
  auto *toset = storage.Create<Function>();
  toset->function_name_ = "TOSET";
  toset->arguments_ = {coalesced};
  return toset;
}
}  // namespace

TEST_F(QueryCostEstimator, UnwindInLowering) {
  auto *expr = MakeInUnwindExpression(storage_, {Literal(1), Literal(2), Literal(3)});
  TEST_OP(MakeOp<memgraph::query::plan::Unwind>(last_op_, expr, NextSymbol()), CostParam::kUnwind, 3);
}

// -- IN-list cardinality estimation tests --

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesInList) {
  AddVertices(100, 30, 20);
  // IN [12]: 1 element, matches 1 vertex. Unwind factor = 1, scan sum = 1.
  // Current estimate: 1 * 1 = 1 (no double-count for single element).
  auto *list = storage_.Create<ListLiteral>(std::vector<Expression *>{Literal(12)});
  auto *unwind_expr = MakeInUnwindExpression(storage_, {Literal(12)});
  MakeOp<memgraph::query::plan::Unwind>(last_op_, unwind_expr, NextSymbol());
  auto *unwind_sym = storage_.Create<Identifier>("anon_sym");
  MakeOp<ScanAllByLabelProperties>(last_op_,
                                   NextSymbol(),
                                   label,
                                   std::vector{ms::PropertyPath{prop_a}},
                                   std::vector{ExpressionRange::In(unwind_sym, list)});
  // cost = CostParam::kUnwind + (1 * 1) * CostParam::kScanAllByLabelProperties
  EXPECT_COST(CostParam::kUnwind + 1 * CostParam::kScanAllByLabelProperties);
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesInListMultipleElements) {
  AddVertices(100, 30, 20);
  // IN [5, 10]: 2 elements, each matches 1 vertex. Unwind factor = 2, scan per-row = 1.
  auto *list = storage_.Create<ListLiteral>(std::vector<Expression *>{Literal(5), Literal(10)});
  auto *unwind_expr = MakeInUnwindExpression(storage_, {Literal(5), Literal(10)});
  MakeOp<memgraph::query::plan::Unwind>(last_op_, unwind_expr, NextSymbol());
  auto *unwind_sym = storage_.Create<Identifier>("anon_sym");
  MakeOp<ScanAllByLabelProperties>(last_op_,
                                   NextSymbol(),
                                   label,
                                   std::vector{ms::PropertyPath{prop_a}},
                                   std::vector{ExpressionRange::In(unwind_sym, list)});
  // Scan returns per-row factor: S / n = 2 / 2 = 1. Cardinality = 2 * 1 = 2.
  EXPECT_COST(CostParam::kUnwind + 2 * CostParam::kScanAllByLabelProperties);
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesInListNonexistentValue) {
  AddVertices(100, 30, 20);
  // IN [999]: 1 element, matches 0 vertices. Unwind factor = 1, scan sum = 0.
  auto *list = storage_.Create<ListLiteral>(std::vector<Expression *>{Literal(999)});
  auto *unwind_expr = MakeInUnwindExpression(storage_, {Literal(999)});
  MakeOp<memgraph::query::plan::Unwind>(last_op_, unwind_expr, NextSymbol());
  auto *unwind_sym = storage_.Create<Identifier>("anon_sym");
  MakeOp<ScanAllByLabelProperties>(last_op_,
                                   NextSymbol(),
                                   label,
                                   std::vector{ms::PropertyPath{prop_a}},
                                   std::vector{ExpressionRange::In(unwind_sym, list)});
  // cardinality = 1 * 0 = 0, cost = min_cost + CostParam::kUnwind
  EXPECT_COST(CostParam::kUnwind + CostParam::kMinimumCost);
}

TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesMultipleInLists) {
  AddVertices(100, 30, 20);
  // Composite index on (prop_c, prop_a, prop_b). All three properties are set to the same
  // value i for each vertex, so only diagonal entries (i,i,i) exist.
  // prop_c = 5 (resolved), prop_a IN [5, 10] (unresolved), prop_b IN [5, 10] (unresolved).
  // True matches: only (5,5,5). Independence estimate: S_a * S_b / T = 1 * 1 / 1 = 1.
  // Unwind factors: 2 * 2 = 4. Scan per-row = 1 / 4 = 0.25. Final cardinality = 1.
  auto *list_a = storage_.Create<ListLiteral>(std::vector<Expression *>{Literal(5), Literal(10)});
  auto *list_b = storage_.Create<ListLiteral>(std::vector<Expression *>{Literal(5), Literal(10)});
  // Chain two Unwinds (the real plan has two Unwinds for two IN clauses)
  auto *unwind_expr_a = MakeInUnwindExpression(storage_, {Literal(5), Literal(10)});
  MakeOp<memgraph::query::plan::Unwind>(last_op_, unwind_expr_a, NextSymbol());
  auto *unwind_expr_b = MakeInUnwindExpression(storage_, {Literal(5), Literal(10)});
  MakeOp<memgraph::query::plan::Unwind>(last_op_, unwind_expr_b, NextSymbol());
  auto *sym_a = storage_.Create<Identifier>("anon_a");
  auto *sym_b = storage_.Create<Identifier>("anon_b");
  MakeOp<ScanAllByLabelProperties>(
      last_op_,
      NextSymbol(),
      label,
      std::vector{ms::PropertyPath{prop_c}, ms::PropertyPath{prop_a}, ms::PropertyPath{prop_b}},
      std::vector{
          ExpressionRange::Equal(Literal(5)), ExpressionRange::In(sym_a, list_a), ExpressionRange::In(sym_b, list_b)});
  // Unwind 1: cost += 1 * kUnwind, cardinality = 2
  // Unwind 2: cost += 2 * kUnwind, cardinality = 4
  // Scan: independence result = 1, unwind_factor = 4, per-row = 0.25. cardinality = 4 * 0.25 = 1.
  EXPECT_COST(3 * CostParam::kUnwind + 1 * CostParam::kScanAllByLabelProperties);
}

#undef TEST_OP
#undef EXPECT_COST

TEST_F(QueryCostEstimator, OrderByHasNonZeroCost) {
  // A plan with OrderBy should have higher cost than the same plan without it.
  // This matters because index-scan rewriting can eliminate an OrderBy when the
  // scan already provides the required order; if OrderBy is free the cost
  // estimator cannot prefer the plan without the sort.
  AddVertices(100, 30, 20);  // ScanAll cardinality ~ 100
  MakeOp<ScanAll>(last_op_, NextSymbol());
  const auto cost_without_order_by = Cost();

  // add an OrderBy on top of the same ScanAll
  const auto sym = NextSymbol();
  MakeOp<OrderBy>(last_op_, std::vector<SortItem>{{Ordering::ASC, Literal(1)}}, std::vector<Symbol>{sym});
  const auto cost_with_order_by = Cost();

  EXPECT_GT(cost_with_order_by, cost_without_order_by);
}

// DESC index should produce the same cost as ASC index for the same data.
TEST_F(QueryCostEstimator, ScanAllByLabelPropertiesDescSameCostAsAsc) {
  AddVertices(100, 30, 20);

  // ASC cost (index already created in SetUp)
  MakeOp<ScanAllByLabelProperties>(nullptr,
                                   NextSymbol(),
                                   label,
                                   std::vector{ms::PropertyPath{prop_a}},
                                   std::vector{ExpressionRange::Equal(Literal(12))});
  static_cast<ScanAllByLabelProperties *>(last_op_.get())->index_order_ = ms::IndexOrder::ASC;
  auto asc_cost = Cost();

  // DESC cost — uses the same VerticesCount path (order-independent), so cost must match
  MakeOp<ScanAllByLabelProperties>(nullptr,
                                   NextSymbol(),
                                   label,
                                   std::vector{ms::PropertyPath{prop_a}},
                                   std::vector{ExpressionRange::Equal(Literal(12))});
  static_cast<ScanAllByLabelProperties *>(last_op_.get())->index_order_ = ms::IndexOrder::DESC;
  auto desc_cost = Cost();

  EXPECT_FLOAT_EQ(asc_cost, desc_cost);
}

TEST_F(QueryCostEstimator, ExtractListFromInUnwindNonMatching) {
  EXPECT_EQ(ExtractListFromInUnwind(nullptr), nullptr);
  EXPECT_EQ(ExtractListFromInUnwind(Literal(42)), nullptr);
  auto *plain_list = storage_.Create<ListLiteral>(std::vector<Expression *>{Literal(1)});
  EXPECT_EQ(ExtractListFromInUnwind(plain_list), nullptr);
}

TEST_F(QueryCostEstimator, ExtractListFromInUnwindMatching) {
  auto *expr = MakeInUnwindExpression(storage_, {Literal(1), Literal(2)});
  auto *extracted = ExtractListFromInUnwind(expr);
  ASSERT_NE(extracted, nullptr);
  EXPECT_EQ(extracted->elements_.size(), 2);
}

/** A fixture for the string predicates, whose scan is narrowed to the string type rather than to a
 * span around the search term. Holds a global property index over values spread across the
 * alphabet, so an estimate that mistook the search term for a bound would move as the term moves. */
class QueryCostEstimatorStringPredicates : public ::testing::Test {
 protected:
  std::unique_ptr<ms::Storage> db = std::make_unique<ms::InMemoryStorage>();
  std::optional<std::unique_ptr<ms::Storage::Accessor>> storage_dba;
  std::optional<memgraph::query::DbAccessor> dba;
  ms::PropertyId prop = db->NameToProperty("s");
  ms::PropertyId mixed = db->NameToProperty("mixed");

  std::shared_ptr<LogicalOperator> last_op_ = std::make_shared<Once>();
  AstStorage storage_;
  SymbolTable symbol_table_;
  Parameters parameters_;
  int symbol_count = 0;

  // The index count a range estimate returns is exact only while the index stays under the size
  // at which SkipListLayerForCountEstimation starts sampling upper skip-list layers. Above it the
  // estimate is a random sample whose spread swamps the ratios these tests compare.
  static constexpr int kStringCount = 400;

  void SetUp() override {
    {
      auto unique_acc = db->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateGlobalVertexIndex(prop).has_value());
      ASSERT_TRUE(unique_acc->CreateGlobalVertexIndex(mixed).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    storage_dba.emplace(db->Access(memgraph::storage::WRITE));
    dba.emplace(storage_dba->get());
    // Spread across the alphabet, so a term mistaken for a bound would cut the count differently
    // depending on the letter it starts with.
    for (int i = 0; i < kStringCount; ++i) {
      auto vertex = dba->InsertVertex();
      auto value = std::string(1, static_cast<char>('a' + i % 26)) + std::to_string(i);
      ASSERT_TRUE(vertex.SetProperty(prop, ms::PropertyValue(std::move(value))).has_value());
    }
    // A property whose values are mostly numbers: a string predicate reads only the tail of it.
    for (int i = 0; i < kStringCount; ++i) {
      auto vertex = dba->InsertVertex();
      auto value = i % 10 == 0 ? ms::PropertyValue(std::to_string(i)) : ms::PropertyValue(i);
      ASSERT_TRUE(vertex.SetProperty(mixed, std::move(value)).has_value());
    }
    dba->AdvanceCommand();
  }

  Symbol NextSymbol() { return symbol_table_.CreateSymbol("Symbol" + std::to_string(symbol_count++), true); }

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

  double CostOf(ExpressionRange range) { return CostOfProperty(prop, range); }

  double CostOfProperty(ms::PropertyId property, ExpressionRange range) {
    last_op_ = std::make_shared<ScanAllByVertexProperty>(nullptr, NextSymbol(), property, range);
    CostEstimator<memgraph::query::DbAccessor> cost_estimator(
        &*dba, symbol_table_, parameters_, memgraph::query::plan::IndexHints());
    last_op_->Accept(cost_estimator);
    return cost_estimator.cost();
  }
};

TEST_F(QueryCostEstimatorStringPredicates, EstimateDoesNotMoveWithTheSearchTerm) {
  // Query stripping turns the term into a parameter and the plan cache is keyed on the stripped
  // text, so a plan costed against one call's term would be handed to every other term. The
  // estimate therefore has to be the same for all of them, non-strings included.
  auto const reference = CostOf(ExpressionRange::Contains(Literal("a")));
  for (auto *term : {Literal("a"), Literal("m"), Literal("z"), Literal("zzzz"), Parameter("z"), Literal(5)}) {
    EXPECT_FLOAT_EQ(CostOf(ExpressionRange::Contains(term)), reference);
    EXPECT_FLOAT_EQ(CostOf(ExpressionRange::EndsWith(term)), reference);
    EXPECT_FLOAT_EQ(CostOf(ExpressionRange::RegexMatch(term)), reference);
  }
}

TEST_F(QueryCostEstimatorStringPredicates, EstimateCoversTheStringsTheScanReads) {
  // Every value here is a string, so the band these predicates read is the whole property. An
  // estimate far below it would make the scan look free and win comparisons it should lose.
  auto const whole_property = CostOf(ExpressionRange::IsNotNull());
  ASSERT_GT(whole_property, 0.0);
  EXPECT_GT(CostOf(ExpressionRange::Contains(Literal("a"))), whole_property / 2);
}

TEST_F(QueryCostEstimatorStringPredicates, EstimateNarrowsToTheStringsWhenMostValuesAreNot) {
  // A string predicate reads the property's string values, not the numbers sorted below them, and
  // the band those occupy is the same whatever the search term is.
  auto const whole_property = CostOfProperty(mixed, ExpressionRange::IsNotNull());
  auto const string_band = CostOfProperty(mixed, ExpressionRange::Contains(Literal("a")));
  EXPECT_LT(string_band, whole_property / 2);
  EXPECT_GT(string_band, CostParam::kMinimumCost);
}

TEST_F(QueryCostEstimatorStringPredicates, StartsWithStillNarrowsToItsPrefix) {
  // The prefix is a real bound on what matches, and STARTS_WITH already accepts that its plan is
  // settled by the term. This pins the line between the two: only the prefix seek reads it.
  EXPECT_LT(CostOf(ExpressionRange::StartsWith(Literal("a"))), CostOf(ExpressionRange::IsNotNull()));
}

// TODO test cost when ScanAll, Expand, Accumulate, Limit
// vs cost for SA, Expand, Limit
