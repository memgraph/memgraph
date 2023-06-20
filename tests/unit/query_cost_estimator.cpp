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

#include "disk_test_utils.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
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
template <typename StorageType>
class QueryCostEstimator : public ::testing::Test {
 protected:
  const std::string testSuite = "query_cost_estimator";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<StorageType>(config);
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba;
  std::optional<memgraph::query::DbAccessor> dba;
  memgraph::storage::LabelId label = db->NameToLabel("label");
  memgraph::storage::PropertyId property = db->NameToProperty("property");

  // we incrementally build the logical operator plan
  // start it off with Once
  std::shared_ptr<LogicalOperator> last_op_ = std::make_shared<Once>();

  AstStorage storage_;
  SymbolTable symbol_table_;
  Parameters parameters_;
  int symbol_count = 0;

  void SetUp() override {
    ASSERT_FALSE(db->CreateIndex(label).HasError());
    ASSERT_FALSE(db->CreateIndex(label, property).HasError());
    storage_dba = db->Access();
    dba.emplace(storage_dba.get());
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    db.reset(nullptr);
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
    this->last_op_->Accept(cost_estimator);
    return cost_estimator.cost();
  }

  template <typename TLogicalOperator, typename... TArgs>
  void MakeOp(TArgs... args) {
    this->last_op_ = std::make_shared<TLogicalOperator>(args...);
  }

  template <typename TValue>
  Expression *Literal(TValue value) {
    return storage_.template Create<PrimitiveLiteral>(value);
  }

  template <typename TValue>
  Expression *Parameter(TValue value) {
    int token_position = parameters_.size();
    parameters_.Add(token_position, memgraph::storage::PropertyValue(value));
    return storage_.template Create<ParameterLookup>(token_position);
  }

  auto InclusiveBound(Expression *expression) {
    return std::make_optional(memgraph::utils::MakeBoundInclusive(expression));
  };

  const std::nullopt_t nullopt = std::nullopt;
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(QueryCostEstimator, StorageTypes);

// multiply with 1 to avoid linker error (possibly fixed in CLang >= 3.81)
#define EXPECT_COST(COST) EXPECT_FLOAT_EQ(this->Cost(), 1 * COST)

TYPED_TEST(QueryCostEstimator, Once) { EXPECT_COST(0); }

TYPED_TEST(QueryCostEstimator, ScanAll) {
  this->AddVertices(100, 30, 20);
  this->template MakeOp<ScanAll>(this->last_op_, this->NextSymbol());
  EXPECT_COST(100 * CostParam::kScanAll);
}

TYPED_TEST(QueryCostEstimator, ScanAllByLabelCardinality) {
  this->AddVertices(100, 30, 20);
  this->template MakeOp<ScanAllByLabel>(this->last_op_, this->NextSymbol(), this->label);
  EXPECT_COST(30 * CostParam::kScanAllByLabel);
}

TYPED_TEST(QueryCostEstimator, ScanAllByLabelPropertyValueConstant) {
  this->AddVertices(100, 30, 20);
  for (auto const_val : {this->Literal(12), this->Parameter(12)}) {
    this->template MakeOp<ScanAllByLabelPropertyValue>(nullptr, this->NextSymbol(), this->label, this->property,
                                                       "property", const_val);
    EXPECT_COST(1 * CostParam::MakeScanAllByLabelPropertyValue);
  }
}

TYPED_TEST(QueryCostEstimator, ScanAllByLabelPropertyValueConstExpr) {
  this->AddVertices(100, 30, 20);
  for (auto const_val : {this->Literal(12), this->Parameter(12)}) {
    this->template MakeOp<ScanAllByLabelPropertyValue>(nullptr, this->NextSymbol(), this->label, this->property,
                                                       "property",
                                                       // once we make expression const-folding this test case will fail
                                                       this->storage_.template Create<UnaryPlusOperator>(const_val));
    EXPECT_COST(20 * CardParam::kFilter * CostParam::MakeScanAllByLabelPropertyValue);
  }
}

TYPED_TEST(QueryCostEstimator, ScanAllByLabelPropertyRangeUpperConstant) {
  this->AddVertices(100, 30, 20);
  for (auto const_val : {this->Literal(12), this->Parameter(12)}) {
    this->template MakeOp<ScanAllByLabelPropertyRange>(nullptr, this->NextSymbol(), this->label, this->property,
                                                       "property", std::nullopt, this->InclusiveBound(const_val));
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(13 * CostParam::MakeScanAllByLabelPropertyRange);
  }
}

TYPED_TEST(QueryCostEstimator, ScanAllByLabelPropertyRangeLowerConstant) {
  this->AddVertices(100, 30, 20);
  for (auto const_val : {this->Literal(17), this->Parameter(17)}) {
    this->template MakeOp<ScanAllByLabelPropertyRange>(nullptr, this->NextSymbol(), this->label, this->property,
                                                       "property", this->InclusiveBound(const_val), std::nullopt);
    // cardinality estimation is exact for very small indexes
    EXPECT_COST(3 * CostParam::MakeScanAllByLabelPropertyRange);
  }
}

TYPED_TEST(QueryCostEstimator, ScanAllByLabelPropertyRangeConstExpr) {
  this->AddVertices(100, 30, 20);
  for (auto const_val : {this->Literal(12), this->Parameter(12)}) {
    auto bound = std::make_optional(memgraph::utils::MakeBoundInclusive(
        static_cast<Expression *>(this->storage_.template Create<UnaryPlusOperator>(const_val))));
    this->template MakeOp<ScanAllByLabelPropertyRange>(nullptr, this->NextSymbol(), this->label, this->property,
                                                       "property", bound, std::nullopt);
    EXPECT_COST(20 * CardParam::kFilter * CostParam::MakeScanAllByLabelPropertyRange);
  }
}

TYPED_TEST(QueryCostEstimator, Expand) {
  this->template MakeOp<Expand>(this->last_op_, this->NextSymbol(), this->NextSymbol(), this->NextSymbol(),
                                EdgeAtom::Direction::IN, std::vector<memgraph::storage::EdgeTypeId>{}, false,
                                memgraph::storage::View::OLD);
  EXPECT_COST(CardParam::kExpand * CostParam::kExpand);
}

TYPED_TEST(QueryCostEstimator, ExpandVariable) {
  this->template MakeOp<ExpandVariable>(
      this->last_op_, this->NextSymbol(), this->NextSymbol(), this->NextSymbol(), EdgeAtom::Type::DEPTH_FIRST,
      EdgeAtom::Direction::IN, std::vector<memgraph::storage::EdgeTypeId>{}, false, nullptr, nullptr, false,
      ExpansionLambda{this->NextSymbol(), this->NextSymbol(), nullptr}, std::nullopt, std::nullopt);
  EXPECT_COST(CardParam::kExpandVariable * CostParam::kExpandVariable);
}

TYPED_TEST(QueryCostEstimator, ForeachListLiteral) {
  constexpr size_t list_expr_sz = 10;
  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(std::make_shared<Once>(), NodeCreationInfo{});
  this->template MakeOp<memgraph::query::plan::Foreach>(
      this->last_op_, create,
      this->storage_.template Create<ListLiteral>(std::vector<Expression *>(list_expr_sz, nullptr)),
      this->NextSymbol());
  EXPECT_COST(CostParam::kForeach * list_expr_sz);
}

TYPED_TEST(QueryCostEstimator, Foreach) {
  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(std::make_shared<Once>(), NodeCreationInfo{});
  this->template MakeOp<memgraph::query::plan::Foreach>(
      this->last_op_, create, this->storage_.template Create<Identifier>(), this->NextSymbol());
  EXPECT_COST(CostParam::kForeach * MiscParam::kForeachNoLiteral);
}

TYPED_TEST(QueryCostEstimator, SubqueryCartesian) {
  auto no_vertices = 4;
  this->AddVertices(no_vertices, 0, 0);
  std::shared_ptr<LogicalOperator> input = std::make_shared<ScanAll>(std::make_shared<Once>(), this->NextSymbol());
  std::shared_ptr<LogicalOperator> subquery = std::make_shared<ScanAll>(std::make_shared<Once>(), this->NextSymbol());
  this->template MakeOp<memgraph::query::plan::Apply>(input, subquery, true);
  EXPECT_COST(CostParam::kSubquery * no_vertices * no_vertices);
}

TYPED_TEST(QueryCostEstimator, UnitSubquery) {
  auto no_vertices = 4;
  this->AddVertices(no_vertices, 0, 0);
  std::shared_ptr<LogicalOperator> input = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> subquery = std::make_shared<ScanAll>(std::make_shared<Once>(), this->NextSymbol());
  this->template MakeOp<memgraph::query::plan::Apply>(input, subquery, true);
  EXPECT_COST(CostParam::kSubquery * no_vertices);
}

TYPED_TEST(QueryCostEstimator, Union) {
  auto no_vertices = 4;
  this->AddVertices(no_vertices, 0, 0);

  std::vector<Symbol> union_symbols{this->NextSymbol()};
  std::shared_ptr<LogicalOperator> left_op = std::make_shared<ScanAll>(std::make_shared<Once>(), this->NextSymbol());
  std::shared_ptr<LogicalOperator> right_op = std::make_shared<ScanAll>(std::make_shared<Once>(), this->NextSymbol());
  this->template MakeOp<memgraph::query::plan::Union>(left_op, right_op, union_symbols,
                                                      left_op->OutputSymbols(this->symbol_table_),
                                                      right_op->OutputSymbols(this->symbol_table_));
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

TYPED_TEST(QueryCostEstimator, Filter) {
  TEST_OP(this->template MakeOp<Filter>(this->last_op_, std::vector<std::shared_ptr<LogicalOperator>>{},
                                        this->Literal(true)),
          CostParam::kFilter, CardParam::kFilter);
}

TYPED_TEST(QueryCostEstimator, EdgeUniquenessFilter) {
  TEST_OP(this->template MakeOp<EdgeUniquenessFilter>(this->last_op_, this->NextSymbol(), std::vector<Symbol>()),
          CostParam::kEdgeUniquenessFilter, CardParam::kEdgeUniquenessFilter);
}

TYPED_TEST(QueryCostEstimator, UnwindLiteral) {
  TEST_OP(this->template MakeOp<memgraph::query::plan::Unwind>(
              this->last_op_, this->storage_.template Create<ListLiteral>(std::vector<Expression *>(7, nullptr)),
              this->NextSymbol()),
          CostParam::kUnwind, 7);
}

TYPED_TEST(QueryCostEstimator, UnwindNoLiteral) {
  TEST_OP(this->template MakeOp<memgraph::query::plan::Unwind>(this->last_op_, nullptr, this->NextSymbol()),
          CostParam::kUnwind, MiscParam::kUnwindNoLiteral);
}

#undef TEST_OP
#undef EXPECT_COST
//
// TODO test cost when ScanAll, Expand, Accumulate, Limit
// vs cost for SA, Expand, Limit
