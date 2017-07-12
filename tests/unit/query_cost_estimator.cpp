#include <gtest/gtest.h>
#include <memory>

#include "database/dbms.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/operator.hpp"
#include "storage/vertex_accessor.hpp"

using namespace query;
using namespace query::plan;

using CardParam = CostEstimator::CardParam;
using CostParam = CostEstimator::CostParam;
using MiscParam = CostEstimator::MiscParam;

/** A fixture for cost estimation. Sets up the database
 * and accessor (adds some vertices). Provides convenience
 * functions for creating the logical plan. Note that the
 * resulting plan is NOT fit for execution, only for cost
 * estimation testing. */
class QueryCostEstimator : public ::testing::Test {
 protected:
  Dbms dbms;
  std::unique_ptr<GraphDbAccessor> dba = dbms.active();

  // we incrementally build the logical operator plan
  // start it off with Once
  std::shared_ptr<LogicalOperator> last_op_ = std::make_shared<Once>();

  AstTreeStorage storage_;
  SymbolTable symbol_table_;
  int symbol_count = 0;

  Symbol NextSymbol() {
    return symbol_table_.CreateSymbol("Symbol" + std::to_string(symbol_count++),
                                      true);
  }

  /** Adds the given number of vertices to the DB, which
   * the given number is labeled with the given label */
  void AddVertices(int vertex_count, GraphDbTypes::Label label,
                   int labeled_count) {
    for (int i = 0; i < vertex_count; i++) {
      auto vertex = dba->insert_vertex();
      if (i < labeled_count) vertex.add_label(label);
    }

    dba->advance_command();
  }

  auto Cost() {
    CostEstimator cost_estimator(*dba);
    last_op_->Accept(cost_estimator);
    return cost_estimator.cost();
  }

  template <typename TLogicalOperator, typename... TArgs>
  void MakeOp(TArgs... args) {
    last_op_ = std::make_shared<TLogicalOperator>(args...);
  }
};

// multiply with 1 to avoid linker error (possibly fixed in CLang >= 3.81)
#define EXPECT_COST(COST) EXPECT_FLOAT_EQ(Cost(), 1 * COST)

TEST_F(QueryCostEstimator, Once) { EXPECT_COST(0); }

TEST_F(QueryCostEstimator, ScanAll) {
  AddVertices(100, dba->label("Label"), 30);
  MakeOp<ScanAll>(last_op_, NextSymbol());
  EXPECT_COST(100 * CostParam::kScanAll);
}

TEST_F(QueryCostEstimator, ScanAllByLabelCardinality) {
  GraphDbTypes::Label label = dba->label("Label");
  AddVertices(100, label, 30);
  MakeOp<ScanAllByLabel>(last_op_, NextSymbol(), label);
  EXPECT_COST(30 * CostParam::kScanAllByLabel);
}

TEST_F(QueryCostEstimator, ExpandCardinality) {
  MakeOp<Expand>(NextSymbol(), NextSymbol(), EdgeAtom::Direction::IN, last_op_,
                 NextSymbol(), false, false);
  EXPECT_COST(CardParam::kExpand * CostParam::kExpand);
}

// helper for testing an operations cost and cardinality
// only for operations that first increment cost, then modify cardinality
// intentially a macro (instead of function) for better test feedback
#define TEST_OP(OP, OP_COST_PARAM, OP_CARD_PARAM) \
  OP;                                             \
  EXPECT_COST(OP_COST_PARAM);                     \
  OP;                                             \
  EXPECT_COST(OP_COST_PARAM + OP_CARD_PARAM * OP_COST_PARAM);

TEST_F(QueryCostEstimator, Filter) {
  TEST_OP(MakeOp<Filter>(last_op_, storage_.Create<PrimitiveLiteral>(true)),
          CostParam::kFilter, CardParam::kFilter);
}

TEST_F(QueryCostEstimator, ExpandUniquenessFilter) {
  TEST_OP(MakeOp<ExpandUniquenessFilter<VertexAccessor>>(last_op_, NextSymbol(),
                                                         std::vector<Symbol>()),
          CostParam::kExpandUniquenessFilter,
          CardParam::kExpandUniquenessFilter);
}

TEST_F(QueryCostEstimator, UnwindLiteral) {
  TEST_OP(MakeOp<query::plan::Unwind>(
              last_op_, storage_.Create<ListLiteral>(
                            std::vector<Expression *>(7, nullptr)),
              NextSymbol()),
          CostParam::kUnwind, 7);
}

TEST_F(QueryCostEstimator, UnwindNoLiteral) {
  TEST_OP(MakeOp<query::plan::Unwind>(last_op_, nullptr, NextSymbol()),
          CostParam::kUnwind, MiscParam::kUnwindNoLiteral);
}

#undef TEST_OP
#undef EXPECT_COST
//
// TODO test cost when ScanAll, Expand, Accumulate, Limit
// vs cost for SA, Expand, Limit
