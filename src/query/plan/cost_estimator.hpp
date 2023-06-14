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

#pragma once

#include "query/frontend/ast/ast.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::plan {

/**
 * Query plan execution time cost estimator, for comparing and choosing optimal
 * execution plans.
 *
 * In Cypher the write part of the query always executes in the same
 * cardinality. It is not allowed to execute a write operation before all the
 * expansion for that query part (WITH splits a query into parts) have executed.
 * For that reason cost estimation comes down to cardinality estimation for the
 * read parts of the query, and their expansion. We want to compare different
 * plans and try to figure out which has the optimal organization of scans,
 * expansions and filters.
 *
 * Note that expansions and filtering can also happen during Merge, which is a
 * write operation. We let that get evaluated like all other cardinality
 * influencing ops. Also, Merge cardinality modification should be contained (it
 * can never reduce it's input cardinality), but since Merge always happens
 * after the read part, and can't be reordered, we can ignore that.
 *
 * Limiting and accumulating (Aggregate, OrderBy, Accumulate) operations are
 * cardinality modifiers that always execute at the end of the query part. Their
 * cardinality influence is irrelevant because they execute the same
 * for all plans for a single query part, and query part reordering is not
 * allowed.
 *
 * This kind of cost estimation can only be used for comparing logical plans.
 * It's aim is to estimate cost(A) to be less then cost(B) in every case where
 * actual query execution for plan A is less then that of plan B. It can NOT be
 * used to estimate how MUCH execution between A and B will differ.
 */
template <class TDbAccessor>
class CostEstimator : public HierarchicalLogicalOperatorVisitor {
 public:
  struct CostParam {
    static constexpr double kScanAll{1.0};
    static constexpr double kScanAllByLabel{1.1};
    static constexpr double MakeScanAllByLabelPropertyValue{1.1};
    static constexpr double MakeScanAllByLabelPropertyRange{1.1};
    static constexpr double MakeScanAllByLabelProperty{1.1};
    static constexpr double kExpand{2.0};
    static constexpr double kExpandVariable{3.0};
    static constexpr double kFilter{1.5};
    static constexpr double kEdgeUniquenessFilter{1.5};
    static constexpr double kUnwind{1.3};
    static constexpr double kForeach{1.0};
    static constexpr double kUnion{1.0};
    static constexpr double kSubquery{1.0};
  };

  struct CardParam {
    static constexpr double kExpand{3.0};
    static constexpr double kExpandVariable{9.0};
    static constexpr double kFilter{0.25};
    static constexpr double kEdgeUniquenessFilter{0.95};
  };

  struct MiscParam {
    static constexpr double kUnwindNoLiteral{10.0};
    static constexpr double kForeachNoLiteral{10.0};
  };

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;

  CostEstimator(TDbAccessor *db_accessor, const Parameters &parameters)
      : db_accessor_(db_accessor), parameters(parameters) {}

  bool PostVisit(ScanAll &) override {
    cardinality_ *= db_accessor_->VerticesCount();
    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAll);
    return true;
  }

  bool PostVisit(ScanAllByLabel &scan_all_by_label) override {
    cardinality_ *= db_accessor_->VerticesCount(scan_all_by_label.label_);
    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByLabel);
    return true;
  }

  bool PostVisit(ScanAllByLabelPropertyValue &logical_op) override {
    // This cardinality estimation depends on the property value (expression).
    // If it's a constant, we can evaluate cardinality exactly, otherwise
    // we estimate
    auto property_value = ConstPropertyValue(logical_op.expression_);
    double factor = 1.0;
    if (property_value)
      // get the exact influence based on ScanAll(label, property, value)
      factor = db_accessor_->VerticesCount(logical_op.label_, logical_op.property_, property_value.value());
    else
      // estimate the influence as ScanAll(label, property) * filtering
      factor = db_accessor_->VerticesCount(logical_op.label_, logical_op.property_) * CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::MakeScanAllByLabelPropertyValue);
    return true;
  }

  bool PostVisit(ScanAllByLabelPropertyRange &logical_op) override {
    // this cardinality estimation depends on Bound expressions.
    // if they are literals we can evaluate cardinality properly
    auto lower = BoundToPropertyValue(logical_op.lower_bound_);
    auto upper = BoundToPropertyValue(logical_op.upper_bound_);

    int64_t factor = 1;
    if (upper || lower)
      // if we have either Bound<PropertyValue>, use the value index
      factor = db_accessor_->VerticesCount(logical_op.label_, logical_op.property_, lower, upper);
    else
      // no values, but we still have the label
      factor = db_accessor_->VerticesCount(logical_op.label_, logical_op.property_);

    // if we failed to take either bound from the op into account, then apply
    // the filtering constant to the factor
    if ((logical_op.upper_bound_ && !upper) || (logical_op.lower_bound_ && !lower)) factor *= CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::MakeScanAllByLabelPropertyRange);
    return true;
  }

  bool PostVisit(ScanAllByLabelProperty &logical_op) override {
    const auto factor = db_accessor_->VerticesCount(logical_op.label_, logical_op.property_);
    cardinality_ *= factor;
    IncrementCost(CostParam::MakeScanAllByLabelProperty);
    return true;
  }

  // TODO: Cost estimate ScanAllById?

// For the given op first increments the cardinality and then cost.
#define POST_VISIT_CARD_FIRST(NAME)     \
  bool PostVisit(NAME &) override {     \
    cardinality_ *= CardParam::k##NAME; \
    IncrementCost(CostParam::k##NAME);  \
    return true;                        \
  }

  POST_VISIT_CARD_FIRST(Expand);
  POST_VISIT_CARD_FIRST(ExpandVariable);

#undef POST_VISIT_CARD_FIRST

// For the given op first increments the cost and then cardinality.
#define POST_VISIT_COST_FIRST(LOGICAL_OP, PARAM_NAME) \
  bool PostVisit(LOGICAL_OP &) override {             \
    IncrementCost(CostParam::PARAM_NAME);             \
    cardinality_ *= CardParam::PARAM_NAME;            \
    return true;                                      \
  }

  POST_VISIT_COST_FIRST(Filter, kFilter)
  POST_VISIT_COST_FIRST(EdgeUniquenessFilter, kEdgeUniquenessFilter);

#undef POST_VISIT_COST_FIRST

  bool PostVisit(Unwind &unwind) override {
    // Unwind cost depends more on the number of lists that get unwound
    // much less on the number of outputs
    // for that reason first increment cost, then modify cardinality
    IncrementCost(CostParam::kUnwind);

    // try to determine how many values will be yielded by Unwind
    // if the Unwind expression is a list literal, we can deduce cardinality
    // exactly, otherwise we approximate
    double unwind_value;
    if (auto *literal = utils::Downcast<query::ListLiteral>(unwind.input_expression_))
      unwind_value = literal->elements_.size();
    else
      unwind_value = MiscParam::kUnwindNoLiteral;

    cardinality_ *= unwind_value;
    return true;
  }

  bool PostVisit(Foreach &foreach) override {
    // Foreach cost depends both on the number elements in the list that get unwound
    // as well as the total clauses that get called for each unwounded element.
    // First estimate cardinality and then increment the cost.

    double foreach_elements{0};
    if (auto *literal = utils::Downcast<query::ListLiteral>(foreach.expression_)) {
      foreach_elements = literal->elements_.size();
    } else {
      foreach_elements = MiscParam::kForeachNoLiteral;
    }

    cardinality_ *= foreach_elements;
    IncrementCost(CostParam::kForeach);
    return true;
  }

  bool PreVisit(Union &op) override {
    double left_cost = EstimateCostOnBranch(&op.left_op_);
    double right_cost = EstimateCostOnBranch(&op.right_op_);

    // the number of hits in the previous operator should be the joined number of results of both parts of the union
    cardinality_ *= (left_cost + right_cost);
    IncrementCost(CostParam::kUnion);

    return false;
  }

  bool PreVisit(Apply &op) override {
    double input_cost = EstimateCostOnBranch(&op.input_);
    double subquery_cost = EstimateCostOnBranch(&op.subquery_);

    // if the query is a unit subquery, we don't want the cost to be zero but 1xN
    input_cost = input_cost == 0 ? 1 : input_cost;
    subquery_cost = subquery_cost == 0 ? 1 : subquery_cost;

    cardinality_ *= input_cost * subquery_cost;
    IncrementCost(CostParam::kSubquery);

    return false;
  }

  bool Visit(Once &) override { return true; }

  auto cost() const { return cost_; }
  auto cardinality() const { return cardinality_; }

 private:
  // cost estimation that gets accumulated as the visitor
  // tours the logical plan
  double cost_{0};

  // cardinality estimation (how many times an operator gets executed)
  // cardinality is a double to make it easier to work with
  double cardinality_{1};

  // accessor used for cardinality estimates in ScanAll and ScanAllByLabel
  TDbAccessor *db_accessor_;
  const Parameters &parameters;

  void IncrementCost(double param) { cost_ += param * cardinality_; }

  double EstimateCostOnBranch(std::shared_ptr<LogicalOperator> *branch) {
    CostEstimator<TDbAccessor> cost_estimator(db_accessor_, parameters);
    (*branch)->Accept(cost_estimator);
    return cost_estimator.cost();
  }

  // converts an optional ScanAll range bound into a property value
  // if the bound is present and is a constant expression convertible to
  // a property value. otherwise returns nullopt
  std::optional<utils::Bound<storage::PropertyValue>> BoundToPropertyValue(
      std::optional<ScanAllByLabelPropertyRange::Bound> bound) {
    if (bound) {
      auto property_value = ConstPropertyValue(bound->value());
      if (property_value) return utils::Bound<storage::PropertyValue>(*property_value, bound->type());
    }
    return std::nullopt;
  }

  // If the expression is a constant property value, it is returned. Otherwise,
  // return nullopt.
  std::optional<storage::PropertyValue> ConstPropertyValue(const Expression *expression) {
    if (auto *literal = utils::Downcast<const PrimitiveLiteral>(expression)) {
      return literal->value_;
    } else if (auto *param_lookup = utils::Downcast<const ParameterLookup>(expression)) {
      return parameters.AtTokenPosition(param_lookup->token_position_);
    }
    return std::nullopt;
  }
};

/** Returns the estimated cost of the given plan. */
template <class TDbAccessor>
double EstimatePlanCost(TDbAccessor *db, const Parameters &parameters, LogicalOperator &plan) {
  CostEstimator<TDbAccessor> estimator(db, parameters);
  plan.Accept(estimator);
  return estimator.cost();
}

}  // namespace memgraph::query::plan
