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

#pragma once

#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/rewrite/index_lookup.hpp"
#include "utils/algorithm.hpp"
#include "utils/math.hpp"

namespace memgraph::query::plan {

/**
 * The symbol statistics specify essential DB statistics which
 * help the query planner (namely here the cost estimator), to decide
 * how to do expands and other types of Cypher manipulations.
 */
struct SymbolStatistics {
  uint64_t count;
  double degree;
};

/**
 * Scope of the statistics for every scanned symbol in
 * the operator tree.
 */
struct Scope {
  std::unordered_map<std::string, SymbolStatistics> symbol_stats;
};

struct CostEstimation {
  // expense of running the query
  double cost;

  // expected number of rows
  double cardinality;
};

struct PlanCost {
  double cost;
  bool use_index_hints;
};

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
 * after the read part, and can't be reoredered, we can ignore that.
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
    static constexpr double kMinimumCost{0.001};  // everything has some runtime cost
    static constexpr double kScanAll{1.0};
    static constexpr double kScanAllByLabel{1.1};
    static constexpr double kScanAllByLabelProperties{1.1};
    static constexpr double kScanAllByPointDistance{1.1};
    static constexpr double kScanAllByPointWithinbbox{1.1};
    static constexpr double kScanAllByEdgeType{1.1};
    static constexpr double kScanAllByEdgeTypePropertyValue{1.1};
    static constexpr double kScanAllByEdgeTypePropertyRange{1.1};
    static constexpr double kScanAllByEdgeTypeProperty{1.1};
    static constexpr double kScanAllByEdgePropertyValue{1.1};
    static constexpr double kScanAllByEdgePropertyRange{1.1};
    static constexpr double kScanAllByEdgeProperty{1.1};
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

  CostEstimator(TDbAccessor *db_accessor, const SymbolTable &table, const Parameters &parameters,
                const IndexHints &index_hints)
      : db_accessor_(db_accessor), table_(table), parameters(parameters), scopes_{Scope()}, index_hints_(index_hints) {}

  CostEstimator(TDbAccessor *db_accessor, const SymbolTable &table, const Parameters &parameters, Scope scope,
                const IndexHints &index_hints)
      : db_accessor_(db_accessor), table_(table), parameters(parameters), scopes_{scope}, index_hints_(index_hints) {}

  bool PostVisit(ScanAll &) override {
    cardinality_ *= db_accessor_->VerticesCount();
    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAll);
    return true;
  }

  bool PostVisit(ScanAllByLabel &scan_all_by_label) override {
    auto index_stats = db_accessor_->GetIndexStats(scan_all_by_label.label_);
    if (index_stats.has_value()) {
      SaveStatsFor(scan_all_by_label.output_symbol_, index_stats.value());
    }

    cardinality_ *= db_accessor_->VerticesCount(scan_all_by_label.label_);
    if (index_hints_.HasLabelIndex(db_accessor_, scan_all_by_label.label_)) {
      use_index_hints_ = true;
    }

    IncrementCost(CostParam::kScanAllByLabel);
    return true;
  }

  bool PostVisit(ScanAllByLabelProperties &logical_op) override {
    auto index_stats = db_accessor_->GetIndexStats(logical_op.label_, logical_op.properties_);
    if (index_stats.has_value()) {
      SaveStatsFor(logical_op.output_symbol_, index_stats.value());
    }

    // this cardinality estimation depends on Bound expressions.
    // if they are literals we can evaluate cardinality properly

    auto maybe_propertyvalue_ranges =
        logical_op.expression_ranges_ |
        ranges::views::transform([&](ExpressionRange const &er) { return er.ResolveAtPlantime(parameters); }) |
        ranges::to_vector;

    auto factor = std::invoke([&]() -> double {
      if (ranges::none_of(maybe_propertyvalue_ranges, [](auto &&pvr) { return pvr == std::nullopt; })) {
        auto propertyvalue_ranges = maybe_propertyvalue_ranges |
                                    ranges::views::transform([](auto &&optional) { return *optional; }) |
                                    ranges::to_vector;

        return db_accessor_->VerticesCount(logical_op.label_, logical_op.properties_, propertyvalue_ranges);
      } else {
        // no values, but we still have the label + properties
        // use filtering constant to modify the factor
        return db_accessor_->VerticesCount(logical_op.label_, logical_op.properties_) * CardParam::kFilter;
      }
    });

    cardinality_ *= factor;

    if (index_hints_.HasLabelPropertiesIndex(db_accessor_, logical_op.label_, logical_op.properties_)) {
      use_index_hints_ = true;
    }

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByLabelProperties);
    return true;
  }

  bool PostVisit(ScanAllByPointDistance &logical_op) override {
    // FYI, no stats for point types

    const auto factor = db_accessor_->VerticesPointCount(logical_op.label_, logical_op.property_);
    cardinality_ *= factor.value_or(1);
    if (index_hints_.HasPointIndex(db_accessor_, logical_op.label_, logical_op.property_)) {
      use_index_hints_ = true;
    }

    IncrementCost(CostParam::kScanAllByPointDistance);
    return true;
  }

  bool PostVisit(ScanAllByPointWithinbbox &logical_op) override {
    // FYI, no stats for point types

    const auto factor = db_accessor_->VerticesPointCount(logical_op.label_, logical_op.property_);
    cardinality_ *= factor.value_or(1);
    if (index_hints_.HasPointIndex(db_accessor_, logical_op.label_, logical_op.property_)) {
      use_index_hints_ = true;
    }

    IncrementCost(CostParam::kScanAllByPointWithinbbox);
    return true;
  }

  bool PostVisit(ScanAllByEdgeType &op) override {
    auto edge_type = op.GetEdgeType();
    cardinality_ *= db_accessor_->EdgesCount(edge_type);
    IncrementCost(CostParam::kScanAllByEdgeType);
    return true;
  }

  bool PostVisit(ScanAllByEdgeTypeProperty &op) override {
    auto edge_type = op.GetEdgeType();
    const auto factor = db_accessor_->EdgesCount(edge_type, op.property_);
    cardinality_ *= factor;

    IncrementCost(CostParam::kScanAllByEdgeTypeProperty);
    return true;
  }

  bool PostVisit(ScanAllByEdgeTypePropertyValue &op) override {
    auto edge_type = op.GetEdgeType();
    auto property_value = ConstPropertyValue(op.expression_);
    double factor = 1.0;
    if (property_value)
      // get the exact influence based on ScanAllByEdge(label, property, value)
      factor = db_accessor_->EdgesCount(edge_type, op.property_, property_value.value());
    else
      // estimate the influence as ScanAllByEdge(label, property) * filtering
      factor = db_accessor_->EdgesCount(edge_type, op.property_) * CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByEdgeTypePropertyValue);
    return true;
  }

  bool PostVisit(ScanAllByEdgeTypePropertyRange &op) override {
    auto edge_type = op.GetEdgeType();

    // this cardinality estimation depends on Bound expressions.
    // if they are literals we can evaluate cardinality properly
    auto lower = BoundToPropertyValue(op.lower_bound_);
    auto upper = BoundToPropertyValue(op.upper_bound_);

    int64_t factor = 1;
    if (upper || lower)
      // if we have either Bound<PropertyValue>, use the value index
      factor = db_accessor_->EdgesCount(edge_type, op.property_, lower, upper);
    else
      // no values, but we still have the label
      factor = db_accessor_->EdgesCount(edge_type, op.property_);

    // if we failed to take either bound from the op into account, then apply
    // the filtering constant to the factor
    if ((op.upper_bound_ && !upper) || (op.lower_bound_ && !lower)) factor *= CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByEdgeTypePropertyRange);
    return true;
  }

  bool PostVisit(ScanAllByEdgeProperty &op) override {
    const auto factor = db_accessor_->EdgesCount(op.property_);
    cardinality_ *= factor;

    IncrementCost(CostParam::kScanAllByEdgeProperty);
    return true;
  }

  bool PostVisit(ScanAllByEdgePropertyValue &op) override {
    auto property_value = ConstPropertyValue(op.expression_);
    double factor = 1.0;
    if (property_value)
      // get the exact influence based on ScanAllByEdge(label, property, value)
      factor = db_accessor_->EdgesCount(op.property_, property_value.value());
    else
      // estimate the influence as ScanAllByEdge(label, property) * filtering
      factor = db_accessor_->EdgesCount(op.property_) * CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByEdgePropertyValue);
    return true;
  }

  bool PostVisit(ScanAllByEdgePropertyRange &op) override {
    // this cardinality estimation depends on Bound expressions.
    // if they are literals we can evaluate cardinality properly
    auto lower = BoundToPropertyValue(op.lower_bound_);
    auto upper = BoundToPropertyValue(op.upper_bound_);

    int64_t factor = 1;
    if (upper || lower)
      // if we have either Bound<PropertyValue>, use the value index
      factor = db_accessor_->EdgesCount(op.property_, lower, upper);
    else
      // no values, but we still have the label
      factor = db_accessor_->EdgesCount(op.property_);

    // if we failed to take either bound from the op into account, then apply
    // the filtering constant to the factor
    if ((op.upper_bound_ && !upper) || (op.lower_bound_ && !lower)) factor *= CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByEdgePropertyRange);
    return true;
  }

  // TODO: Cost estimate ScanAllById?

  bool PostVisit(Expand &expand) override {
    auto card_param = CardParam::kExpand;
    auto stats = GetStatsFor(expand.input_symbol_);

    if (stats.has_value()) {
      card_param = stats.value().degree;
    }

    cardinality_ *= card_param;
    IncrementCost(CostParam::kExpand);

    return true;
  }

// For the given op first increments the cardinality and then cost.
#define POST_VISIT_CARD_FIRST(NAME)     \
  bool PostVisit(NAME &) override {     \
    cardinality_ *= CardParam::k##NAME; \
    IncrementCost(CostParam::k##NAME);  \
    return true;                        \
  }

  POST_VISIT_CARD_FIRST(ExpandVariable);

#undef POST_VISIT_CARD_FIRST

// For the given op first increments the cost and then cardinality.
#define POST_VISIT_COST_FIRST(LOGICAL_OP, PARAM_NAME) \
  bool PostVisit(LOGICAL_OP &) override {             \
    IncrementCost(CostParam::PARAM_NAME);             \
    cardinality_ *= CardParam::PARAM_NAME;            \
    return true;                                      \
  }

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
    CostEstimation left_estimation = EstimateCostOnBranch(&op.left_op_);
    CostEstimation right_estimation = EstimateCostOnBranch(&op.right_op_);

    // the number of hits in the previous operator should be the joined number of results of both parts of the union
    cost_ = left_estimation.cost + right_estimation.cost;
    cardinality_ = left_estimation.cardinality + right_estimation.cardinality;

    return false;
  }

  bool PostVisit(Produce &op) override {
    auto scope = Scope();

    // translate all the stats to the scope outside the return
    for (const auto &symbol : op.ModifiedSymbols(table_)) {
      auto stats = GetStatsFor(symbol);
      if (stats.has_value()) {
        scope.symbol_stats[symbol.name()] =
            SymbolStatistics{.count = stats.value().count, .degree = stats.value().degree};
      }
    }

    scopes_.push_back(std::move(scope));
    return true;
  }

  bool PreVisit(Filter &op) override {
    op.input_->Accept(*this);
    auto total_branch_cost = 0.0;
    for (auto const &pattern_filter : op.pattern_filters_) {
      auto &last_scope = scopes_.back();
      CostEstimation pattern_estimation = EstimateCostOnBranch(&pattern_filter, last_scope);
      total_branch_cost += pattern_estimation.cost;
    }
    IncrementCost(std::max(total_branch_cost, CostParam::kFilter));
    cardinality_ *= CardParam::kFilter;
    return false;
  }

  bool PreVisit(Apply &op) override {
    // Get the cost of the main branch
    op.input_->Accept(*this);

    // Estimate cost on the subquery branch independently, use a copy
    auto &last_scope = scopes_.back();
    CostEstimation subquery_estimation = EstimateCostOnBranch(&op.subquery_, last_scope);
    double subquery_cost = !utils::ApproxEqualDecimal(subquery_estimation.cost, 0.0) ? subquery_estimation.cost : 1;
    IncrementCost(subquery_cost);

    double subquery_cardinality =
        !utils::ApproxEqualDecimal(subquery_estimation.cardinality, 0.0) ? subquery_estimation.cardinality : 1;
    cardinality_ *= subquery_cardinality;

    return false;
  }

  bool PreVisit(Cartesian &op) override {
    // Get the cost of the main branch
    op.left_op_->Accept(*this);

    // add cost from the right branch and multiply cardinalities
    CostEstimation right_cost_estimation = EstimateCostOnBranch(&op.right_op_);
    cost_ += right_cost_estimation.cost;
    double right_cardinality =
        !utils::ApproxEqualDecimal(right_cost_estimation.cardinality, 0.0) ? right_cost_estimation.cardinality : 1;
    cardinality_ *= right_cardinality;

    return false;
  }

  bool PreVisit(IndexedJoin &op) override {
    // Get the cost of the main branch
    op.main_branch_->Accept(*this);

    // add cost from the right branch and multiply cardinalities
    CostEstimation right_cost_estimation = EstimateCostOnBranch(&op.sub_branch_);
    IncrementCost(right_cost_estimation.cost);

    double right_cardinality =
        !utils::ApproxEqualDecimal(right_cost_estimation.cardinality, 0.0) ? right_cost_estimation.cardinality : 1;
    cardinality_ *= right_cardinality;

    return false;
  }

  bool PreVisit(HashJoin &op) override {
    // Get the cost of the main branch
    op.left_op_->Accept(*this);

    // add cost from the right branch and multiply cardinalities
    CostEstimation right_cost_estimation = EstimateCostOnBranch(&op.right_op_);
    IncrementCost(right_cost_estimation.cost);

    double right_cardinality =
        !utils::ApproxEqualDecimal(right_cost_estimation.cardinality, 0.0) ? right_cost_estimation.cardinality : 1;
    cardinality_ *= right_cardinality;

    return false;
  }

  bool PostVisit(EmptyResult & /*op*/) override {
    scopes_.emplace_back();
    return true;
  }

  bool Visit(Once &) override { return true; }

  auto cost() const { return cost_; }
  auto cardinality() const { return cardinality_; }
  auto use_index_hints() const { return use_index_hints_; }

 private:
  // cost estimation that gets accumulated as the visitor
  // tours the logical plan
  double cost_{0};

  // cardinality estimation (how many times an operator gets executed)
  // cardinality is a double to k it easier to work with
  double cardinality_{1};

  // accessor used for cardinality estimates in ScanAll and ScanAllByLabel
  TDbAccessor *db_accessor_;
  const SymbolTable &table_;
  const Parameters &parameters;
  std::vector<Scope> scopes_;
  IndexHints index_hints_;
  bool use_index_hints_{false};

  void IncrementCost(double param) { cost_ += std::max(CostParam::kMinimumCost, param * cardinality_); }

  CostEstimation EstimateCostOnBranch(std::shared_ptr<LogicalOperator> *branch) {
    CostEstimator<TDbAccessor> cost_estimator(db_accessor_, table_, parameters, index_hints_);
    (*branch)->Accept(cost_estimator);
    return CostEstimation{.cost = cost_estimator.cost(), .cardinality = cost_estimator.cardinality()};
  }

  CostEstimation EstimateCostOnBranch(std::shared_ptr<LogicalOperator> const *branch, Scope scope) {
    CostEstimator<TDbAccessor> cost_estimator(db_accessor_, table_, parameters, scope, index_hints_);
    (*branch)->Accept(cost_estimator);
    return CostEstimation{.cost = cost_estimator.cost(), .cardinality = cost_estimator.cardinality()};
  }

  // converts an optional ScanAll range bound into a property value
  // if the bound is present and is a constant expression convertible to
  // a property value. otherwise returns nullopt
  std::optional<utils::Bound<storage::PropertyValue>> BoundToPropertyValue(
      std::optional<ScanAllByEdgeTypePropertyRange::Bound> bound) {
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

  bool HasStatsFor(const Symbol &symbol) const { return utils::Contains(scopes_.back().symbol_stats, symbol.name()); }

  std::optional<SymbolStatistics> GetStatsFor(const Symbol &symbol) {
    if (!HasStatsFor(symbol)) {
      return std::nullopt;
    }

    auto &scope = scopes_.back();
    return scope.symbol_stats[symbol.name()];
  }

  template <typename T>
  void SaveStatsFor(const Symbol &symbol, T index_stats) {
    scopes_.back().symbol_stats[symbol.name()] = SymbolStatistics{
        .count = index_stats.count,
        .degree = index_stats.avg_degree,
    };
  }
};

/** Returns the estimated cost of the given plan. */
template <class TDbAccessor>
PlanCost EstimatePlanCost(TDbAccessor *db, const SymbolTable &table, const Parameters &parameters,
                          LogicalOperator &plan, const IndexHints &index_hints) {
  CostEstimator<TDbAccessor> estimator(db, table, parameters, index_hints);
  plan.Accept(estimator);
  return PlanCost{.cost = estimator.cost(), .use_index_hints = estimator.use_index_hints()};
}

}  // namespace memgraph::query::plan
