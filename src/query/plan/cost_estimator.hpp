#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/typed_value.hpp"

namespace query::plan {

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
    static constexpr double kScanAll{1.0};
    static constexpr double kScanAllByLabel{1.1};
    static constexpr double MakeScanAllByLabelPropertyValue{1.1};
    static constexpr double MakeScanAllByLabelPropertyRange{1.1};
    static constexpr double kExpand{2.0};
    static constexpr double kExpandVariable{3.0};
    static constexpr double kExpandBreadthFirst{5.0};
    static constexpr double kFilter{1.5};
    static constexpr double kExpandUniquenessFilter{1.5};
    static constexpr double kUnwind{1.3};
  };

  struct CardParam {
    static constexpr double kExpand{3.0};
    static constexpr double kExpandVariable{9.0};
    static constexpr double kExpandBreadthFirst{8.0};
    static constexpr double kFilter{0.25};
    static constexpr double kExpandUniquenessFilter{0.95};
  };

  struct MiscParam {
    static constexpr double kUnwindNoLiteral{10.0};
  };

  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::PostVisit;

  CostEstimator(const TDbAccessor &db_accessor) : db_accessor_(db_accessor) {}

  bool PostVisit(ScanAll &) override {
    cardinality_ *= db_accessor_.VerticesCount();
    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAll);
    return true;
  }

  bool PostVisit(ScanAllByLabel &scan_all_by_label) override {
    cardinality_ *= db_accessor_.VerticesCount(scan_all_by_label.label());
    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::kScanAllByLabel);
    return true;
  }

  bool PostVisit(ScanAllByLabelPropertyValue &logical_op) override {
    // this cardinality estimation depends on the property value (expression).
    // if it's a literal (const) we can evaluate cardinality exactly, otherwise
    // we estimate
    std::experimental::optional<PropertyValue> property_value =
        std::experimental::nullopt;
    if (auto *literal =
            dynamic_cast<PrimitiveLiteral *>(logical_op.expression()))
      if (literal->value_.IsPropertyValue())
        property_value =
            std::experimental::optional<PropertyValue>(literal->value_);

    double factor = 1.0;
    if (property_value)
      // get the exact influence based on ScanAll(label, property, value)
      factor = db_accessor_.VerticesCount(
          logical_op.label(), logical_op.property(), property_value.value());
    else
      // estimate the influence as ScanAll(label, property) * filtering
      factor = db_accessor_.VerticesCount(logical_op.label(),
                                          logical_op.property()) *
               CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::MakeScanAllByLabelPropertyValue);
    return true;
  }

  bool PostVisit(ScanAllByLabelPropertyRange &logical_op) override {
    // this cardinality estimation depends on Bound expressions.
    // if they are literals we can evaluate cardinality properly
    auto lower = BoundToPropertyValue(logical_op.lower_bound());
    auto upper = BoundToPropertyValue(logical_op.upper_bound());

    int64_t factor = 1;
    if (upper || lower)
      // if we have either Bound<PropertyValue>, use the value index
      factor = db_accessor_.VerticesCount(logical_op.label(),
                                          logical_op.property(), lower, upper);
    else
      // no values, but we still have the label
      factor =
          db_accessor_.VerticesCount(logical_op.label(), logical_op.property());

    // if we failed to take either bound from the op into account, then apply
    // the filtering constant to the factor
    if ((logical_op.upper_bound() && !upper) ||
        (logical_op.lower_bound() && !lower))
      factor *= CardParam::kFilter;

    cardinality_ *= factor;

    // ScanAll performs some work for every element that is produced
    IncrementCost(CostParam::MakeScanAllByLabelPropertyRange);
    return true;
  }

// For the given op first increments the cardinality and then cost.
#define POST_VISIT_CARD_FIRST(NAME)     \
  bool PostVisit(NAME &) override {     \
    cardinality_ *= CardParam::k##NAME; \
    IncrementCost(CostParam::k##NAME);  \
    return true;                        \
  }

  POST_VISIT_CARD_FIRST(Expand);
  POST_VISIT_CARD_FIRST(ExpandVariable);
  POST_VISIT_CARD_FIRST(ExpandBreadthFirst);

#undef POST_VISIT_CARD_FIRST

// For the given op first increments the cost and then cardinality.
#define POST_VISIT_COST_FIRST(LOGICAL_OP, PARAM_NAME) \
  bool PostVisit(LOGICAL_OP &) override {             \
    IncrementCost(CostParam::PARAM_NAME);             \
    cardinality_ *= CardParam::PARAM_NAME;            \
    return true;                                      \
  }

  POST_VISIT_COST_FIRST(Filter, kFilter)
  POST_VISIT_COST_FIRST(ExpandUniquenessFilter<VertexAccessor>,
                        kExpandUniquenessFilter);
  POST_VISIT_COST_FIRST(ExpandUniquenessFilter<EdgeAccessor>,
                        kExpandUniquenessFilter);

#undef POST_VISIT_COST_FIRST

  bool PostVisit(Unwind &unwind) override {
    // Unwind cost depends more on the number of lists that get unwound
    // much less on the number of outputs
    // for that reason first increment cost, then modify cardinality
    IncrementCost(CostParam::kUnwind);

    // try to determine how many values will be yielded by Unwind
    // if the Unwind expression is a list literal, we can deduce cardinality
    // exactly, otherwise we approximate
    int unwind_value;
    if (auto literal =
            dynamic_cast<query::ListLiteral *>(unwind.input_expression()))
      unwind_value = literal->elements_.size();
    else
      unwind_value = MiscParam::kUnwindNoLiteral;

    cardinality_ *= unwind_value;
    return true;
  }

  bool Visit(Once &) override { return true; }
  bool Visit(CreateIndex &) override { return true; }

  auto cost() const { return cost_; }
  auto cardinality() const { return cardinality_; }

 private:
  // cost estimation that gets accumulated as the visitor
  // tours the logical plan
  double cost_{0};

  // cardinality estimation (how many times an operator gets executed)
  // cardinality is a double to make it easier to work with
  double cardinality_{1};
  //
  // accessor used for cardinality estimates in ScanAll and ScanAllByLabel
  const TDbAccessor &db_accessor_;

  void IncrementCost(double param) { cost_ += param * cardinality_; }

  // converts an optional ScanAll range bound into a property value
  // if the bound is present and is a literal expression convertible to
  // a property value. otherwise returns nullopt
  static std::experimental::optional<utils::Bound<PropertyValue>>
  BoundToPropertyValue(
      std::experimental::optional<ScanAllByLabelPropertyRange::Bound> bound) {
    if (bound)
      if (auto *literal = dynamic_cast<PrimitiveLiteral *>(bound->value()))
        return std::experimental::make_optional(
            utils::Bound<PropertyValue>(literal->value_, bound->type()));
    return std::experimental::nullopt;
  }
};

/** Returns the estimated cost of the given plan. */
template <class TDbAccessor>
double EstimatePlanCost(TDbAccessor &db, LogicalOperator &plan) {
  CostEstimator<TDbAccessor> estimator(db);
  plan.Accept(estimator);
  return estimator.cost();
}

}  // namespace query::plan
