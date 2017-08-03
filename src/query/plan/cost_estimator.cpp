#include <experimental/optional>

#include "cost_estimator.hpp"

namespace query::plan {

bool CostEstimator::PostVisit(ScanAll &) {
  cardinality_ *= db_accessor_.vertices_count();
  // ScanAll performs some work for every element that is produced
  IncrementCost(CostParam::kScanAll);
  return true;
}

bool CostEstimator::PostVisit(ScanAllByLabel &scan_all_by_label) {
  cardinality_ *= db_accessor_.vertices_count(scan_all_by_label.label());
  // ScanAll performs some work for every element that is produced
  IncrementCost(CostParam::kScanAllByLabel);
  return true;
}

bool CostEstimator::PostVisit(ScanAllByLabelPropertyValue &logical_op) {
  // this cardinality estimation depends on the property value (expression).
  // if it's a literal (const) we can evaluate cardinality exactly, otherwise
  // we estimate
  std::experimental::optional<PropertyValue> property_value =
      std::experimental::nullopt;
  if (auto *literal = dynamic_cast<PrimitiveLiteral *>(logical_op.expression()))
    if (literal->value_.IsPropertyValue())
      property_value =
          std::experimental::optional<PropertyValue>(literal->value_);

  double factor = 1.0;
  if (property_value)
    // get the exact influence based on ScanAll(label, property, value)
    factor = db_accessor_.vertices_count(
        logical_op.label(), logical_op.property(), property_value.value());
  else
    // estimate the influence as ScanAll(label, property) * filtering
    factor =
        db_accessor_.vertices_count(logical_op.label(), logical_op.property()) *
        CardParam::kFilter;

  cardinality_ *= factor;

  // ScanAll performs some work for every element that is produced
  IncrementCost(CostParam::MakeScanAllByLabelPropertyValue);
  return true;
}

namespace {
// converts an optional ScanAll range bound into a property value
// if the bound is present and is a literal expression convertible to
// a property value. otherwise returns nullopt
std::experimental::optional<utils::Bound<PropertyValue>> BoundToPropertyValue(
    std::experimental::optional<ScanAllByLabelPropertyRange::Bound> bound) {
  if (bound)
    if (auto *literal = dynamic_cast<PrimitiveLiteral *>(bound->value()))
      return std::experimental::make_optional(
          utils::Bound<PropertyValue>(literal->value_, bound->type()));
  return std::experimental::nullopt;
}
}

bool CostEstimator::PostVisit(ScanAllByLabelPropertyRange &logical_op) {
  // this cardinality estimation depends on Bound expressions.
  // if they are literals we can evaluate cardinality properly
  auto lower = BoundToPropertyValue(logical_op.lower_bound());
  auto upper = BoundToPropertyValue(logical_op.upper_bound());

  int64_t factor = 1;
  if (upper || lower)
    // if we have either Bound<PropertyValue>, use the value index
    factor = db_accessor_.vertices_count(logical_op.label(),
                                         logical_op.property(), lower, upper);
  else
    // no values, but we still have the label
    factor =
        db_accessor_.vertices_count(logical_op.label(), logical_op.property());

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
#define POST_VISIT_CARD_FIRST(NAME)       \
  bool CostEstimator::PostVisit(NAME &) { \
    cardinality_ *= CardParam::k##NAME;   \
    IncrementCost(CostParam::k##NAME);    \
    return true;                          \
  }

POST_VISIT_CARD_FIRST(Expand);
POST_VISIT_CARD_FIRST(ExpandVariable);
POST_VISIT_CARD_FIRST(ExpandBreadthFirst);

#undef POST_VISIT_CARD_FIRST

// For the given op first increments the cost and then cardinality.
#define POST_VISIT_COST_FIRST(LOGICAL_OP, PARAM_NAME) \
  bool CostEstimator::PostVisit(LOGICAL_OP &) {       \
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

bool CostEstimator::PostVisit(Unwind &unwind) {
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

bool CostEstimator::Visit(Once &) { return true; }
bool CostEstimator::Visit(CreateIndex &) { return true; }

}  // namespace query::plan
