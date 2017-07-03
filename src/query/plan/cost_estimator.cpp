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
  // ScanAllByLabel performs some work for every element that is produced
  IncrementCost(CostParam::kScanAllByLabel);
  return true;
}

bool CostEstimator::PostVisit(Expand &) {
  cardinality_ *= CardParam::kExpand;
  // Expand performs some work for every expansion
  IncrementCost(CostParam::kExpand);
  return true;
}

// for the given op first increments the cost and then cardinality
#define POST_VISIT(LOGICAL_OP, PARAM_NAME)      \
  bool CostEstimator::PostVisit(LOGICAL_OP &) { \
    IncrementCost(CostParam::PARAM_NAME);       \
    cardinality_ *= CardParam::PARAM_NAME;      \
    return true;                                \
  }

POST_VISIT(Filter, kFilter)
POST_VISIT(ExpandUniquenessFilter<VertexAccessor>, kExpandUniquenessFilter);
POST_VISIT(ExpandUniquenessFilter<EdgeAccessor>, kExpandUniquenessFilter);

#undef POST_VISIT

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
