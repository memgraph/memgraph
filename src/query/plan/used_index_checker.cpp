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

#include "query/plan/used_index_checker.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define PRE_VISIT(TOp) \
  bool UsedIndexChecker::PreVisit(TOp &) { return true; }

namespace memgraph::query::plan {

PRE_VISIT(CreateNode)
PRE_VISIT(CreateExpand)
PRE_VISIT(Delete)

PRE_VISIT(SetProperty)
PRE_VISIT(SetProperties)
PRE_VISIT(SetLabels)

PRE_VISIT(RemoveProperty)
PRE_VISIT(RemoveLabels)

PRE_VISIT(ScanAll)
bool UsedIndexChecker::PreVisit(ScanAllByLabel &op) {
  required_indices_.label_.emplace_back(op.label_);
  return true;
}
bool UsedIndexChecker::PreVisit(ScanAllByLabelProperties &op) {
  required_indices_.label_properties_.emplace_back(op.label_, op.properties_);
  return true;
}

bool UsedIndexChecker::PreVisit(ScanAllByEdgeType &op) {
  required_indices_.edge_type_.emplace_back(op.common_.edge_types[0]);
  return true;
}

bool UsedIndexChecker::PreVisit(ScanAllByEdgeProperty &op) {
  required_indices_.edge_property_.emplace_back(op.property_);
  return true;
}

bool UsedIndexChecker::PreVisit(ScanAllByEdgePropertyValue &op) {
  required_indices_.edge_property_.emplace_back(op.property_);
  return true;
}

bool UsedIndexChecker::PreVisit(ScanAllByEdgePropertyRange &op) {
  required_indices_.edge_property_.emplace_back(op.property_);
  return true;
}

PRE_VISIT(ScanAllById)
PRE_VISIT(ScanAllByEdge)

PRE_VISIT(ScanAllByEdgeTypeProperty)       // TODO: gather for concurrent index check
PRE_VISIT(ScanAllByEdgeTypePropertyValue)  // TODO: gather for concurrent index check
PRE_VISIT(ScanAllByEdgeTypePropertyRange)  // TODO: gather for concurrent index check

PRE_VISIT(ScanAllByEdgeId)

PRE_VISIT(Expand)
PRE_VISIT(ExpandVariable)

PRE_VISIT(ConstructNamedPath)

PRE_VISIT(Filter)
PRE_VISIT(EdgeUniquenessFilter)

PRE_VISIT(Merge)
PRE_VISIT(Optional)

bool UsedIndexChecker::PreVisit(Cartesian &op) {
  op.left_op_->Accept(*this);
  op.right_op_->Accept(*this);
  return true;
}

PRE_VISIT(EmptyResult)
PRE_VISIT(Produce)
PRE_VISIT(Accumulate)
PRE_VISIT(Aggregate)
PRE_VISIT(Skip)
PRE_VISIT(Limit)
PRE_VISIT(OrderBy)
PRE_VISIT(Distinct)
PRE_VISIT(PeriodicCommit)

bool UsedIndexChecker::PreVisit(Union &op) {
  op.left_op_->Accept(*this);
  op.right_op_->Accept(*this);
  return true;
}

PRE_VISIT(Unwind)

bool UsedIndexChecker::PreVisit(CallProcedure &op) {
  if (op.is_write_) {
    return true;
  }
  return true;
}

bool UsedIndexChecker::PreVisit([[maybe_unused]] Foreach &op) { return true; }

bool UsedIndexChecker::PreVisit(Apply &op) {
  op.input_->Accept(*this);
  op.subquery_->Accept(*this);
  return true;
}

bool UsedIndexChecker::PreVisit(IndexedJoin &op) {
  op.main_branch_->Accept(*this);
  op.sub_branch_->Accept(*this);
  return true;
}

bool UsedIndexChecker::PreVisit(HashJoin &op) {
  op.left_op_->Accept(*this);
  op.right_op_->Accept(*this);
  return true;
}

bool UsedIndexChecker::PreVisit(PeriodicSubquery &op) {
  op.input_->Accept(*this);
  op.subquery_->Accept(*this);
  return true;
}

bool UsedIndexChecker::PreVisit(RollUpApply &op) {
  op.input_->Accept(*this);
  op.list_collection_branch_->Accept(*this);
  return true;
}

#undef PRE_VISIT

bool UsedIndexChecker::Visit(Once &) { return true; }  // NOLINT(hicpp-named-parameter)

}  // namespace memgraph::query::plan
