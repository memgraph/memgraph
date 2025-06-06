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

/// @file
#pragma once

#include <iosfwd>

#include <nlohmann/json.hpp>

#include "query/plan/operator.hpp"

namespace memgraph::query {
class DbAccessor;

namespace plan {

class LogicalOperator;

/// Pretty print a `LogicalOperator` plan to a `std::ostream`.
/// DbAccessor is needed for resolving label and property names.
/// Note that `plan_root` isn't modified, but we can't take it as a const
/// because we don't have support for visiting a const LogicalOperator.
void PrettyPrint(const DbAccessor &dba, const LogicalOperator *plan_root, std::ostream *out);

/// Overload of `PrettyPrint` which defaults the `std::ostream` to `std::cout`.
inline void PrettyPrint(const DbAccessor &dba, const LogicalOperator *plan_root) {
  PrettyPrint(dba, plan_root, &std::cout);
}

/// Convert a `LogicalOperator` plan to a JSON representation.
/// DbAccessor is needed for resolving label and property names.
nlohmann::json PlanToJson(const DbAccessor &dba, const LogicalOperator *plan_root);

class PlanPrinter : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  PlanPrinter(const DbAccessor *dba, std::ostream *out);

  bool DefaultPreVisit() override;

  bool PreVisit(CreateNode &) override;
  bool PreVisit(CreateExpand &) override;
  bool PreVisit(Delete &) override;

  bool PreVisit(SetProperty &) override;
  bool PreVisit(SetProperties &) override;
  bool PreVisit(SetLabels &) override;

  bool PreVisit(RemoveProperty &) override;
  bool PreVisit(RemoveLabels &) override;

  bool PreVisit(ScanAll &) override;
  bool PreVisit(ScanAllByLabel &) override;
  bool PreVisit(ScanAllByLabelProperties &) override;
  bool PreVisit(ScanAllById &) override;
  bool PreVisit(ScanAllByEdge &) override;
  bool PreVisit(ScanAllByEdgeType &) override;
  bool PreVisit(ScanAllByEdgeTypeProperty &) override;
  bool PreVisit(ScanAllByEdgeTypePropertyValue &) override;
  bool PreVisit(ScanAllByEdgeTypePropertyRange &) override;
  bool PreVisit(ScanAllByEdgeProperty &) override;
  bool PreVisit(ScanAllByEdgePropertyValue &) override;
  bool PreVisit(ScanAllByEdgePropertyRange &) override;
  bool PreVisit(ScanAllByPointDistance &) override;
  bool PreVisit(ScanAllByPointWithinbbox &) override;
  bool PreVisit(ScanAllByEdgeId &) override;

  bool PreVisit(Expand &) override;
  bool PreVisit(ExpandVariable &) override;

  bool PreVisit(ConstructNamedPath &) override;

  bool PreVisit(Filter &) override;
  bool PreVisit(EvaluatePatternFilter & /*unused*/) override;
  bool PreVisit(EdgeUniquenessFilter &) override;

  bool PreVisit(Merge &) override;
  bool PreVisit(Optional &) override;
  bool PreVisit(Cartesian &) override;
  bool PreVisit(HashJoin &) override;

  bool PreVisit(EmptyResult &) override;
  bool PreVisit(Produce &) override;
  bool PreVisit(Accumulate &) override;
  bool PreVisit(Aggregate &) override;
  bool PreVisit(Skip &) override;
  bool PreVisit(Limit &) override;
  bool PreVisit(OrderBy &) override;
  bool PreVisit(Distinct &) override;
  bool PreVisit(Union &) override;
  bool PreVisit(RollUpApply &) override;
  bool PreVisit(PeriodicCommit &) override;
  bool PreVisit(PeriodicSubquery &) override;

  bool PreVisit(Unwind &) override;
  bool PreVisit(CallProcedure &) override;
  bool PreVisit(LoadCsv &) override;
  bool PreVisit(Foreach &) override;
  bool PreVisit(Apply & /*unused*/) override;
  bool PreVisit(IndexedJoin & /*unused*/) override;

  bool Visit(Once &) override;

  /// Call fun with output stream. The stream is prefixed with amount of spaces
  /// corresponding to the current depth_.
  template <class TFun>
  void WithPrintLn(TFun fun) {
    *out_ << " ";
    for (int64_t i = 0; i < depth_; ++i) {
      *out_ << "| ";
    }
    fun(*out_);
    *out_ << std::endl;
  }

  /// Forward this printer to another operator branch by incrementing the depth
  /// and printing the branch name.
  void Branch(LogicalOperator &op, const std::string &branch_name = "");

  int64_t depth_{0};
  const DbAccessor *dba_{nullptr};
  std::ostream *out_{nullptr};
};

namespace impl {

std::string ToString(EdgeAtom::Direction dir);

std::string ToString(EdgeAtom::Type type);

std::string ToString(Ordering ord);

nlohmann::json ToJson(Expression *expression, const DbAccessor &dba);

nlohmann::json ToJson(const utils::Bound<Expression *> &bound, const DbAccessor &dba);

nlohmann::json ToJson(const Symbol &symbol);

nlohmann::json ToJson(storage::EdgeTypeId edge_type, const DbAccessor &dba);

nlohmann::json ToJson(storage::LabelId label, const DbAccessor &dba);

nlohmann::json ToJson(storage::PropertyId property, const DbAccessor &dba);

nlohmann::json ToJson(storage::PropertyPath path, const DbAccessor &dba);

nlohmann::json ToJson(NamedExpression *nexpr, const DbAccessor &dba);

nlohmann::json ToJson(const std::vector<std::pair<storage::PropertyId, Expression *>> &properties,
                      const DbAccessor &dba);

nlohmann::json ToJson(const NodeCreationInfo &node_info, const DbAccessor &dba);

nlohmann::json ToJson(const EdgeCreationInfo &edge_info, const DbAccessor &dba);

nlohmann::json ToJson(const Aggregate::Element &elem, const DbAccessor &dba);

nlohmann::json ToJson(const ExpressionRange &expression_range, const DbAccessor &dba);

template <class T, class... Args>
nlohmann::json ToJson(const std::vector<T> &items, Args &&...args) {
  nlohmann::json json;
  for (const auto &item : items) {
    json.emplace_back(ToJson(item, std::forward<Args>(args)...));
  }
  return json;
}

class PlanToJsonVisitor : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  explicit PlanToJsonVisitor(const DbAccessor *dba) : dba_(dba) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(CreateNode &) override;
  bool PreVisit(CreateExpand &) override;
  bool PreVisit(Delete &) override;

  bool PreVisit(SetProperty &) override;
  bool PreVisit(SetProperties &) override;
  bool PreVisit(SetLabels &) override;

  bool PreVisit(RemoveProperty &) override;
  bool PreVisit(RemoveLabels &) override;

  bool PreVisit(Expand &) override;
  bool PreVisit(ExpandVariable &) override;

  bool PreVisit(ConstructNamedPath &) override;

  bool PreVisit(Merge &) override;
  bool PreVisit(Optional &) override;

  bool PreVisit(Filter &) override;
  bool PreVisit(EvaluatePatternFilter & /*op*/) override;
  bool PreVisit(EdgeUniquenessFilter &) override;
  bool PreVisit(Cartesian &) override;
  bool PreVisit(Apply & /*unused*/) override;
  bool PreVisit(HashJoin &) override;
  bool PreVisit(IndexedJoin & /*unused*/) override;

  bool PreVisit(ScanAll &) override;
  bool PreVisit(ScanAllByLabel &) override;
  bool PreVisit(ScanAllByLabelProperties &) override;
  bool PreVisit(ScanAllById &) override;

  bool PreVisit(ScanAllByEdge &) override;
  bool PreVisit(ScanAllByEdgeType &) override;
  bool PreVisit(ScanAllByEdgeTypeProperty &) override;
  bool PreVisit(ScanAllByEdgeTypePropertyValue &) override;
  bool PreVisit(ScanAllByEdgeTypePropertyRange &) override;
  bool PreVisit(ScanAllByEdgeProperty &) override;
  bool PreVisit(ScanAllByEdgePropertyValue &) override;
  bool PreVisit(ScanAllByEdgePropertyRange &) override;
  bool PreVisit(ScanAllByEdgeId &) override;

  bool PreVisit(EmptyResult &) override;
  bool PreVisit(Produce &) override;
  bool PreVisit(Accumulate &) override;
  bool PreVisit(Aggregate &) override;
  bool PreVisit(Skip &) override;
  bool PreVisit(Limit &) override;
  bool PreVisit(OrderBy &) override;
  bool PreVisit(Distinct &) override;
  bool PreVisit(Union &) override;

  bool PreVisit(Unwind &) override;
  bool PreVisit(Foreach &) override;
  bool PreVisit(CallProcedure &) override;
  bool PreVisit(LoadCsv &) override;
  bool PreVisit(RollUpApply &) override;
  bool PreVisit(PeriodicCommit &) override;
  bool PreVisit(PeriodicSubquery &) override;

  bool Visit(Once &) override;

  nlohmann::json output() { return output_; }

 protected:
  nlohmann::json output_;
  const DbAccessor *dba_;

  nlohmann::json PopOutput() {
    nlohmann::json tmp;
    tmp.swap(output_);
    return tmp;
  }
};

}  // namespace impl

}  // namespace plan
}  // namespace memgraph::query
