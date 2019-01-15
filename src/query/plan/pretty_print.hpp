/// @file
#pragma once

#include <iostream>

#include "query/plan/operator.hpp"

namespace database {
class GraphDbAccessor;
}

namespace query::plan {

class LogicalOperator;

/// Pretty print a `LogicalOperator` plan to a `std::ostream`.
/// GraphDbAccessor is needed for resolving label and property names.
/// Note that `plan_root` isn't modified, but we can't take it as a const
/// because we don't have support for visiting a const LogicalOperator.
void PrettyPrint(const database::GraphDbAccessor &dba,
                 const LogicalOperator *plan_root, std::ostream *out);

/// Overload of `PrettyPrint` which defaults the `std::ostream` to `std::cout`.
inline void PrettyPrint(const database::GraphDbAccessor &dba,
                        const LogicalOperator *plan_root) {
  PrettyPrint(dba, plan_root, &std::cout);
}

class PlanPrinter : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  PlanPrinter(const database::GraphDbAccessor *dba, std::ostream *out);

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
  bool PreVisit(ScanAllByLabelPropertyValue &) override;
  bool PreVisit(ScanAllByLabelPropertyRange &) override;

  bool PreVisit(Expand &) override;
  bool PreVisit(ExpandVariable &) override;

  bool PreVisit(ConstructNamedPath &) override;

  bool PreVisit(Filter &) override;
  bool PreVisit(EdgeUniquenessFilter &) override;

  bool PreVisit(Merge &) override;
  bool PreVisit(Optional &) override;
  bool PreVisit(Cartesian &) override;

  bool PreVisit(Produce &) override;
  bool PreVisit(Accumulate &) override;
  bool PreVisit(Aggregate &) override;
  bool PreVisit(Skip &) override;
  bool PreVisit(Limit &) override;
  bool PreVisit(OrderBy &) override;
  bool PreVisit(Distinct &) override;
  bool PreVisit(Union &) override;

  bool PreVisit(Unwind &) override;

  bool Visit(Once &) override;

  /// Call fun with output stream. The stream is prefixed with amount of spaces
  /// corresponding to the current depth_.
  template <class TFun>
  void WithPrintLn(TFun fun) {
    *out_ << " ";
    for (int i = 0; i < depth_; ++i) {
      *out_ << "| ";
    }
    fun(*out_);
    *out_ << std::endl;
  }

  /// Forward this printer to another operator branch by incrementing the depth
  /// and printing the branch name.
  void Branch(LogicalOperator &op, const std::string &branch_name = "");

  int64_t depth_{0};
  const database::GraphDbAccessor *dba_{nullptr};
  std::ostream *out_{nullptr};
};

}  // namespace query::plan
