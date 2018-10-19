/// @file
#pragma once

#include "query/plan/distributed_ops.hpp"
#include "query/plan/pretty_print.hpp"

namespace query::plan {

void DistributedPrettyPrint(const database::GraphDbAccessor &dba,
                            const LogicalOperator *plan_root,
                            std::ostream *out);

inline void DistributedPrettyPrint(const database::GraphDbAccessor &dba,
                                   const LogicalOperator *plan_root) {
  DistributedPrettyPrint(dba, plan_root, &std::cout);
}

class DistributedPlanPrinter : public PlanPrinter,
                               public DistributedOperatorVisitor {
 public:
  using DistributedOperatorVisitor::PostVisit;
  using DistributedOperatorVisitor::PreVisit;
  using DistributedOperatorVisitor::Visit;
  using PlanPrinter::PlanPrinter;
  using PlanPrinter::PostVisit;
  using PlanPrinter::PreVisit;
  using PlanPrinter::Visit;

  bool PreVisit(DistributedExpand &) override;
  bool PreVisit(DistributedExpandBfs &) override;

  bool PreVisit(PullRemote &) override;
  bool PreVisit(PullRemoteOrderBy &) override;

  bool PreVisit(DistributedCreateNode &) override;
  bool PreVisit(DistributedCreateExpand &) override;
  bool PreVisit(Synchronize &) override;
};

}  // namespace query::plan
