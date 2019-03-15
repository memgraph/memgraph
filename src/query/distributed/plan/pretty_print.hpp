/// @file
#pragma once

#include "query/distributed/plan/ops.hpp"
#include "query/plan/pretty_print.hpp"

#include <json/json.hpp>

namespace query::plan {

void DistributedPrettyPrint(const database::GraphDbAccessor &dba,
                            const LogicalOperator *plan_root,
                            std::ostream *out);

inline void DistributedPrettyPrint(const database::GraphDbAccessor &dba,
                                   const LogicalOperator *plan_root) {
  DistributedPrettyPrint(dba, plan_root, &std::cout);
}

nlohmann::json DistributedPlanToJson(const database::GraphDbAccessor &dba,
                                     const LogicalOperator *plan_root);

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

namespace impl {

class DistributedPlanToJsonVisitor : public PlanToJsonVisitor,
                                     public DistributedOperatorVisitor {
 public:
  using DistributedOperatorVisitor::PostVisit;
  using DistributedOperatorVisitor::PreVisit;
  using DistributedOperatorVisitor::Visit;
  using PlanToJsonVisitor::PlanToJsonVisitor;
  using PlanToJsonVisitor::PostVisit;
  using PlanToJsonVisitor::PreVisit;
  using PlanToJsonVisitor::Visit;

  bool PreVisit(DistributedExpand &) override;
  bool PreVisit(DistributedExpandBfs &) override;

  bool PreVisit(PullRemote &) override;
  bool PreVisit(PullRemoteOrderBy &) override;

  bool PreVisit(DistributedCreateNode &) override;
  bool PreVisit(DistributedCreateExpand &) override;
  bool PreVisit(Synchronize &) override;
};

}  // namespace impl

}  // namespace query::plan
