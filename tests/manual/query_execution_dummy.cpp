// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "interactive/planning.hpp"

#include <chrono>
#include <thread>

#include <gflags/gflags.h>

#include "interactive/db_accessor.hpp"
#include "interactive/plan.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "storage/v2/storage.hpp"
#include "utils/string.hpp"

namespace memgraph::query::plan {

class TestLogicalOperatorVisitor final : public HierarchicalLogicalOperatorVisitor {
 public:
  TestLogicalOperatorVisitor() {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  void Start() {}

  bool IsDone() { return true; }

  bool Visit(Once &) override {
    std::cout << "Visit Once" << std::endl;
    return true;
  }

  bool PreVisit(Filter &op) override { return true; }
  bool PostVisit(Filter &op) override { return true; }

  bool PreVisit(ScanAll &op) override {
    std::cout << "PreVisit ScanAll, output " << op.output_symbol_.name_ << std::endl;
    return true;
  }
  bool PostVisit(ScanAll &scan) override { return true; }

  bool PreVisit(Expand &op) override { return true; }
  bool PostVisit(Expand &expand) override { return true; }

  bool PreVisit(ExpandVariable &op) override { return true; }
  bool PostVisit(ExpandVariable &expand) override { return true; }

  bool PreVisit(Merge &op) override { return false; }
  bool PostVisit(Merge &) override { return true; }

  bool PreVisit(Optional &op) override { return false; }
  bool PostVisit(Optional &) override { return true; }

  bool PreVisit(Cartesian &op) override { return true; }
  bool PostVisit(Cartesian &) override { return true; }

  bool PreVisit(Union &op) override { return false; }
  bool PostVisit(Union &) override { return true; }

  bool PreVisit(CreateNode &op) override { return true; }
  bool PostVisit(CreateNode &) override { return true; }

  bool PreVisit(CreateExpand &op) override { return true; }
  bool PostVisit(CreateExpand &) override { return true; }

  bool PreVisit(ScanAllByLabel &op) override { return true; }
  bool PostVisit(ScanAllByLabel &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyRange &op) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyRange &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyValue &op) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyValue &) override { return true; }

  bool PreVisit(ScanAllByLabelProperty &op) override { return true; }
  bool PostVisit(ScanAllByLabelProperty &) override { return true; }

  bool PreVisit(ScanAllById &op) override { return true; }
  bool PostVisit(ScanAllById &) override { return true; }

  bool PreVisit(ConstructNamedPath &op) override { return true; }
  bool PostVisit(ConstructNamedPath &) override { return true; }

  bool PreVisit(Produce &op) override {
    std::cout << "PreVisit Produce, named expressions: ";
    for (const auto &name_expr : op.named_expressions_) {
      std::cout << name_expr->name_ << " ";
    }
    std::cout << std::endl;
    return true;
  }
  bool PostVisit(Produce &) override { return true; }

  bool PreVisit(Delete &op) override { return true; }
  bool PostVisit(Delete &) override { return true; }

  bool PreVisit(SetProperty &op) override { return true; }
  bool PostVisit(SetProperty &) override { return true; }

  bool PreVisit(SetProperties &op) override { return true; }
  bool PostVisit(SetProperties &) override { return true; }

  bool PreVisit(SetLabels &op) override { return true; }
  bool PostVisit(SetLabels &) override { return true; }

  bool PreVisit(RemoveProperty &op) override { return true; }
  bool PostVisit(RemoveProperty &) override { return true; }

  bool PreVisit(RemoveLabels &op) override { return true; }
  bool PostVisit(RemoveLabels &) override { return true; }

  bool PreVisit(EdgeUniquenessFilter &op) override { return true; }
  bool PostVisit(EdgeUniquenessFilter &) override { return true; }

  bool PreVisit(Accumulate &op) override { return true; }
  bool PostVisit(Accumulate &) override { return true; }

  bool PreVisit(Aggregate &op) override { return true; }
  bool PostVisit(Aggregate &) override { return true; }

  bool PreVisit(Skip &op) override { return true; }
  bool PostVisit(Skip &) override { return true; }

  bool PreVisit(Limit &op) override { return true; }
  bool PostVisit(Limit &) override { return true; }

  bool PreVisit(OrderBy &op) override { return true; }
  bool PostVisit(OrderBy &) override { return true; }

  bool PreVisit(Unwind &op) override { return true; }
  bool PostVisit(Unwind &) override { return true; }

  bool PreVisit(Distinct &op) override { return true; }
  bool PostVisit(Distinct &) override { return true; }

  bool PreVisit(CallProcedure &op) override { return true; }
  bool PostVisit(CallProcedure &) override { return true; }
};
}  // namespace memgraph::query::plan

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::info);

  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  Timer planning_timer;
  InteractiveDbAccessor interactive_db(&dba, 10, planning_timer);
  std::string input_query = "MATCH (n) RETURN n;";
  memgraph::query::AstStorage ast;
  auto *query = dynamic_cast<memgraph::query::CypherQuery *>(MakeAst(input_query, &ast));
  if (!query) {
    throw memgraph::utils::BasicException("Create CypherQuery failed");
  }
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  planning_timer.Start();
  auto plans = MakeLogicalPlans(query, ast, symbol_table, &interactive_db);
  if (plans.size() == 0) {
    throw memgraph::utils::BasicException("No plans");
  }

  memgraph::query::plan::TestLogicalOperatorVisitor executor;
  plans[0].unoptimized_plan->Accept(executor);
  executor.Start();
  while (!executor.IsDone()) {
    std::cout << "Executor NOT done yet" << std::endl;
  }
  std::cout << "Executor done" << std::endl;
  return 0;
}
