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

  bool PreVisit(Filter &) override { return true; }
  bool PostVisit(Filter &) override { return true; }

  bool PreVisit(ScanAll &op) override {
    std::cout << "PreVisit ScanAll, output " << op.output_symbol_.name_ << std::endl;
    return true;
  }
  bool PostVisit(ScanAll &) override { return true; }

  bool PreVisit(Expand &) override { return true; }
  bool PostVisit(Expand &) override { return true; }

  bool PreVisit(ExpandVariable &) override { return true; }
  bool PostVisit(ExpandVariable &) override { return true; }

  bool PreVisit(Merge &) override { return false; }
  bool PostVisit(Merge &) override { return true; }

  bool PreVisit(Optional &) override { return true; }
  bool PostVisit(Optional &) override { return true; }

  bool PreVisit(Cartesian &) override { return true; }
  bool PostVisit(Cartesian &) override { return true; }

  bool PreVisit(Union &) override { return true; }
  bool PostVisit(Union &) override { return true; }

  bool PreVisit(CreateNode &) override { return true; }
  bool PostVisit(CreateNode &) override { return true; }

  bool PreVisit(CreateExpand &) override { return true; }
  bool PostVisit(CreateExpand &) override { return true; }

  bool PreVisit(ScanAllByLabel &) override { return true; }
  bool PostVisit(ScanAllByLabel &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyRange &) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyRange &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyValue &) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyValue &) override { return true; }

  bool PreVisit(ScanAllByLabelProperty &) override { return true; }
  bool PostVisit(ScanAllByLabelProperty &) override { return true; }

  bool PreVisit(ScanAllById &) override { return true; }
  bool PostVisit(ScanAllById &) override { return true; }

  bool PreVisit(ConstructNamedPath &) override { return true; }
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

  bool PreVisit(Delete &) override { return true; }
  bool PostVisit(Delete &) override { return true; }

  bool PreVisit(SetProperty &) override { return true; }
  bool PostVisit(SetProperty &) override { return true; }

  bool PreVisit(SetProperties &) override { return true; }
  bool PostVisit(SetProperties &) override { return true; }

  bool PreVisit(SetLabels &) override { return true; }
  bool PostVisit(SetLabels &) override { return true; }

  bool PreVisit(RemoveProperty &) override { return true; }
  bool PostVisit(RemoveProperty &) override { return true; }

  bool PreVisit(RemoveLabels &) override { return true; }
  bool PostVisit(RemoveLabels &) override { return true; }

  bool PreVisit(EdgeUniquenessFilter &) override { return true; }
  bool PostVisit(EdgeUniquenessFilter &) override { return true; }

  bool PreVisit(Accumulate &) override { return true; }
  bool PostVisit(Accumulate &) override { return true; }

  bool PreVisit(Aggregate &) override { return true; }
  bool PostVisit(Aggregate &) override { return true; }

  bool PreVisit(Skip &) override { return true; }
  bool PostVisit(Skip &) override { return true; }

  bool PreVisit(Limit &) override { return true; }
  bool PostVisit(Limit &) override { return true; }

  bool PreVisit(OrderBy &) override { return true; }
  bool PostVisit(OrderBy &) override { return true; }

  bool PreVisit(Unwind &) override { return true; }
  bool PostVisit(Unwind &) override { return true; }

  bool PreVisit(Distinct &) override { return true; }
  bool PostVisit(Distinct &) override { return true; }

  bool PreVisit(CallProcedure &) override { return true; }
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
