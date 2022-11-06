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
#include "query/v2/physical/physical.hpp"
#include "storage/v2/storage.hpp"
#include "utils/string.hpp"

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
  if (plans.empty()) {
    throw memgraph::utils::BasicException("No plans");
  }

  // Example of the physical plan execution.
  memgraph::query::v2::physical::PhysicalPlanGenerator physical_plan_generator;
  // Generate physical plan.
  plans[0].unoptimized_plan->Accept(physical_plan_generator);
  auto physical_plan = physical_plan_generator.Generate();
  // Start physical plan execution.
  memgraph::query::v2::physical::ExecutionContext ctx;
  physical_plan->Execute(ctx);
  // Fetch physical plan execution results.
  auto data = physical_plan->Next();
  MG_ASSERT(data->Size() == 0);

  return 0;
}
