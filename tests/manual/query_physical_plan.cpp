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
#include "query/v2/physical/execution.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/mock/mock.hpp"
#include "query/v2/physical/physical_ene.hpp"
#include "storage/v2/storage.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::debug);

  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  Timer timer;
  InteractiveDbAccessor interactive_db(&dba, 10, timer);
  std::string input_query = "MATCH (n) RETURN n;";
  memgraph::query::AstStorage ast;
  auto *query = dynamic_cast<memgraph::query::CypherQuery *>(MakeAst(input_query, &ast));
  if (!query) {
    throw memgraph::utils::BasicException("Create CypherQuery failed");
  }
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plans = MakeLogicalPlans(query, ast, symbol_table, &interactive_db);
  if (plans.empty()) {
    throw memgraph::utils::BasicException("No plans");
  }

  // Example of the physical plan execution.
  using TDataPool = memgraph::query::v2::physical::multiframe::MPMCMultiframeFCFSPool;
  memgraph::query::v2::physical::PhysicalPlanGenerator<TDataPool> physical_plan_generator;
  // Generate physical plan.
  plans[0].unoptimized_plan->Accept(physical_plan_generator);
  auto physical_plan = physical_plan_generator.Generate();
  // Start physical plan execution.
  memgraph::utils::ThreadPool thread_pool{16};
  memgraph::query::v2::physical::mock::ExecutionContext ctx{.thread_pool = &thread_pool};
  physical_plan->Execute(ctx);
  // Fetch physical plan execution results.
  auto token = physical_plan->NextRead();
  if (token) {
    SPDLOG_TRACE("Produce token is some");
  } else {
    SPDLOG_TRACE("Produce token is none");
  }

  using Op = memgraph::query::v2::physical::mock::Op;
  using OpType = memgraph::query::v2::physical::mock::OpType;
  std::vector<int> pool_sizes = {1, 2, 4, 8, 16};
  std::vector<int> mf_sizes = {1, 10, 100, 1000, 10000};
  int scan_all_elems = 10;
  std::vector<Op> ops{
      Op{.type = OpType::Produce},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::Once},
  };

  SPDLOG_INFO("---- Single Frame Pull Execution ----");
  memgraph::query::v2::physical::mock::Frame frame;
  auto plan = memgraph::query::v2::physical::mock::MakePullPlan(ops);
  timer.Start();
  int64_t cnt{0};
  while (plan->Pull(frame, ctx)) {
    cnt++;
  }
  auto time = std::chrono::duration_cast<std::chrono::milliseconds>(timer.Elapsed()).count();
  std::cout << "pull done " << cnt << " cnt "
            << "in " << time << std::endl;

  SPDLOG_INFO("---- Parallelized Execution Single Thread per Operator with constant loops ----");
  for (const auto &pool_size : pool_sizes) {
    for (const auto &mf_size : mf_sizes) {
      // TODO(gitbuda): MakePlan is allocating space for data pools -> measure the overhead.
      auto plan = MakeENEPlan(ops, pool_size, mf_size);
      timer.Start();
      plan->Execute(ctx);
      auto time = std::chrono::duration_cast<std::chrono::milliseconds>(timer.Elapsed()).count();
      std::cout << pool_size << " " << mf_size << " " << time << std::endl;
    }
  }

  SPDLOG_INFO("---- Parallelized Execution Single Thread per Operator with async await ----");
  for (const auto &pool_size : pool_sizes) {
    for (const auto &mf_size : mf_sizes) {
      auto async_plan = memgraph::query::v2::physical::mock::MakeAsyncPlan(ops, pool_size, mf_size);
      memgraph::query::v2::physical::execution::Executor executor(16);
      timer.Start();
      auto tuples_no = executor.Execute(async_plan);
      MG_ASSERT(tuples_no == 100000, "Wrong number of results");
      auto time = std::chrono::duration_cast<std::chrono::milliseconds>(timer.Elapsed()).count();
      std::cout << pool_size << " " << mf_size << " " << time << std::endl;
    }
  }

  return 0;
}
