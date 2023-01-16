// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>
#include <variant>
#include <vector>

#include "io/future.hpp"
#include "query/v2/physical/execution.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "utils/logging.hpp"

using namespace memgraph::query::v2::physical;
using namespace memgraph::query::v2::physical::execution;

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::info);

  /// SYNC
  std::vector<DataOperator> ops;
  ops.emplace_back(DataOperator{.name = "Produce", .execution = Execution{.state = Produce{}}});
  ops.emplace_back(DataOperator{.name = "ScanAll", .execution = Execution{.state = ScanAll{}}});
  ops.emplace_back(DataOperator{.name = "ScanAll", .execution = Execution{.state = ScanAll{}}});
  ops.emplace_back(DataOperator{.name = "Once", .execution = Execution{.state = Once{}}});
  for (auto &op : ops) {
    auto status = Call(op.execution.state);
    SPDLOG_INFO("name: {} has_more: {}", op.name, status.has_more);
    while (status.has_more) {
      status = Call(op.execution.state);
      SPDLOG_INFO("name: {} has_more: {}", op.name, status.has_more);
    }
  }

  /// ASYNC
  std::vector<DataOperator> ops_async;
  ops_async.emplace_back(DataOperator{.name = "Once", .execution = Execution{.state = Once{}}});
  ops_async.emplace_back(DataOperator{.name = "ScanAll", .execution = Execution{.state = ScanAll{}}});
  memgraph::utils::ThreadPool thread_pool{16};
  mock::ExecutionContext ctx{.thread_pool = &thread_pool};
  for (auto &op : ops_async) {
    auto notifier = []() {};
    auto future = CallAsync(ctx, op.execution.state, notifier);
    auto status = std::move(future).Wait();
    SPDLOG_INFO("name: {} has_more: {}", op.name, status.has_more);
    ;
    while (status.has_more) {
      auto future = CallAsync(ctx, op.execution.state, notifier);
      status = std::move(future).Wait();
      SPDLOG_INFO("name: {} has_more: {}", op.name, status.has_more);
      ;
    }
  }

  return 0;
}
