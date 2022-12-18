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
  std::vector<Operator> ops;
  ops.emplace_back(Operator{.name = "Produce", .state = Produce{}});
  ops.emplace_back(Operator{.name = "ScanAll", .state = ScanAll{}});
  ops.emplace_back(Operator{.name = "ScanAll", .state = ScanAll{}});
  ops.emplace_back(Operator{.name = "Once", .state = Once{}});
  for (auto &op : ops) {
    auto status = Call(op.state);
    SPDLOG_INFO("name: {} has_more: {}", op.name, status.has_more);
    while (status.has_more) {
      status = Call(op.state);
      SPDLOG_INFO("name: {} has_more: {}", op.name, status.has_more);
    }
  }

  /// ASYNC
  std::vector<Operator> ops_async;
  ops_async.emplace_back(Operator{.name = "Once", .state = Once{}});
  ops_async.emplace_back(Operator{.name = "ScanAll", .state = ScanAll{}});
  memgraph::utils::ThreadPool thread_pool{8};
  mock::ExecutionContext ctx{.thread_pool = &thread_pool};
  for (auto &op : ops_async) {
    // TODO(gitbuda): This is not correct, the point it so illustrate the concept (op.state) is moved!
    auto notifier = []() { SPDLOG_INFO("op done"); };
    auto future = CallAsync(ctx, std::move(op.state), notifier);
    auto execution = std::move(future).Wait();
    SPDLOG_INFO("name: {} has_more: {}", op.name, execution.status.has_more);
    ;
    while (execution.status.has_more) {
      auto future = CallAsync(ctx, std::move(execution.state), notifier);
      execution = std::move(future).Wait();
      SPDLOG_INFO("name: {} has_more: {}", op.name, execution.status.has_more);
      ;
    }
  }

  return 0;
}
