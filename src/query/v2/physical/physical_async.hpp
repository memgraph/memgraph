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

#pragma once

///
/// Physical Execute/Next/Emit Async Architecture Implementation
///
/// The whole new set of possibilities!
///
/// Since most of the operators have complex internal state, each Execute
/// function should be implemented in a way so that single threaded execution
/// of the whole query is possible via SingleThreadedExecutor. With the right
/// implementation, it should also be possible to parallelize execution of
/// stateless operators and simpler statefull operators like ScanAll by using
/// the same Execute implementation and MultiThreadedExecutor.
///
/// Blocking, but time and space limited, implementations of Execute functions,
/// should be wrapped into ExecuteAsync to allow efficient multi-threaded
/// execution.
///

#include <future>
#include <variant>

#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::physical {

struct NonCopyable {
  NonCopyable() = default;
  NonCopyable(const NonCopyable &) = delete;
  NonCopyable &operator=(const NonCopyable &) = delete;
  NonCopyable(NonCopyable &&) noexcept = default;
  NonCopyable &operator=(NonCopyable &&) noexcept = default;
  ~NonCopyable() = default;
};

struct Operator;

struct Cursor {
  /// Operator Part
  Operator *op;                      // access to the write data pool and other op data
  std::vector<Operator *> children;  // access to the read data pool
  std::vector<Operator *> upstream;  // access to the read data pool

  /// Internal Part
  // internal state in the derived struct
};
struct Once : public Cursor {
  bool has_more{true};
};
struct ScanAll : public Cursor {
  int cnt{0};
};
struct Produce : public Cursor {};
using ExecuteState = std::variant<Once, ScanAll, Produce>;

/// NOTE: In theory status and state could be coupled together.

struct ExecuteStatus {
  bool has_more;
  std::optional<std::string> error;
};

struct Operator {
  std::string name;
  std::vector<std::unique_ptr<Operator>> children;
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool;
  ExecuteState state;
};

ExecuteStatus Execute(Once &state) {
  state.has_more = false;
  return ExecuteStatus{.has_more = false};
}

ExecuteStatus Execute(ScanAll &state) {
  state.cnt++;
  if (state.cnt >= 10) {
    return ExecuteStatus{.has_more = false};
  }
  return ExecuteStatus{.has_more = true};
}

ExecuteStatus Execute(Produce &) { return ExecuteStatus{.has_more = false}; }

// TODO(gitbuda): Macro would be much better here.

std::future<std::pair<ExecuteStatus, ExecuteState>> ExecuteAsync(mock::ExecutionContext &ctx, Once &&state) {
  std::promise<std::pair<ExecuteStatus, ExecuteState>> promise;
  auto future = promise.get_future();
  ctx.thread_pool->AddTask(
      [state = std::move(state), promise = std::make_shared<decltype(promise)>(std::move(promise))]() mutable {
        promise->set_value({Execute(state), std::move(state)});
      });
  return future;
}

std::future<std::pair<ExecuteStatus, ExecuteState>> ExecuteAsync(mock::ExecutionContext &ctx, ScanAll &&state) {
  std::promise<std::pair<ExecuteStatus, ExecuteState>> promise;
  auto future = promise.get_future();
  ctx.thread_pool->AddTask(
      [state = std::move(state), promise = std::make_shared<decltype(promise)>(std::move(promise))]() mutable {
        promise->set_value({Execute(state), std::move(state)});
      });
  return future;
}

std::future<std::pair<ExecuteStatus, ExecuteState>> ExecuteAsync(mock::ExecutionContext &ctx, Produce &&state) {
  std::promise<std::pair<ExecuteStatus, ExecuteState>> promise;
  auto future = promise.get_future();
  ctx.thread_pool->AddTask(
      [state = std::move(state), promise = std::make_shared<decltype(promise)>(std::move(promise))]() mutable {
        promise->set_value({Execute(state), std::move(state)});
      });
  return future;
}

}  // namespace memgraph::query::v2::physical
