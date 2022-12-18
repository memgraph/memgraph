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

#include <variant>

#include "io/future.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::physical::execution {

struct NonCopyable {
  NonCopyable() = default;
  NonCopyable(const NonCopyable &) = delete;
  NonCopyable &operator=(const NonCopyable &) = delete;
  NonCopyable(NonCopyable &&) noexcept = default;
  NonCopyable &operator=(NonCopyable &&) noexcept = default;
  ~NonCopyable() = default;
};

struct Operator;

struct Status {
  bool has_more;
  std::optional<std::string> error;
};
/// NOTE: In theory status and state could be coupled together, but since STATE
/// is a variant it's easier to access STATUS via generic object.

struct Once {
  Operator *op;
  bool has_more{true};
};
struct ScanAll {
  Operator *op;
  std::vector<Operator *> children;
};
struct Produce {
  Operator *op;
  std::vector<Operator *> children;
};
using VarState = std::variant<Once, ScanAll, Produce>;

struct Execution {
  Status status;
  VarState state;
};

struct Operator {
  std::string name;
  std::vector<std::shared_ptr<Operator>> children;
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool;
  VarState state;
};

/// SINGLE THREADED EXECUTE IMPLEMENTATIONS

inline Status Execute(Once & /*unused*/) { return Status{.has_more = false}; }

inline Status Execute(ScanAll & /*unused*/) { return Status{.has_more = false}; }

inline Status Execute(Produce & /*unused*/) { return Status{.has_more = false}; }

/// ASYNC EXECUTE WRAPPERS

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_EXECUTE_ASYNC(state_type)                                                                            \
  inline io::Future<Execution> ExecuteAsync(                                                                        \
      mock::ExecutionContext &ctx, std::function<void()> notifier, /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
      state_type &&state) {                                                                                         \
    auto [future, promise] = io::FuturePromisePairWithNotifications<Execution>(nullptr, std::move(notifier));       \
    auto shared_promise = std::make_shared<decltype(promise)>(std::move(promise));                                  \
    ctx.thread_pool->AddTask([state = std::move(state), promise = std::move(shared_promise)]() mutable {            \
      promise->Fill({.status = Execute(state), .state = std::move(state)});                                         \
    });                                                                                                             \
    return std::move(future);                                                                                       \
  }

DEFINE_EXECUTE_ASYNC(Once)
DEFINE_EXECUTE_ASYNC(ScanAll)
DEFINE_EXECUTE_ASYNC(Produce)

#undef DEFINE_EXECUTE_ASYNC

inline Status Call(VarState &any_state) {
  return std::visit([](auto &state) { return Execute(state); }, any_state);
}

inline io::Future<Execution> CallAsync(mock::ExecutionContext &ctx, VarState &&any_state,
                                       std::function<void()> &&notifier) {
  return std::visit(
      [&ctx, notifier = std::move(notifier)](auto &&state) {
        return ExecuteAsync(ctx, std::move(notifier), std::forward<decltype(state)>(state));
      },
      std::move(any_state));
}

/// The responsibility of an executor is to be aware of how much resources is
/// available in the data/thread pools and initiate operator execution guided
/// by the query semantics.
///
/// E.g. Call Execute on an operator if:
///   1) There is data in the input data pool
///   2) There is an available worker thread
///
/// In addition, an executor is responsible to take data out of the produce
/// data pool and ship it to the data stream.
///
class Executor {
  // ThreadPools
  // PhysicalPlans

  /// Each operator should have a method of the following signature
  ///   future<status> Execute(context);
  /// because the execution will end up in a thread pool.
  ///
  /// Each Execute method should essentially process one input batch.
  ///
  /// Parallelization can easily be achieved by multiple concurrent Execute
  /// calls on a single operator.
  ///
  /// Aggregations are essentially single threaded operator which will exhaust
  /// all available input, since Executor is responsible for the semantic, it
  /// has to ensure all dependencies are executed before.
  ///
 public:
  void Execute(Operator &plan) {
    // TODO(gitbuda): Determin the execution order -> topo sort.
    std::vector<Operator *> ops{&plan};
    Operator *op = &plan;
    while (true) {
      if (op->children.size() == 0) {
        break;
      }
      op = op->children[0].get();
      ops.push_back(op);
    }
    // TODO(gitbuda): Move outside 🤔
    memgraph::utils::ThreadPool thread_pool{8};
    mock::ExecutionContext ctx{.thread_pool = &thread_pool};
    for (auto &op : ops) {
      // TODO(gitbuda): This is not correct, the point it so illustrate the concept (op.state) is moved!
      auto notifier = []() { SPDLOG_INFO("op done"); };
      auto future = CallAsync(ctx, std::move(op->state), notifier);
      auto execution = std::move(future).Wait();
      SPDLOG_INFO("name: {} has_more: {}", op->name, execution.status.has_more);
      while (execution.status.has_more) {
        auto future = CallAsync(ctx, std::move(execution.state), notifier);
        execution = std::move(future).Wait();
        SPDLOG_INFO("name: {} has_more: {}", op->name, execution.status.has_more);
      }
    }
  }
};

}  // namespace memgraph::query::v2::physical::execution
