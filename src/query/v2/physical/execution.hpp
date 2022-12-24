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
#include "io/notifier.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "utils/cast.hpp"
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

struct DataOperator;

struct Status {
  bool has_more;
  std::optional<std::string> error;
};
/// NOTE: In theory status and state could be coupled together, but since STATE
/// is a variant it's easier to access STATUS via generic object.

/// STATES
struct Once {
  DataOperator *op;
  bool has_more{true};
};
struct ScanAll {
  DataOperator *op;
  std::vector<DataOperator *> children;
  int results;
};
struct Produce {
  DataOperator *op;
  std::vector<DataOperator *> children;
};
using VarState = std::variant<Once, ScanAll, Produce>;
/// STATES

struct Execution {
  Status status;
  VarState state;
};

struct DataOperator {
  std::string name;
  std::vector<std::shared_ptr<DataOperator>> children;
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool;
  // Should the state be accessible from other data operators? -> Probably NOT
  // TODO(gitbuda): Remove the state from DataOperator
  // NOTE: At the moment CallAsync depends on the state here.
  // NOTE: State actually holds info about the operator type.
  VarState state;
};

struct WorkOperator {
  DataOperator *data;
  Execution execution;
};

struct ExecutionPlan {
  std::vector<WorkOperator> ops;
  std::unordered_map<size_t, io::Future<Execution>> f_execs;
};

/// SINGLE THREADED EXECUTE IMPLEMENTATIONS

inline Status Execute(Once & /*unused*/) { return Status{.has_more = false}; }

inline Status Execute(ScanAll &state) {
  if (state.results == 0) {
    return Status{.has_more = false};
  }
  state.results--;
  return Status{.has_more = true};
}

inline Status Execute(Produce &state) {
  auto &child_state = state.children[0]->state;
  if (std::holds_alternative<ScanAll>(child_state) && std::get<ScanAll>(child_state).results > 0) {
    return Status{.has_more = true};
  }
  return Status{.has_more = false};
}

/// ASYNC EXECUTE WRAPPERS

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_EXECUTE_ASYNC(state_type)                                                                            \
  inline io::Future<Execution> ExecuteAsync(                                                                        \
      mock::ExecutionContext &ctx, std::function<void()> notifier, /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
      state_type &&state) {                                                                                         \
    auto [future, promise] = io::FuturePromisePairWithNotifications<Execution>(nullptr, notifier);                  \
    auto shared_promise = std::make_shared<decltype(promise)>(std::move(promise));                                  \
    ctx.thread_pool->AddTask([state = std::move(state), promise = std::move(shared_promise)]() mutable {            \
      auto status = Execute(state);                                                                                 \
      promise->Fill({.status = status, .state = std::move(state)});                                                 \
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
                                       std::function<void()> notifier) {
  return std::visit(
      [&ctx, notifier](auto &&state) { return ExecuteAsync(ctx, notifier, std::forward<decltype(state)>(state)); },
      std::move(any_state));
}

inline ExecutionPlan SequentialExecutionPlan(std::shared_ptr<DataOperator> plan) {
  // TODO(gitbuda): Implement proper topological order.
  std::vector<WorkOperator> ops{
      WorkOperator{.data = plan.get(), .execution = Execution{.status = Status{.has_more = true}}}};
  DataOperator *data = plan.get();
  while (true) {
    if (data->children.empty()) {
      break;
    }
    data = data->children[0].get();
    ops.push_back(WorkOperator{.data = data, .execution = Execution{.status = Status{.has_more = true}}});
  }
  return ExecutionPlan{.ops = ops};
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
  // TODO(gitbuda): Input to the Execute method should be some container
  // because there might be additional preprocessed data structures (e.g.
  // execution order).
  //
  void Execute(std::shared_ptr<DataOperator> deps) {
    mock::ExecutionContext ctx{.thread_pool = &thread_pool_};
    auto plan = SequentialExecutionPlan(deps);

    bool any_has_more = true;
    int no = 0;
    while (any_has_more) {
      any_has_more = false;
      no = 0;

      // start async calls
      for (int64_t i = utils::MemcpyCast<int64_t>(plan.ops.size()) - 1; i >= 0; --i) {
        auto &op = plan.ops.at(i);
        if (!op.execution.status.has_more) {
          continue;
        }
        io::ReadinessToken readiness_token{static_cast<size_t>(i)};
        std::function<void()> fill_notifier = [notifier = notifier_, readiness_token]() {
          notifier.Notify(readiness_token);
        };
        auto future = CallAsync(ctx, std::move(op.data->state), fill_notifier);
        plan.f_execs.insert_or_assign(i, std::move(future));
        ++no;
      }

      // await async calls
      while (no > 0) {
        auto token = notifier_.Await();
        auto &op = plan.ops.at(token.GetId());
        op.execution = std::move(*(plan.f_execs.at(token.GetId()).TryGet()));
        if (op.execution.status.has_more) {
          any_has_more = true;
        }
        // TODO(gitbuda): State should be at one place at most.
        // NOTE: State has to be moved here because the next iteration expects the state.
        op.data->state = std::move(op.execution.state);
        SPDLOG_INFO("{} {}", op.data->name, op.execution.status.has_more);
        --no;
      }
    }
  }

 private:
  // TODO(gitbuda): Add configurable size to the executor thread pool.
  utils::ThreadPool thread_pool_{8};
  io::Notifier notifier_;
};

}  // namespace memgraph::query::v2::physical::execution
