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

struct PlanOperator;

struct Status {
  bool has_more;
  std::optional<std::string> error;
};
/// NOTE: In theory status and state could be coupled together, but since STATE
/// is a variant it's easier to access STATUS via generic object.

/// STATES
struct Once {
  PlanOperator *op;
  bool has_more{true};
};
struct ScanAll {
  PlanOperator *op;
  std::vector<PlanOperator *> children;
  int results;
};
struct Produce {
  PlanOperator *op;
  std::vector<PlanOperator *> children;
};
using VarState = std::variant<Once, ScanAll, Produce>;
/// STATES

struct Execution {
  Status status;
  VarState state;
};

struct PlanOperator {
  std::string name;
  std::vector<std::shared_ptr<PlanOperator>> children;
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool;
  VarState state;
};
// TODO(gitbuda): Figure out a better name for the operators.
struct ExecutionOperator {
  PlanOperator *op;
  Execution execution;
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
    auto [future, promise] = io::FuturePromisePairWithNotifications<Execution>(nullptr, std::move(notifier));       \
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
                                       std::function<void()> &&notifier) {
  return std::visit([&ctx, notifier = std::move(notifier)](
                        auto &&state) { return ExecuteAsync(ctx, notifier, std::forward<decltype(state)>(state)); },
                    std::move(any_state));
}

inline std::vector<ExecutionOperator> SequentialExecutionOrder(std::shared_ptr<PlanOperator> plan) {
  // TODO(gitbuda): Implement proper topological order.
  std::vector<ExecutionOperator> ops{
      ExecutionOperator{.op = plan.get(), .execution = Execution{.status = Status{.has_more = true}}}};
  PlanOperator *op = plan.get();
  while (true) {
    if (op->children.empty()) {
      break;
    }
    op = op->children[0].get();
    ops.push_back(ExecutionOperator{.op = op, .execution = Execution{.status = Status{.has_more = true}}});
  }
  return ops;
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
  void Execute(std::shared_ptr<PlanOperator> plan) {
    mock::ExecutionContext ctx{.thread_pool = &thread_pool_};
    auto ops = SequentialExecutionOrder(plan);

    // TODO(gitbuda): Use Notifier to Await all operators to finish.
    std::vector<io::Future<Execution>> futures;
    futures.reserve(ops.size());
    bool any_has_more = true;
    while (any_has_more) {
      any_has_more = false;
      for (int64_t i = utils::MemcpyCast<int64_t>(ops.size()) - 1; i >= 0; --i) {
        if (!ops.at(i).execution.status.has_more) {
          continue;
        }
        auto fi = futures.size();
        auto notifier = [i, fi, &futures, &ops, &any_has_more]() {
          auto *op = ops[i].op;
          ops[i].execution = *(futures[fi].TryGet());
          op->state = std::move(ops.at(i).execution.state);
          auto status = std::move(ops.at(i).execution.status);
          if (status.has_more) {
            any_has_more = true;
          }
        };
        auto *op = ops[i].op;
        auto future = CallAsync(ctx, std::move(op->state), std::move(notifier));
        futures.emplace_back(std::move(future));
      }
      std::this_thread::sleep_for(std::chrono::microseconds(10000));
      futures.clear();
      futures.reserve(ops.size());
    }
  }

 private:
  // TODO(gitbuda): Add configurable size to the executor thread pool.
  utils::ThreadPool thread_pool_{8};
};

}  // namespace memgraph::query::v2::physical::execution
