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

#pragma once

#include "io/notifier.hpp"
#include "query/v2/physical/execution/_execution.hpp"
#include "query/v2/physical/execution/create_vertices.hpp"
#include "query/v2/physical/execution/once.hpp"
#include "query/v2/physical/execution/produce.hpp"
#include "query/v2/physical/execution/scanall.hpp"
#include "query/v2/physical/execution/unwind.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "utils/cast.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::physical::execution {

/// Helper method to pass the right state to the right execute implementation.
inline Status Call(VarState &any_state) {
  return std::visit([](auto &state) { return Execute(state); }, any_state);
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_EXECUTE_ASYNC(state_type)                                                                            \
  inline io::Future<Status> ExecuteAsync(                                                                           \
      mock::ExecutionContext &ctx, std::function<void()> notifier, /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
      state_type &state) {                                                                                          \
    auto [future, promise] = io::FuturePromisePairWithNotifications<Status>(nullptr, notifier);                     \
    auto shared_promise = std::make_shared<decltype(promise)>(std::move(promise));                                  \
    ctx.thread_pool->AddTask([&state, promise = std::move(shared_promise)]() mutable {                              \
      auto status = Execute(state);                                                                                 \
      promise->Fill(status);                                                                                        \
    });                                                                                                             \
    return std::move(future);                                                                                       \
  }
DEFINE_EXECUTE_ASYNC(CreateVertices)
DEFINE_EXECUTE_ASYNC(Once)
DEFINE_EXECUTE_ASYNC(Produce)
DEFINE_EXECUTE_ASYNC(ScanAll)
DEFINE_EXECUTE_ASYNC(Unwind)
#undef DEFINE_EXECUTE_ASYNC

/// NOTE: State is passed as a reference. Moving state is tricky because:
///   * io::Future is not yet implemented to fully support moves
///   * it's easy to make a mistake and copy the state for no reason.
///
inline io::Future<Status> CallAsync(mock::ExecutionContext &ctx, VarState &any_state, std::function<void()> notifier) {
  return std::visit([&ctx, notifier](auto &state) { return ExecuteAsync(ctx, notifier, state); }, any_state);
}

// A dummy example of how to transform DataOperator tree to the WorkOperator tree. This step is required in the
//
//
inline ExecutionPlan SequentialExecutionPlan(std::shared_ptr<DataOperator> plan) {
  // TODO(gitbuda): Implement proper topological order.
  DataOperator *data = plan.get();
  std::vector<WorkOperator> ops{WorkOperator{.data = data}};
  while (true) {
    if (data->children.empty()) {
      break;
    }
    data = data->children[0].get();
    ops.push_back(WorkOperator{.data = data});
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
  explicit Executor(size_t thread_pool_size) : thread_pool_(thread_pool_size) {}

  // TODO(gitbuda): Input to the Execute method should be some container
  // because there might be additional preprocessed data structures (e.g.
  // execution order).
  //
  // NOTE: Execution order could be determined based on both data_pool states
  // and execution priority.
  //
  uint64_t Execute(std::shared_ptr<DataOperator> deps) {
    mock::ExecutionContext ctx{.thread_pool = &thread_pool_};
    auto plan = SequentialExecutionPlan(deps);
    MG_ASSERT(!plan.ops.empty(), "Execution plan has to have at least 1 operator");
    bool any_has_more = true;
    int no = 0;
    bool init_run = true;
    while (any_has_more) {
      any_has_more = false;
      no = 0;

      // start async calls
      for (int64_t i = utils::MemcpyCast<int64_t>(plan.ops.size()) - 1; i >= 0; --i) {
        auto &op = plan.ops.at(i);
        if (!op.data->execution.status.has_more) {
          continue;
        }
        // Skip execution if there is no input data except in the first
        // iteration because the first iteration will initialize pipeline.
        if (!init_run && !op.data->HasInputData()) {
          continue;
        }
        io::ReadinessToken readiness_token{static_cast<size_t>(i)};
        std::function<void()> fill_notifier = [readiness_token, this]() { notifier_.Notify(readiness_token); };
        auto future = CallAsync(ctx, op.data->execution.state, fill_notifier);
        op.data->stats.execute_calls++;
        plan.f_execs.insert_or_assign(i, std::move(future));
        ++no;
      }
      init_run = false;

      // await async calls
      while (no > 0) {
        auto token = notifier_.Await();
        auto &op = plan.ops.at(token.GetId());
        op.data->execution.status = std::move(*(plan.f_execs.at(token.GetId()).TryGet()));
        if (op.data->execution.status.has_more) {
          any_has_more = true;
        }
        SPDLOG_TRACE("EXECUTOR: {} has_more {}", op.data->name, op.data->execution.status.has_more);
        --no;
      }
    }
    // TODO(gitbuda): Return some the whole plan or some stats.
    return plan.ops.at(0).data->stats.processed_frames;
  }

  static void PrintStats(const ExecutionPlan &plan) {
    for (const auto &op : plan.ops) {
      SPDLOG_DEBUG("EXECUTOR: {} processed {} during {} execution calls", op.data->name,
                   op.data->stats.processed_frames, op.data->stats.execute_calls);
    }
  }

 private:
  utils::ThreadPool thread_pool_;
  io::Notifier notifier_;
};

}  // namespace memgraph::query::v2::physical::execution
