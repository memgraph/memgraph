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

struct Stats {
  int64_t processed_frames{0};
};

/// STATES
struct Once {
  DataOperator *op;
  bool has_more{true};
};
struct ScanAll {
  DataOperator *op;
  std::vector<DataOperator *> children;
  int scan_all_elems;
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
  using TDataPool = multiframe::MPMCMultiframeFCFSPool;

  std::string name;
  std::vector<std::shared_ptr<DataOperator>> children;
  std::unique_ptr<TDataPool> data_pool;
  // Should the state be accessible from other data operators? -> Probably NOT
  // TODO(gitbuda): Remove the state from DataOperator
  // NOTE: At the moment CallAsync depends on the state here.
  // NOTE: State actually holds info about the operator type.
  // NOTE: The state here is also used in the sync execution.
  VarState state;
  std::optional<typename TDataPool::Token> current_token_;
  Stats stats;

  // TODO(gitbuda): DataPool access code is duplicated -> move to a base class or to the data pool itself.

  ////// DATA POOL HANDLING
  /// Get/Push data from/to surrounding operators.
  ///
  /// 2 MAIN CONCERNS:
  ///   * No more data  -> the writer hinted there is no more data
  ///                      read first to check if there is more data and return
  ///                      check for exhaustion because the reader will mark exhausted  when there is no more data +
  ///                      writer hint is set
  ///   * No more space -> the writer is too slow -> wait for a moment
  ///
  /// responsible for
  /// if nullopt -> there is nothing more to read AT THE MOMENT -> repeat again as soon as possible
  /// NOTE: Caller is responsible for checking if there is no more data at all by checking both token and exhaustion.
  ///
  /// TODO(gitbuda): Move the sleep on the caller side | add a flag to enable/disable sleep here?
  std::optional<typename TDataPool::Token> NextRead(bool exp_backoff = false) const {
    auto token = data_pool->GetFull();
    if (token) {
      return token;
    }
    // First try to read a multiframe because there might be some full frames
    // even if the whole pool is marked as exhausted. In case we come to this
    // point and the pool is exhausted there is no more data for sure.
    if (data_pool->IsExhausted()) {
      return std::nullopt;
    }
    if (exp_backoff) {
      // TODO(gitbuda): Replace with exponential backoff or something similar.
      // Sleep only once in this loop and return because if the whole plan is
      // executed in a single thread, this will just block.
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return std::nullopt;
  }

  void PassBackRead(const typename TDataPool::Token &token) const { data_pool->ReturnEmpty(token.id); }

  std::optional<typename TDataPool::Token> NextWrite(bool exp_backoff = false) const {
    // In this case it's different compared to the NextRead because if
    // somebody has marked the pool as exhausted furher writes shouldn't be
    // possible.
    // TODO(gitbuda): Consider an assert in the NextWrite implementation.
    if (data_pool->IsExhausted()) {
      return std::nullopt;
    }
    while (true) {
      auto token = data_pool->GetEmpty();
      if (token) {
        return token;
      }
      if (exp_backoff) {
        // TODO(gitbuda): Replace with exponential backoff or something similar.
        // TODO(gitbuda): Sleeping is not that good of an option -> consider conditional variables.
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }
  }
  void PassBackWrite(const typename TDataPool::Token &token) const { data_pool->ReturnFull(token.id); }

  template <typename TTuple>
  void Emit(const TTuple &tuple) {
    if (!current_token_) {
      // TODO(gitbuda): We have to wait here if there is no empty multiframe,
      // NOTE: This wait here is tricky because if there is no more space, this
      // thread will spin -> one less thread in the thread pool -> REFACTOR ->
      // everything has to give the control back at some point. NOTE: An issue
      // on the other side are active iterators, because that's context has to
      // be restored.
      //
      current_token_ = NextWrite();
      if (!current_token_) {
        return;
      }
    }

    // It might be the case that previous Emit just put a frame at the last
    // available spot, but that's covered in the next if condition. In other
    // words, because of the logic above and below, the multiframe in the next
    // line will have at least one empty spot for the frame.
    current_token_->multiframe->PushBack(tuple);
    // TODO(gitbuda): Remove any potential copies from here.
    if (current_token_->multiframe->IsFull()) {
      CloseEmit();
    }
  }

  /// An additional function is required because sometimes, e.g., during
  /// for-range loop we don't know if there is more elements -> an easy solution
  /// is to expose an additional method.
  ///
  /// TODO(gitbuda): Rename CloseEmit to something better.
  void CloseEmit() {
    if (!current_token_) {
      return;
    }
    PassBackWrite(*current_token_);
    current_token_ = std::nullopt;
  }

  /// TODO(gitbuda): Create Emit which is suitable to be used in the concurrent environment.
  template <typename TTuple>
  void EmitWhere(const typename TDataPool::Token &where, const TTuple &what) {}

  bool IsWriterDone() const { return data_pool->IsWriterDone(); }
  /// TODO(gitbuda): Consider renaming this to MarkJobDone because the point is
  /// just to hint there will be no more data but from the caller perspective
  /// there is nothing more to do.
  /// TODO(gitbuda): Consider blending CloseEmit and MarkJobDone into a single
  /// RAII object.
  ///
  void MarkWriterDone() const { data_pool->MarkWriterDone(); }
  bool IsExhausted() const { return data_pool->IsExhausted(); }
  void MarkExhausted() const { data_pool->MarkExhausted(); }
  ////// DATA POOL HANDLING
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

template <typename TFun>
inline Status ProcessNext(DataOperator *input, DataOperator *output, TFun fun) {
  auto read_token = input->NextRead();
  if (read_token) {
    fun(*(read_token->multiframe));
    input->PassBackRead(*read_token);
    return Status{.has_more = true};
  }
  if (!read_token && input->IsWriterDone()) {
    // Even if read token was null before we have to exhaust the pool
    // again because in the meantime the writer could write more and hint
    // that it's done.
    read_token = input->NextRead();
    if (read_token) {
      fun(*(read_token->multiframe));
      input->PassBackRead(*read_token);
      return Status{.has_more = true};
    }
    input->MarkExhausted();
    output->MarkWriterDone();
    return Status{.has_more = false};
  }
  return Status{.has_more = true};
}

inline Status Execute(Once &state) {
  MG_ASSERT(state.op->children.empty(), "{} should have 0 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  state.op->Emit(DataOperator::TDataPool::TFrame{});
  state.op->CloseEmit();
  state.op->MarkWriterDone();
  return Status{.has_more = false};
}

inline Status Execute(ScanAll &state) {
  MG_ASSERT(state.op->children.size() == 1, "{} should have exactly 1 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto *input = state.op->children[0].get();

  // TODO(gitbuda): Add ExecutionContext and inject the data_fun probably via state.
  auto data_fun = [&state](DataOperator::TDataPool::TMultiframe &multiframe) {
    std::vector<DataOperator::TDataPool::TFrame> frames;
    for (int i = 0; i < state.scan_all_elems; ++i) {
      for (int j = 0; j < multiframe.Data().size(); ++j) {
        frames.push_back(DataOperator::TDataPool::TFrame{});
      }
    }
    return frames;
  };

  auto scan_all_fun = [&state, &data_fun](DataOperator::TDataPool::TMultiframe &multiframe) {
    int64_t cnt{0};
    for (const auto &new_frame : data_fun(multiframe)) {
      state.op->Emit(new_frame);
      cnt++;
    }
    state.op->stats.processed_frames += cnt;
    state.op->CloseEmit();
  };

  return ProcessNext<decltype(scan_all_fun)>(input, state.op, std::move(scan_all_fun));
}

inline Status Execute(Produce &state) {
  MG_ASSERT(state.op->children.size() == 1, "{} should have exactly 1 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto input = state.op->children[0].get();

  auto produce_fun = [&state](DataOperator::TDataPool::TMultiframe &multiframe) {
    auto size = multiframe.Data().size();
    state.op->stats.processed_frames += size;
  };

  return ProcessNext<decltype(produce_fun)>(input, state.op, std::move(produce_fun));
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

    for (auto &op : plan.ops) {
      SPDLOG_INFO("{} processed {}", op.data->name, op.data->stats.processed_frames);
    }
  }

 private:
  // TODO(gitbuda): Add configurable size to the executor thread pool.
  utils::ThreadPool thread_pool_{8};
  io::Notifier notifier_;
};

}  // namespace memgraph::query::v2::physical::execution
