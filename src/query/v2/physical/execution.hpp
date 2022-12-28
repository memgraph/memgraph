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
  uint64_t execute_calls{0};
  uint64_t processed_frames{0};
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
  std::vector<mock::Frame> data;
  std::vector<mock::Frame>::iterator data_it{data.end()};
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

  ////// DATA POOL HANDLING
  /// Get/Push data from/to surrounding operators.
  std::optional<typename TDataPool::Token> NextRead() const { return data_pool->GetFull(); }
  void PassBackRead(const typename TDataPool::Token &token) const { data_pool->ReturnEmpty(token.id); }
  std::optional<typename TDataPool::Token> NextWrite() const { return data_pool->GetEmpty(); }
  void PassBackWrite(const typename TDataPool::Token &token) const { data_pool->ReturnFull(token.id); }

  template <typename TTuple>
  bool Emit(const TTuple &tuple) {
    if (!current_token_) {
      current_token_ = NextWrite();
      if (!current_token_) {
        return false;
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
    return true;
  }

  /// An additional function is required because sometimes, e.g., during
  /// for-range loop we don't know if there is more elements -> an easy solution
  /// is to expose an additional method.
  void CloseEmit() {
    if (!current_token_) {
      return;
    }
    PassBackWrite(*current_token_);
    current_token_ = std::nullopt;
  }

  bool IsWriterDone() const { return data_pool->IsWriterDone(); }
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
  uint64_t top_level_tuples{0};
};

/// SINGLE THREADED EXECUTE IMPLEMENTATIONS

template <typename TFun>
inline bool ProcessNext(DataOperator *input, TFun fun) {
  auto read_token = input->NextRead();
  if (read_token) {
    fun(*(read_token->multiframe));
    input->PassBackRead(*read_token);
    return true;
  }
  if (!read_token && input->IsWriterDone()) {
    // Even if read token was null before we have to exhaust the pool
    // again because in the meantime the writer could write more and hint
    // that it's done.
    read_token = input->NextRead();
    if (read_token) {
      fun(*(read_token->multiframe));
      input->PassBackRead(*read_token);
      return true;
    }
    return false;
  }
  return true;
}

inline Status Execute(Once &state) {
  MG_ASSERT(state.op->children.empty(), "{} should have 0 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto is_emitted = state.op->Emit(DataOperator::TDataPool::TFrame{});
  MG_ASSERT(is_emitted, "{} should always be able to emit", state.op->name);
  state.op->CloseEmit();
  state.op->MarkWriterDone();
  state.op->stats.processed_frames = 1;
  return Status{.has_more = false};
}

inline Status Execute(ScanAll &state) {
  MG_ASSERT(state.op->children.size() == 1, "{} should have exactly 1 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto *input = state.op->children[0].get();
  auto *output = state.op;

  // TODO(gitbuda): Add ExecutionContext and inject the data_fun probably via state.
  auto data_fun = [](ScanAll &state, DataOperator::TDataPool::TMultiframe &multiframe) {
    std::vector<DataOperator::TDataPool::TFrame> frames{};
    for (int i = 0; i < state.scan_all_elems; ++i) {
      for (int j = 0; j < multiframe.Data().size(); ++j) {
        frames.push_back(DataOperator::TDataPool::TFrame{});
      }
    }
    return frames;
  };

  // Returns true if data is inialized.
  auto init_data = [&data_fun](ScanAll &state, DataOperator *input) -> bool {
    auto read_token = input->NextRead();
    if (read_token) {
      state.data = data_fun(state, *(read_token->multiframe));
      input->PassBackRead(*read_token);
      if (state.data.empty()) {
        state.data_it = state.data.end();
        return false;
      }
      state.data_it = state.data.begin();
      return true;
    }
    return false;
  };

  // Returns true if all data has been written OR if there was no data at all.
  // Returns false if not all data has been written.
  auto write_fun = [](ScanAll &state, DataOperator *output) -> bool {
    if (state.data_it != state.data.end()) {
      int64_t cnt = 0;
      while (state.data_it != state.data.end()) {
        auto written = output->Emit(*state.data_it);
        if (!written) {
          // There is no more space -> return.
          output->stats.processed_frames += cnt;
          return false;
        }
        state.data_it += 1;
        cnt++;
      }
      output->stats.processed_frames += cnt;
      output->CloseEmit();
    }
    return true;
  };

  // First write if there is any data from the previous run.
  if (!write_fun(state, output)) {
    // If not all data has been written return control because our buffer is
    // full -> someone has to make it empty.
    return Status{.has_more = true};
  }

  MG_ASSERT(state.data_it == state.data.end(), "data_it has to be end()");
  bool more_data = init_data(state, input);
  if (!more_data && input->IsWriterDone()) {
    more_data = init_data(state, input);
    if (more_data) {
      write_fun(state, output);
      return Status{.has_more = true};
    }
    output->MarkWriterDone();
    return Status{.has_more = false};
  }
  write_fun(state, output);
  return Status{.has_more = true};
}

inline Status Execute(Produce &state) {
  MG_ASSERT(state.op->children.size() == 1, "{} should have exactly 1 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto *input = state.op->children[0].get();

  auto produce_fun = [&state](DataOperator::TDataPool::TMultiframe &multiframe) {
    auto size = multiframe.Data().size();
    state.op->stats.processed_frames += size;
  };

  return Status{.has_more = ProcessNext<decltype(produce_fun)>(input, std::move(produce_fun))};
}

/// ASYNC EXECUTE WRAPPERS

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_EXECUTE_ASYNC(state_type)                                                                            \
  inline io::Future<Execution> ExecuteAsync(                                                                        \
      mock::ExecutionContext &ctx, std::function<void()> notifier, /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
      state_type &state) {                                                                                          \
    auto [future, promise] = io::FuturePromisePairWithNotifications<Execution>(nullptr, notifier);                  \
    auto shared_promise = std::make_shared<decltype(promise)>(std::move(promise));                                  \
    ctx.thread_pool->AddTask([&state, promise = std::move(shared_promise)]() mutable {                              \
      auto status = Execute(state);                                                                                 \
      promise->Fill({.status = status});                                                                            \
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

// NOTE: State is passed as a reference. Moving state is trickly because:
//   * io::Future is not yet implemented to fully support moves
//   * it's easy to make a mistake and copy state for no reasone.
//
inline io::Future<Execution> CallAsync(mock::ExecutionContext &ctx, VarState &any_state,
                                       std::function<void()> notifier) {
  return std::visit([&ctx, notifier](auto &state) { return ExecuteAsync(ctx, notifier, state); }, any_state);
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
    while (any_has_more) {
      any_has_more = false;
      no = 0;

      // start async calls
      for (int64_t i = utils::MemcpyCast<int64_t>(plan.ops.size()) - 1; i >= 0; --i) {
        auto &op = plan.ops.at(i);
        if (!op.execution.status.has_more) {
          continue;
        }
        // TODO(gitbuda): It's possible to skip calls if PoolState == FULL.
        io::ReadinessToken readiness_token{static_cast<size_t>(i)};
        std::function<void()> fill_notifier = [readiness_token, this]() { notifier_.Notify(readiness_token); };
        auto future = CallAsync(ctx, op.data->state, fill_notifier);
        op.data->stats.execute_calls++;
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
        SPDLOG_TRACE("EXECUTOR: {} has_more {}", op.data->name, op.execution.status.has_more);
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
