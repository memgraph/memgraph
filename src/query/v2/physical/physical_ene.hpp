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
/// Physical Execute/Next/Emit Architecture Implementation
///

#include <future>
#include <iostream>
#include <memory>
#include <vector>

#include "io/future.hpp"
#include "query/plan/operator.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/mock/frame.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "utils/visitor.hpp"

namespace memgraph::query::v2::physical {

using memgraph::query::plan::HierarchicalLogicalOperatorVisitor;
using memgraph::query::plan::Once;
using memgraph::query::plan::Produce;
using memgraph::query::plan::ScanAll;

struct ExecuteStatus {
  bool has_more;
  std::optional<std::string> error;
  // TODO(gitbuda): Add std::optional<ExecuteError>
};

/// Moving/copying Frames between operators is questionable because operators
/// mostly operate on a single Frame value.

/// THREAD POOLS
/// There might be the case we need many different thread pools (some of them
/// might be the same):
///   * Query    Pool -> Responsible for top-level query work
///   * Operator Pool -> Responsible for the operator work (e.g. data moves)
///   * IO       Pool -> Responsible for the interactions with the storage
///   * CPU      Pool -> Responsible for CPU heavy operations like aggregations
///
/// TODO(gitbuda): Design thread pools and allocators.
///   * Thread pool has to support cooperative cancellation of specific tasks.

/// TODO(gitbuda): Consider variant instead of virtual for the PhysicalOperator.
/// NOTE: Frame[OutputSymbol] tells where a single operator should place values.
///
/// Each derived PhysicalOperator may be constructed with 0|1 function providing the data.
/// Each PhysicalOperator has an arbitrary number of children AKA input operators.
template <typename TDataPool>
class PhysicalOperator {
 public:
  using TExecutionContext = mock::ExecutionContext;
  // TODO(gitbuda): Consider moving TDataPool as an alias here because the
  // implementation of execution is dependent on the way how the data pool is
  // operating. -> it would simplify both implementation and usage.
  //
  explicit PhysicalOperator(const std::string &name, std::unique_ptr<TDataPool> &&data_pool)
      : name_(name), data_pool_(std::move(data_pool)) {}
  PhysicalOperator() = delete;
  PhysicalOperator(const PhysicalOperator &) = delete;
  PhysicalOperator(PhysicalOperator &&) noexcept = default;
  PhysicalOperator &operator=(const PhysicalOperator &) = delete;
  PhysicalOperator &operator=(PhysicalOperator &&) noexcept = default;
  virtual ~PhysicalOperator() {}

  void AddChild(std::shared_ptr<PhysicalOperator> child) { children_.push_back(child); }
  std::string Name() const { return name_; }

  /// Init/start execution.
  /// NOTE: In the current implementation Executes behave blocking only for the root operator.
  ///
  /// If Execute does not block, in other words, if it's limited in time or
  /// space, it has to return some status. The status should contain is there
  /// more to do + any error.
  ///
  virtual void Execute(TExecutionContext ctx) = 0;

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
  std::optional<typename TDataPool::Token> NextRead(bool exp_backoff = false) {
    auto token = data_pool_->GetFull();
    if (token) {
      return token;
    }
    // First try to read a multiframe because there might be some full frames
    // even if the whole pool is marked as exhausted. In case we come to this
    // point and the pool is exhausted there is no more data for sure.
    if (data_pool_->IsExhausted()) {
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

  void PassBackRead(const typename TDataPool::Token &token) { data_pool_->ReturnEmpty(token.id); }

  std::optional<typename TDataPool::Token> NextWrite(bool exp_backoff = false) {
    // In this case it's different compared to the NextRead because if
    // somebody has marked the pool as exhausted furher writes shouldn't be
    // possible.
    // TODO(gitbuda): Consider an assert in the NextWrite implementation.
    if (data_pool_->IsExhausted()) {
      return std::nullopt;
    }
    while (true) {
      auto token = data_pool_->GetEmpty();
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
  void PassBackWrite(const typename TDataPool::Token &token) { data_pool_->ReturnFull(token.id); }

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

  bool IsWriterDone() const { return data_pool_->IsWriterDone(); }
  /// TODO(gitbuda): Consider renaming this to MarkJobDone because the point is
  /// just to hint there will be no more data but from the caller perspective
  /// there is nothing more to do.
  /// TODO(gitbuda): Consider blending CloseEmit and MarkJobDone into a single
  /// RAII object.
  ///
  void MarkWriterDone() { data_pool_->MarkWriterDone(); }
  bool IsExhausted() const { return data_pool_->IsExhausted(); }
  void MarkExhausted() { data_pool_->MarkExhausted(); }
  ////// DATA POOL HANDLING

  /// Reset state.
  void Reset() {}
  /// Set execution timeout.
  void Timeout(int /*unused*/) {}
  /// Quit execution as soon as possible.
  void Cancel() {}
  /// Deallocate all resources.
  void Shutdown() {}

  struct Stats {
    int64_t processed_frames;
  };
  const Stats &GetStats() const { return stats_; }

 protected:
  std::string name_;
  std::vector<std::shared_ptr<PhysicalOperator<TDataPool>>> children_;
  /// TODO(gitbuda): Pass the Multiframe capacity via the PhysicalOperator constructor.
  /// uptr is here to make the PhysicalOperator moveable.
  /// TODO(gitbuda): Make it more clear that below is a uptr.
  std::unique_ptr<TDataPool> data_pool_;
  std::optional<typename TDataPool::Token> current_token_;
  Stats stats_{.processed_frames = 0};
  std::function<void(typename TDataPool::TMultiframe &multiframe)> fun_;
  std::function<void(void)> after_done_fun_;

  void ExecuteChildren(TExecutionContext ctx) {
    for (const auto &child : children_) {
      child->Execute(ctx);
    }
  }

  /// This method is iterating over all multiframes from a single input and
  /// process them serially. This method is reading all from the input (from
  /// the input data pool). The injected function (fun) will write data to the
  /// data pool of this operator.
  ///
  void SingleChildSingleThreadExaust(TExecutionContext ctx,
                                     std::function<void(typename TDataPool::TMultiframe &multiframe)> fun,
                                     std::function<void(void)> after_done_fun) {
    // NOTE/PROBLEM/TODO(gitbuda): Since all operators are while true loops,
    // what if there is more operators then threads in the pool -> infinite
    // looping -> "bottom" operators should run first. Keep in mind that's not
    // the full story. Because of the limited data/result pool, the "upper"
    // operators should also be executed at some point.
    //
    // Inability to write has to block the thread if the context should be
    // preserved. On the other side, if a function context can be preserved in
    // some way -> it's ok to return from Emit.
    //
    //   * IDEA: Introduce phsical plan executor who will execute plan based on
    //   the available number of threads in the thread pool.
    //
    //   * IDEA: Introduce read and write thread pools, or maintain limited
    //   number of threads in a pool -> hard because operators are both read
    //   and write in terms of processing frames.

    // If we don't store functions, they are deleted because this function ends
    // quickly (AddTask does not block).
    fun_ = fun;
    after_done_fun_ = after_done_fun;

    // Since the problem with the "scheduling" maybe the worker thread should
    // process one multiframe and then return.

    // It's possible to use the future + promise to sync execution and
    // exchange some data.
    std::promise<bool> promise;
    auto future = promise.get_future();

    // The logic here is tricky because you have to think about both reader and writer.
    ctx.thread_pool->AddTask([&, this]() {
      auto *input = children_[0].get();
      // TODO(gitbuda): All this logic (except the fun(...) call) could be
      // moved to NextRead?
      //
      // TODO(gitbuda): One obvious problem is the top level operator are
      // spinning a lot until some data comes in.
      //
      int64_t iter{0};
      while (true) {
        iter++;
        auto read_token = input->NextRead();
        if (read_token) {
          fun_(*(read_token->multiframe));
          input->PassBackRead(*read_token);
        }

        // If empty and the writer has finished his job -> mark exhaustion
        if (!read_token && input->IsWriterDone()) {
          // Even if read token was null before we have to exhaust the pool
          // again because in the meantime the writer could write more and hint
          // that it's done.
          while (true) {
            read_token = input->NextRead();
            if (read_token) {
              fun_(*(read_token->multiframe));
              input->PassBackRead(*read_token);
            } else {
              break;
            }
          }
          input->MarkExhausted();
        }
        // If empty and marked as exhausted -> STOP
        if (!read_token && input->IsExhausted()) {
          break;
        }
      }
      after_done_fun_();

      // TODO/BROKEN(gitbuda): Replace this with something better, top operator should be notified.
      if (name_ == "Physical Produce") {
        promise.set_value(true);
      }
    });
    if (name_ == "Physical Produce") {
      future.wait();
    }
  }

  void SingleChildParallelExaust(TExecutionContext /*unused*/,
                                 std::function<void(typename TDataPool::TMultiframe &multiframe)> fun,
                                 std::function<void(void)> after_done_fun) {
    fun_ = fun;
    after_done_fun_ = after_done_fun;
    // TODO(gitbuda): Start N threads in parallel
  }
};

// TODO(gitbuda): Consider kicking out Once because it's an artefact from the single Frame execution.
/// In the Execute/Next/Emit concept once is implemented very concisely.
template <typename TDataPool>
class OncePhysicalOperator final : public PhysicalOperator<TDataPool> {
  using Base = PhysicalOperator<TDataPool>;
  using TExecutionContext = mock::ExecutionContext;

 public:
  explicit OncePhysicalOperator(const std::string &name, std::unique_ptr<TDataPool> &&data_pool)
      : PhysicalOperator<TDataPool>(name, std::move(data_pool)) {}

  void Execute(TExecutionContext ctx) override {
    MG_ASSERT(Base::children_.empty(), "{} should have 0 input/child", Base::name_);
    SPDLOG_TRACE("{} Execute()", Base::name_);
    Base::ExecuteChildren(ctx);

    Base::Emit(typename TDataPool::TFrame{});
    Base::CloseEmit();
    Base::MarkWriterDone();
  }
};

/// NOTE: ScanAll get_vertices needs frame+ctx because ScanAllByXYZValue has an
/// expression that has to be evaluated.
///
template <typename TDataFun, typename TDataPool>
class ScanAllPhysicalOperator final : public PhysicalOperator<TDataPool> {
  using Base = PhysicalOperator<TDataPool>;
  using TExecutionContext = mock::ExecutionContext;

 public:
  ScanAllPhysicalOperator(const std::string &name, TDataFun data_fun, std::unique_ptr<TDataPool> &&data_pool)
      : PhysicalOperator<TDataPool>(name, std::move(data_pool)), data_fun_(std::move(data_fun)) {}

  void Execute(TExecutionContext ctx) override {
    MG_ASSERT(Base::children_.size() == 1, "{} should have exactly 1 input/child", Base::name_);
    SPDLOG_TRACE("{} Execute()", Base::name_);
    Base::ExecuteChildren(ctx);

    // SCANALL PSEUDO
    // ∀mf : input multiframes
    //  ∀tuple : data_fun(evaluate(mf))
    //   emit(tuple)
    //
    // NOTE: ScanAll is about getting data from the database and evaluate the
    // filtering expression -> if the whole filter is pushed down -> ScanAll is
    // only about fetching data and passing it further.
    //
    // The general logic here is:
    //  1. give me the input multiframe
    //  2. iterate over fetched or calculated data (more iterators might be involved here)
    //  3. store results to the output multiframes (owned multiframes)
    //
    //  THE TRADEOFF -> iterate / wait forever (both because of input or
    //  output) OR return control to the caller
    //
    //    * IDEA: Limiting of the input data is done by design (each multiframe
    //    is limited), but limiting the amount of the produced data could be
    //    injected. In addition, limiting the execution time might also be
    //    valuable.
    //
    //    * IDEA: In both cases (limiting time or output data), state/stack
    //    preserve mechanism has to be in place. With multiple concurrent
    //    threads per operator, it gets more complex.
    //
    this->stats_.processed_frames = 0;
    Base::SingleChildSingleThreadExaust(
        ctx,
        [this, &ctx](typename TDataPool::TMultiframe &multiframe) {
          int64_t cnt{0};
          for (const auto &new_frame : data_fun_(multiframe, ctx)) {
            Base::Emit(new_frame);
            cnt++;
          }
          this->stats_.processed_frames += cnt;
          Base::CloseEmit();
        },
        [this]() { Base::MarkWriterDone(); });
  }

 private:
  TDataFun data_fun_;
};

/// Produce responsibility is to evaluate the result list of expressions.
template <typename TDataPool>
class ProducePhysicalOperator final : public PhysicalOperator<TDataPool> {
  using Base = PhysicalOperator<TDataPool>;
  using TExecutionContext = mock::ExecutionContext;

 public:
  using PhysicalOperator<TDataPool>::PhysicalOperator;

  void Execute(TExecutionContext ctx) override {
    MG_ASSERT(Base::children_.size() == 1, "{} should have exactly 1 input/child", Base::name_);
    SPDLOG_TRACE("{} Execute()", Base::name_);
    Base::ExecuteChildren(ctx);

    this->stats_.processed_frames = 0;
    Base::SingleChildSingleThreadExaust(
        ctx,
        [this](typename TDataPool::TMultiframe &multiframe) {
          auto size = multiframe.Data().size();
          this->stats_.processed_frames += size;
        },
        [this]() { Base::MarkWriterDone(); });
  }
};

/// TODO(gitbuda): Consider how to add higher level database concepts like
/// authorization.

template <typename TDataPool>
class PhysicalPlanGenerator final : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  using TPhysicalOperator = PhysicalOperator<TDataPool>;
  using TPhysicalOperatorPtr = std::shared_ptr<TPhysicalOperator>;
  using TOnceOperator = OncePhysicalOperator<TDataPool>;
  template <typename TDataFun>
  using TScanAllOperator = ScanAllPhysicalOperator<TDataFun, TDataPool>;
  using TProduceOperator = ProducePhysicalOperator<TDataPool>;
  using TExecutionContext = mock::ExecutionContext;

  std::vector<TPhysicalOperatorPtr> pops_;

  TPhysicalOperatorPtr Generate() {
    MG_ASSERT(pops_.size() == 1, "Number of generated physical operators too big.");
    return pops_.back();
  }

  bool DefaultPost() {
    if (pops_.size() == 1) return true;
    pops_.pop_back();
    return true;
  }

  bool DefaultPre(TPhysicalOperatorPtr pop) {
    if (pops_.empty()) {
      pops_.push_back(pop);
      return true;
    }
    pops_.back()->AddChild(pop);
    pops_.emplace_back(std::move(pop));
    return true;
  }

  bool PreVisit(Produce & /*unused*/) override {
    auto data_pool = std::make_unique<TDataPool>(16, 100);
    auto pop = std::make_shared<TProduceOperator>(TProduceOperator("Physical Produce", std::move(data_pool)));
    return DefaultPre(pop);
  }

  bool PostVisit(Produce & /*unused*/) override { return DefaultPost(); }

  bool PreVisit(ScanAll & /*unused*/) override {
    // NOTE: In the ScanAll case srm.Request(state, scanned_vertices) has to be
    // called until the state becomes COMPLETED. Return data type after all
    // Request calls is Iterator<Batch> (many std::vector<VertexId>).
    //
    // Evaluating the input frames + send requests + putting data into the output frames
    // is the actual work for the ScanAllPhysicalOperator.
    //
    // output_frame[output_symbol_] = TypedValue(std::move(*current_vertex_it));
    //   * TODO(gitbuda): Add output_symbol to the PhysicalOperators
    //
    // TODO(gitbuda): What would be a nice interface for ScanAll::Fetch function?
    //   * For each Multiframe there will be an Iterator<Batch>
    //     -> 1 data_fun call for each Multiframe
    //       -> parallelization possible by calling data_fun from multiple threads
    //
    auto data_fun = [](typename TDataPool::TMultiframe &multiframe, TExecutionContext &) {
      std::vector<mock::Frame> frames;
      for (const auto &frame : multiframe.Data()) {
        frames.push_back(frame);
      }
      return frames;
    };
    auto data_pool = std::make_unique<TDataPool>(16, 100);
    auto pop = std::make_shared<TScanAllOperator<decltype(data_fun)>>(
        TScanAllOperator<decltype(data_fun)>("Physical ScanAll", std::move(data_fun), std::move(data_pool)));
    return DefaultPre(pop);
  }

  bool PostVisit(ScanAll & /*unused*/) override { return DefaultPost(); }

  bool Visit(Once & /*unused*/) override {
    auto data_pool = std::make_unique<TDataPool>(16, 100);
    auto pop = std::make_shared<TOnceOperator>(TOnceOperator("Physical Once", std::move(data_pool)));
    MG_ASSERT(!pops_.empty(), "There is nothing where Once could be attached.");
    pops_.back()->AddChild(pop);
    return true;
  }
};

}  // namespace memgraph::query::v2::physical
