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

#include <iostream>
#include <memory>
#include <vector>

#include "query/plan/operator.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "utils/thread_pool.hpp"
#include "utils/visitor.hpp"

namespace memgraph::query::v2::physical {

using memgraph::query::plan::HierarchicalLogicalOperatorVisitor;
using memgraph::query::plan::Once;
using memgraph::query::plan::Produce;
using memgraph::query::plan::ScanAll;
using memgraph::utils::ThreadPool;

/// Moving/copying Frames between operators is questionable because operators
/// mostly operate on a single Frame value.
///
/// TODO(gitbuda): Consider adding PipelineSegment abstraction.
///
struct PipelineSegment {};

/// Access to the thread and data pools should be given via the
/// ExecutionContext.
///
struct ExecutionContext {
  ThreadPool *thread_pool;
};

/// THREAD POOLS
/// There might be the case we need many different thread pools (some of them
/// might be the same):
///   * Query    Pool -> Responsible for top-level query work
///   * Operator Pool -> Responsible for the operator work (e.g. data moves)
///   * IO       Pool -> Responsible for the interactions with the storage
///   * CPU      Pool -> Responsible for CPU heavy operations like aggregations
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
  virtual void Execute(ExecutionContext ctx) = 0;

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
  std::optional<multiframe::Token> NextRead(bool exp_backoff = false) {
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
  void PassBackRead(const multiframe::Token &token) { data_pool_->ReturnEmpty(token.id); }

  std::optional<multiframe::Token> NextWrite() {
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
      // TODO(gitbuda): Replace with exponential backoff or something similar.
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  }
  void PassBackWrite(const multiframe::Token &token) { data_pool_->ReturnFull(token.id); }

  template <typename TTuple>
  void Emit(const TTuple &tuple) {
    if (!current_token_) {
      // TODO(gitbuda): We have to wait here if there is no empty multiframe.
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
  void EmitWhere(const multiframe::Token &where, const TTuple &what) {}

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
  std::optional<multiframe::Token> current_token_;
  Stats stats_{.processed_frames = 0};
  std::function<void(multiframe::Multiframe &multiframe)> fun_;
  std::function<void(void)> after_done_fun_;

  void BaseExecute(ExecutionContext ctx) {
    for (const auto &child : children_) {
      child->Execute(ctx);
    }
  }

  /// This method is iterating over all multiframes from a single input and
  /// process them serially. This method is reading all from the input (from
  /// the input data pool). The injected function (fun) will write data to the
  /// data pool of this operator.
  ///
  void SingleChildSingleThreadExaust(ExecutionContext ctx, std::function<void(multiframe::Multiframe &multiframe)> fun,
                                     std::function<void(void)> after_done_fun) {
    // If we don't store the function, it's gets deleted.
    fun_ = fun;
    after_done_fun_ = after_done_fun;
    // The logic here is tricky because you have to think about both reader and writer.
    ctx.thread_pool->AddTask([&, this]() {
      auto *input = children_[0].get();
      // TODO(gitbuda): All this logic (except the fun(...) call) could be moved to NextRead!
      while (true) {
        auto read_token = input->NextRead();
        // If empty and the writer has finished his job -> mark exhaustion
        if (!read_token && input->IsWriterDone()) {
          input->MarkExhausted();
        }
        // If empty and marked as exhausted -> STOP
        if (!read_token && input->IsExhausted()) {
          break;
        }
        if (read_token) {
          fun_(*(read_token->multiframe));
          input->PassBackRead(*read_token);
        }
      }
      after_done_fun_();
    });
  }
};

// TODO(gitbuda): Consider kicking out Once because it's an artefact from the single Frame execution.
/// In the Execute/Next/Emit concept once is implemented very concisely.
template <typename TDataPool>
class OncePhysicalOperator final : public PhysicalOperator<TDataPool> {
  using Base = PhysicalOperator<TDataPool>;

 public:
  explicit OncePhysicalOperator(const std::string &name, std::unique_ptr<TDataPool> &&data_pool)
      : PhysicalOperator<TDataPool>(name, std::move(data_pool)) {}
  void Execute(ExecutionContext ctx) override {
    MG_ASSERT(Base::children_.empty(), "Once should have 0 inputs");
    std::cout << Base::name_ << std::endl;
    Base::BaseExecute(ctx);
    Base::Emit(Frame{});
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

 public:
  ScanAllPhysicalOperator(const std::string &name, TDataFun data_fun, std::unique_ptr<TDataPool> &&data_pool)
      : PhysicalOperator<TDataPool>(name, std::move(data_pool)), data_fun_(std::move(data_fun)) {}

  void Execute(ExecutionContext ctx) override {
    std::cout << Base::name_ << std::endl;
    MG_ASSERT(Base::children_.size() == 1, "ScanAll should have exactly 1 input");
    Base::BaseExecute(ctx);

    // SCANALL PSEUDO
    // ∀mf : input multiframes
    //  ∀tuple : data_fun(evaluate(mf))
    //   emit(tuple)
    //
    // NOTE: Again, very concise and parallelizable implementation.
    //
    // TODO(gitbuda): It should be possible to parallelize the below ScanAll
    // because the underlying data pool provides all needed to manage the
    // result data.
    Base::SingleChildSingleThreadExaust(
        ctx,
        [this, &ctx](multiframe::Multiframe &multiframe) {
          // for (auto &batch : data_fun_(multiframe, ctx)) {
          //    for (auto &vertex : batch) {
          //     // PROBLEM/TODO(gitbuda): Emit is designed to accept the full Frame,
          //     // while we only need to add single variable into the frame.
          //     // BUT we also need the entire "input" frame to pass it forward
          //   }
          // }
          for (const auto &new_frame : data_fun_(multiframe, ctx)) {
            this->stats_.processed_frames++;
            Base::Emit(new_frame);
          }
          Base::CloseEmit();
        },
        [this]() {
          // TODO(gitbuda): MarkWritterDone has to know when all the thread is done.
          Base::MarkWriterDone();
        });
  }

 private:
  TDataFun data_fun_;
};

template <typename TDataPool>
class ProducePhysicalOperator final : public PhysicalOperator<TDataPool> {
  using Base = PhysicalOperator<TDataPool>;

 public:
  using PhysicalOperator<TDataPool>::PhysicalOperator;

  void Execute(ExecutionContext ctx) override {
    std::cout << Base::name_ << std::endl;
    MG_ASSERT(Base::children_.size() == 1, "ScanAll should have exactly 1 input");
    Base::BaseExecute(ctx);

    Base::SingleChildSingleThreadExaust(
        ctx, [this](multiframe::Multiframe &multiframe) { this->stats_.processed_frames += multiframe.Data().size(); },
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
    auto data_fun = [](multiframe::Multiframe &multiframe, ExecutionContext &) {
      std::vector<Frame> frames;
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
