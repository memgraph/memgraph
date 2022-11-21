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
class PhysicalOperator {
 public:
  explicit PhysicalOperator(const std::string &name)
      : name_(name), data_pool_(std::make_unique<multiframe::MPMCMultiframeFCFSPool>(16, 100)) {}
  PhysicalOperator() = delete;
  PhysicalOperator(const PhysicalOperator &) = delete;
  PhysicalOperator(PhysicalOperator &&) = default;
  PhysicalOperator &operator=(const PhysicalOperator &) = delete;
  PhysicalOperator &operator=(PhysicalOperator &&) = default;
  virtual ~PhysicalOperator() {}

  void AddChild(std::shared_ptr<PhysicalOperator> child) { children_.push_back(child); }
  std::string Name() const { return name_; }

  /// Init/start execution.
  virtual void Execute(ExecutionContext ctx) = 0;

  ////// DATA POOL HANDLING
  /// Get/Push data from/to surrounding operators.
  /// if nullopt -> there is nothing more to read or write.
  std::optional<multiframe::Token> NextRead() {
    while (true) {
      auto token = data_pool_->GetFull();
      if (token) {
        return token;
      }
      // First try to read a multiframe because there might be some full frames
      // even if the whole pool is marked as exhaused. In case we come to this
      // point and the pool is exhaused ther is no more data for sure.
      if (data_pool_->IsExhausted()) {
        return std::nullopt;
      }
      // TODO(gitbuda): Replace with exponential backoff or something similar.
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
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
  void Emit(const TTuple &tuple, bool has_more = true) {
    if (!current_token_) {
      current_token_ = data_pool_->GetEmpty();
      if (!current_token_) {
        return;
      }
    }
    if (current_token_->multiframe->IsFull()) {
      // TODO(gitbuda): What happens if we don't manage to populate the full multiframe -> pass is_full flag to Emit.
      data_pool_->ReturnFull(current_token_->id);
      current_token_ = std::nullopt;
      current_token_ = data_pool_->GetEmpty();
      if (!current_token_) {
        return;
      }
    }
    current_token_->multiframe->PushBack(tuple);
    if (!has_more) {
      data_pool_->ReturnFull(current_token_->id);
      current_token_ = std::nullopt;
    }
  }
  void CloseEmit() {
    if (!current_token_) {
      return;
    }
    data_pool_->ReturnFull(current_token_->id);
    current_token_ = std::nullopt;
  }
  /// TODO(gitbuda): Create Emit which is suitable to be used in the concurrent environment.
  template <typename TTuple>
  void EmitWhere(const multiframe::Token &where, const TTuple &what) {}
  ////// DATA POOL HANDLING

  /// Reset state.
  void Reset() {}
  /// Set execution timeout.
  void Timeout(int /*unused*/) {}
  /// Quit execution as soon as possible.
  void Cancel() {}
  /// Deallocate all resources.
  void Shutdown() {}

 protected:
  std::string name_;
  std::vector<std::shared_ptr<PhysicalOperator>> children_;
  /// TODO(gitbuda): Pass the Multiframe capacity via the PhysicalOperator constructor.
  /// uptr is here to make the PhysicalOperator moveable.
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool_;
  std::optional<multiframe::Token> current_token_;

  void BaseExecute(ExecutionContext ctx) {
    for (const auto &child : children_) {
      child->Execute(ctx);
    }
  }
};

// TODO(gitbuda): Consider kicking out Once because it's an artefact from the single Frame execution.
/// In the Execute/Next/Emit concept once is implemented very concisely.
class OncePhysicalOperator final : public PhysicalOperator {
 public:
  explicit OncePhysicalOperator(const std::string &name) : PhysicalOperator(name) {}
  void Execute(ExecutionContext ctx) override {
    MG_ASSERT(children_.empty(), "Once should have 0 inputs");
    std::cout << name_ << std::endl;
    PhysicalOperator::BaseExecute(ctx);
    this->Emit(Frame{}, false);
    data_pool_->MarkExhausted();
  }
};

/// NOTE: ScanAll get_vertices needs frame+ctx because ScanAllByXYZValue has an
/// expression that has to be evaluated.
///
template <typename TDataFun>
class ScanAllPhysicalOperator final : public PhysicalOperator {
 public:
  ScanAllPhysicalOperator(const std::string &name, TDataFun data_fun)
      : PhysicalOperator(name), data_fun_(std::move(data_fun)) {}

  void Execute(ExecutionContext ctx) override {
    std::cout << name_ << std::endl;
    MG_ASSERT(children_.size() == 1, "ScanAll should have exactly 1 input");
    PhysicalOperator::BaseExecute(ctx);

    // SCANALL PSEUDO
    // ∀mf : input multiframes
    //  ∀tuple : data_fun(evaluate(mf))
    //   emit(tuple)
    //
    // TODO(gitbuda): It should be possible to parallelize the below ScanAll code.
    auto *input = children_[0].get();
    while (true) {
      auto read_token = input->NextRead();
      if (read_token) {
        // TODO(gitbuda): Replace frame w/ multiframe.
        // NOTE: Again, very concise and parallelizable implementation.
        for (const auto &frame : read_token->multiframe->Data()) {
          for (const auto &new_frame : data_fun_(Evaluate(frame))) {
            Emit(new_frame);
          }
        }
        // Since Emit in the for loops hasn't passed has_more=false,
        // TODO(gitbuda): Rename CloseEmit to something better.
        CloseEmit();
      } else {
        // TODO(gitbuda): Test  below Execute logic is wrong, reconsider.
        data_pool_->MarkExhausted();
      }
      if (data_pool_->IsExhausted()) {
        break;
      }
    }
  }

 private:
  template <typename T>
  T Evaluate(T t) {
    return t;
  }
  TDataFun data_fun_;
};

class ProducePhysicalOperator final : public PhysicalOperator {
 public:
  using PhysicalOperator::PhysicalOperator;
  void Execute(ExecutionContext ctx) override {
    std::cout << name_ << std::endl;
    MG_ASSERT(children_.size() == 1, "ScanAll should have exactly 1 input");
    PhysicalOperator::BaseExecute(ctx);

    // NOTE: The single threaded
    auto *input = children_[0].get();
    while (true) {
      auto read_token = input->NextRead();
      if (read_token) {
        for (const auto &frame : read_token->multiframe->Data()) {
          std::cout << "Produce: " << frame.a << std::endl;
        }
      } else {
        data_pool_->MarkExhausted();
      }
      if (data_pool_->IsExhausted()) {
        break;
      }
    }
  }
};

/// TODO(gitbuda): Consider how to add higher level database concepts like
/// authorization.

class PhysicalPlanGenerator final : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  std::vector<std::shared_ptr<PhysicalOperator>> pops_;

  std::shared_ptr<PhysicalOperator> Generate() {
    MG_ASSERT(pops_.size() == 1, "Number of generated physical operators too big.");
    return pops_.back();
  }

  bool DefaultPost() {
    if (pops_.size() == 1) return true;
    pops_.pop_back();
    return true;
  }

  bool DefaultPre(std::shared_ptr<PhysicalOperator> pop) {
    if (pops_.empty()) {
      pops_.push_back(pop);
      return true;
    }
    pops_.back()->AddChild(pop);
    pops_.emplace_back(std::move(pop));
    return true;
  }

  bool PreVisit(Produce & /*unused*/) override {
    auto pop = std::make_shared<ProducePhysicalOperator>(ProducePhysicalOperator("Physical Produce"));
    return DefaultPre(pop);
  }

  bool PostVisit(Produce & /*unused*/) override { return DefaultPost(); }

  bool PreVisit(ScanAll & /*unused*/) override {
    auto data_fun = [](Frame frame) {
      std::vector<Frame> frames;
      frames.push_back(frame);
      return frames;
    };
    auto pop = std::make_shared<ScanAllPhysicalOperator<decltype(data_fun)>>(
        ScanAllPhysicalOperator("Physical ScanAll", std::move(data_fun)));
    return DefaultPre(pop);
  }

  bool PostVisit(ScanAll & /*unused*/) override { return DefaultPost(); }

  bool Visit(Once & /*unused*/) override {
    auto pop = std::make_shared<OncePhysicalOperator>(OncePhysicalOperator("Physical Once"));
    MG_ASSERT(!pops_.empty(), "There is nothing where Once could be attached.");
    pops_.back()->AddChild(pop);
    return true;
  }
};

}  // namespace memgraph::query::v2::physical
