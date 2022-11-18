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
#include "utils/thread_pool.hpp"
#include "utils/visitor.hpp"

namespace memgraph::query::v2::physical {

using memgraph::query::plan::HierarchicalLogicalOperatorVisitor;
using memgraph::query::plan::Once;
using memgraph::query::plan::Produce;
using memgraph::query::plan::ScanAll;
using memgraph::utils::ThreadPool;

struct Frame {
  int64_t a;
  int64_t b;
};

/// Fixed in size during query execution.
/// NOTE/TODO(gitbuda): Accessing Multiframe might be tricky because of multi-threading.
/// As soon as one operator "gives" data to the other operator (in any direction), if operators
/// operate on different threads there has to be some way of synchronization.
///   OPTIONS:
///     * Make Multiframe thread-safe.
///     * Ensure returned Multiframe is not accessed until is not reclaimed (additional method).
/// TODO(gitbuda): Articulate how to actually do/pass aggregations in the Multiframe context.
///
class Multiframe {
 public:
  Multiframe() = delete;
  Multiframe(const Multiframe &) = default;
  Multiframe(Multiframe &&) = default;
  Multiframe &operator=(const Multiframe &) = default;
  Multiframe &operator=(Multiframe &&) = default;
  explicit Multiframe(const size_t capacity = 0) : data_(0) { data_.reserve(capacity); }
  ~Multiframe() = default;

  size_t Size() const { return data_.size(); }
  void Put(const Frame &frame) { data_.push_back(frame); }
  const std::vector<Frame> &Data() const { return data_; }

 private:
  std::vector<Frame> data_;
};

/// Preallocated memory for operators' results (data / data pool).
///
struct MultiframePool {};

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
  PhysicalOperator() = delete;
  PhysicalOperator(const PhysicalOperator &) = default;
  PhysicalOperator(PhysicalOperator &&) = default;
  PhysicalOperator &operator=(const PhysicalOperator &) = default;
  PhysicalOperator &operator=(PhysicalOperator &&) = default;
  explicit PhysicalOperator(const std::string &name) : name_(name) {}
  virtual ~PhysicalOperator() {}

  void AddChild(std::shared_ptr<PhysicalOperator> child) { children_.push_back(child); }
  std::string Name() const { return name_; }

  /// Init/start execution.
  virtual void Execute(ExecutionContext ctx) = 0;
  /// Get data from children operators.
  virtual const Multiframe *NextRead() = 0;
  virtual const Multiframe *NextWrite() = 0;
  virtual void Reclaim() = 0;
  /// Prepare data for the next call.
  template <typename TTuple>
  void Emit(const TTuple &tuple) {
    // TODO(gitbuda): Implement correct Emit logic.
    //   * Correct frame handling (probably no need to copy).
    //   * Correct memory management (don't resize multiframe)..
    //   * Correct multi-threading.
    std::cout << "An Emit call" << std::endl;
    data_.Put(Frame{.a = tuple});
  }
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
  Multiframe data_{72};

  void BaseExecute(ExecutionContext ctx) {
    for (const auto &child : children_) {
      child->Execute(ctx);
    }
  }

  const Multiframe *BaseNext() const {
    // TODO(gitbuda): Implement correct Next logic.
    return &data_;
  }
};

// TODO(gitbuda): Consider kicking out Once because it's an artefact from the single Frame execution.
class OncePhysicalOperator final : public PhysicalOperator {
 public:
  explicit OncePhysicalOperator(const std::string &name) : PhysicalOperator(name), multiframe_(72) {
    multiframe_.Put(Frame{.a = 0});
  }
  void Execute(ExecutionContext ctx) override {
    MG_ASSERT(children_.empty(), "Once should have 0 inputs");
    std::cout << name_ << std::endl;
    PhysicalOperator::BaseExecute(ctx);
  }
  const Multiframe *NextRead() override {
    if (!executed_) {
      executed_ = true;
      return &multiframe_;
    }
    return nullptr;
  }
  const Multiframe *NextWrite() override { return nullptr; }
  void Reclaim() override {}

 private:
  bool executed_{false};
  Multiframe multiframe_;
  // Once has to return 1 empty frame on Next.
  // TODO(gitbuda): An issue because data also exists in the base class.
  // Multiframe data_{{0, 0}};
  // NOTE: Once is very similar to aggregate because only a single value has to be "processed".
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
      const auto *multiframe = input->NextRead();
      if (multiframe == nullptr) {
        break;
      }
      // TODO(gitbuda): Replace frame w/ multiframe.
      for (const auto &frame : multiframe->Data()) {
        for (const auto &tuple : data_fun_(Evaluate(frame))) {
          Emit(tuple);
        }
      }
    }
  }

  const Multiframe *NextRead() override {
    if (!next_called_) {
      next_called_ = true;
      return &data_;
    }
    return nullptr;
  }
  const Multiframe *NextWrite() override { return nullptr; }
  void Reclaim() override {}

 private:
  template <typename T>
  T Evaluate(T t) {
    return t;
  }
  TDataFun data_fun_;
  bool next_called_{false};
};

class ProducePhysicalOperator final : public PhysicalOperator {
 public:
  using PhysicalOperator::PhysicalOperator;
  void Execute(ExecutionContext ctx) override {
    std::cout << name_ << std::endl;
    MG_ASSERT(children_.size() == 1, "ScanAll should have exactly 1 input");
    PhysicalOperator::BaseExecute(ctx);

    auto *input = children_[0].get();
    while (true) {
      const auto *multiframe = input->NextRead();
      if (multiframe == nullptr) {
        break;
      }
      // TODO(gitbuda): Replace frame w/ multiframe.
      for (const auto &frame : multiframe->Data()) {
        std::cout << "Produce: " << frame.a << std::endl;
      }
    }
  }
  const Multiframe *NextRead() override { return BaseNext(); }
  const Multiframe *NextWrite() override { return nullptr; }
  void Reclaim() override {}
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
    auto data_fun = [](Frame) { return std::vector<int>{0, 1, 2}; };
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
