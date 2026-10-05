// Copyright 2026 Memgraph Ltd.
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

#include <vector>

#include "plan/operator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/typed_value.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

class Frame;
class FrameChangeCollector;

struct FrameWriter {
  FrameWriter(Frame &frame, FrameChangeCollector *change_collector, utils::MemoryResource *res)
      : frame_(frame), frame_change_collector_{change_collector}, res_(res) {}

  template <typename T>
    requires(!std::is_same_v<std::remove_cv_t<T>, TypedValue>)
  auto Write(const Symbol &symbol, T &&value) -> TypedValue & {
    return Write(symbol, TypedValue(std::forward<T>(value), res_));
  }

  template <typename T>
    requires(!std::is_same_v<std::remove_cv_t<T>, TypedValue>)
  auto WriteAt(const Symbol &symbol, T &&value) -> TypedValue & {
    return WriteAt(symbol, TypedValue(std::forward<T>(value), res_));
  }

  auto Write(const Symbol &symbol, TypedValue &&value) -> TypedValue &;
  auto WriteAt(const Symbol &symbol, TypedValue &&value) -> TypedValue &;

  template <typename Func>
  auto Modify(const Symbol &symbol, Func f) -> std::invoke_result_t<Func, TypedValue &>;

 private:
  void ResetCache(const Symbol &symbol);

  Frame &frame_;
  FrameChangeCollector *frame_change_collector_;
  utils::MemoryResource *res_;
};

class Frame {
 public:
  using allocator_type = utils::Allocator<TypedValue>;

  /// Create a Frame of given size backed by a utils::NewDeleteResource()
  explicit Frame(int64_t size) : elems_(size, utils::NewDeleteResource()), eval_scratch_(utils::NewDeleteResource()) {
    MG_ASSERT(size >= 0);
  }

  Frame(int64_t size, allocator_type alloc) : elems_(size, alloc), eval_scratch_(alloc) { MG_ASSERT(size >= 0); }

  /// Where evaluating an expression leaves a node's value. Each node owns one,
  /// so the value's storage is reused from row to row rather than built and
  /// torn down for every row. A frame belongs to one execution, so the slots
  /// are not shared across threads.
  ///
  /// Grows on demand, which moves what it holds: a reference taken from here
  /// must not be held across anything that could evaluate another node.
  auto EvalSlot(uint32_t index) -> TypedValue & {
    if (index >= eval_scratch_.size()) eval_scratch_.resize(index + 1);
    return eval_scratch_[index];
  }

  const TypedValue &operator[](const Symbol &symbol) const {
    DebugAssertInBounds(symbol);
    return elems_[symbol.position()];
  }

  const TypedValue &at(const Symbol &symbol) const { return elems_.at(symbol.position()); }

  auto elems() const -> const utils::pmr::vector<TypedValue> & { return elems_; }

  auto get_allocator() const -> allocator_type { return elems_.get_allocator(); }

  auto GetFrameWriter(FrameChangeCollector *change_collector, utils::MemoryResource *res) -> FrameWriter {
    return FrameWriter{*this, change_collector, res};
  }

 private:
  friend struct FrameWriter;

  // Unchecked access is the hot path, so only debug builds pay for this. A position outside the frame is a bug in
  // whatever built the plan, e.g. a default Symbol, whose position is -1.
  void DebugAssertInBounds([[maybe_unused]] const Symbol &symbol) const {
    DMG_ASSERT(symbol.position() >= 0 && static_cast<size_t>(symbol.position()) < elems_.size(),
               "Symbol '{}' has frame position {} outside a frame of size {}",
               symbol.name(),
               symbol.position(),
               elems_.size());
  }

  utils::pmr::vector<TypedValue> elems_;
  utils::pmr::vector<TypedValue> eval_scratch_;
};

template <typename Func>
auto FrameWriter::Modify(const Symbol &symbol, Func f) -> std::invoke_result_t<Func, TypedValue &> {
  frame_.DebugAssertInBounds(symbol);
  auto &value = frame_.elems_[symbol.position()];
  ResetCache(symbol);
  return f(value);
}

}  // namespace memgraph::query
