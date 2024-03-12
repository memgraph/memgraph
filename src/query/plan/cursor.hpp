// Copyright 2024 Memgraph Ltd.
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

#include <functional>
#include <memory>

#include "utils/memory.hpp"

namespace memgraph::query {

struct ExecutionContext;
class Frame;

namespace plan {

/// Base class for iteration cursors of @c LogicalOperator classes.
///
/// Each @c LogicalOperator must produce a concrete @c Cursor, which provides
/// the iteration mechanism.
class Cursor {
 public:
  Cursor() = default;
  Cursor(const Cursor &) = delete;
  Cursor(Cursor &&) = delete;
  Cursor &operator=(const Cursor &) = delete;
  Cursor &operator=(Cursor &&) = delete;
  virtual ~Cursor() = default;

  /// Run an iteration of a @c LogicalOperator.
  ///
  /// Since operators may be chained, the iteration may pull results from
  /// multiple operators.
  ///
  /// @param Frame May be read from or written to while performing the
  ///     iteration.
  /// @param ExecutionContext Used to get the position of symbols in frame and
  ///     other information.
  ///
  /// @throws QueryRuntimeException if something went wrong with execution
  virtual bool Pull(Frame &, ExecutionContext &) = 0;

  /// Resets the Cursor to its initial state.
  virtual void Reset() = 0;

  /// Perform cleanup which may throw an exception
  virtual void Shutdown() = 0;
};

/// unique_ptr to Cursor managed with a custom deleter.
/// This allows us to use utils::MemoryResource for allocation.
using UniqueCursorPtr = std::unique_ptr<Cursor, std::function<void(Cursor *)>>;

template <class TCursor, class... TArgs>
std::unique_ptr<Cursor, std::function<void(Cursor *)>> MakeUniqueCursorPtr(utils::Allocator<TCursor> allocator,
                                                                           TArgs &&...args) {
  auto *ptr = allocator.allocate(1);
  try {
    auto *cursor = new (ptr) TCursor(std::forward<TArgs>(args)...);
    return std::unique_ptr<Cursor, std::function<void(Cursor *)>>(cursor, [allocator](Cursor *base_ptr) mutable {
      auto *p = static_cast<TCursor *>(base_ptr);
      p->~TCursor();
      allocator.deallocate(p, 1);
    });
  } catch (...) {
    allocator.deallocate(ptr, 1);
    throw;
  }
}
}  // namespace plan
}  // namespace memgraph::query
