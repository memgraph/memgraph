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

#include <vector>

#include "expr/semantic/symbol_table.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::expr {

class Frame {
 public:
  /// Create a Frame of given size backed by a utils::NewDeleteResource()
  explicit Frame(size_t size) : elems_(size, utils::NewDeleteResource()) { MG_ASSERT(size >= 0); }

  Frame(size_t size, utils::MemoryResource *memory) : elems_(size, memory) { MG_ASSERT(size >= 0); }

  TypedValue &operator[](const Symbol &symbol) { return elems_[symbol.position()]; }
  const TypedValue &operator[](const Symbol &symbol) const { return elems_[symbol.position()]; }

  TypedValue &at(const Symbol &symbol) { return elems_.at(symbol.position()); }
  const TypedValue &at(const Symbol &symbol) const { return elems_.at(symbol.position()); }

  auto &elems() { return elems_; }
  const auto &elems() const { return elems_; }

  utils::MemoryResource *GetMemoryResource() const { return elems_.get_allocator().GetMemoryResource(); }

 private:
  utils::pmr::vector<TypedValue> elems_;
};

class FrameWithValidity final : public Frame {
 public:
  explicit FrameWithValidity(size_t size) : Frame(size), is_valid_(false) {}

  FrameWithValidity(size_t size, utils::MemoryResource *memory) : Frame(size, memory), is_valid_(false) {}

  bool IsValid() const noexcept { return is_valid_; }
  void MakeValid() noexcept { is_valid_ = true; }
  void MakeInvalid() noexcept { is_valid_ = false; }

 private:
  bool is_valid_;
};

}  // namespace memgraph::expr
