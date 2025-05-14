// Copyright 2025 Memgraph Ltd.
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

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/typed_value.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

class Frame {
 public:
  using allocator_type = utils::Allocator<TypedValue>;

  /// Create a Frame of given size backed by a utils::NewDeleteResource()
  explicit Frame(int64_t size) : elems_(size, utils::NewDeleteResource()) { MG_ASSERT(size >= 0); }

  Frame(int64_t size, allocator_type alloc) : elems_(size, alloc) { MG_ASSERT(size >= 0); }

  TypedValue &operator[](const Symbol &symbol) { return elems_[symbol.position()]; }
  const TypedValue &operator[](const Symbol &symbol) const { return elems_[symbol.position()]; }

  TypedValue &at(const Symbol &symbol) { return elems_.at(symbol.position()); }
  const TypedValue &at(const Symbol &symbol) const { return elems_.at(symbol.position()); }

  auto &elems() { return elems_; }

  auto get_allocator() const -> allocator_type { return elems_.get_allocator(); }

 private:
  utils::pmr::vector<TypedValue> elems_;
};

}  // namespace memgraph::query
