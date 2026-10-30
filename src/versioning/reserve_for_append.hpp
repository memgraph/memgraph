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

#include <algorithm>
#include <concepts>
#include <cstddef>

namespace memgraph::versioning {

// A container ReserveForAppend can grow: size/capacity reads plus a reserve that takes the new capacity.
template <typename C>
concept AppendReservable = requires(C &container, std::size_t n) {
  container.size();
  container.capacity();
  container.reserve(n);
};

// Guarantees `extra` push_backs will not reallocate. Grows geometrically: an exact-size reserve per append would
// reallocate and copy the whole vector every time (O(N^2)).
template <AppendReservable V>
void ReserveForAppend(V &v, std::size_t const extra) {
  constexpr std::size_t kMinCapacity = 8;
  std::size_t const needed = v.size() + extra;
  if (needed <= v.capacity()) return;
  v.reserve(std::max({needed, v.capacity() * 2, kMinCapacity}));
}

}  // namespace memgraph::versioning
