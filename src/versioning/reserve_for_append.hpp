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
#include <cstddef>

namespace memgraph::versioning {

// Guarantees `extra` push_backs will not reallocate. Grows geometrically: an exact-size reserve per append would
// reallocate and copy the whole vector every time (O(N^2)).
template <class V>
void ReserveForAppend(V &v, std::size_t const extra) {
  constexpr std::size_t kMinCapacity = 8;
  std::size_t const needed = v.size() + extra;
  if (needed <= v.capacity()) return;
  v.reserve(std::max({needed, v.capacity() * 2, kMinCapacity}));
}

}  // namespace memgraph::versioning
