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

#include "query/relations/orderability.hpp"

#include <algorithm>

namespace memgraph::query::relations::orderability {

std::partial_ordering CompareOfLists(TypedValue const &a, TypedValue const &b) {
  auto const &list_a = a.UnsafeValueList();
  auto const &list_b = b.UnsafeValueList();
  return std::lexicographical_compare_three_way(
      list_a.begin(), list_a.end(), list_b.begin(), list_b.end(), [](TypedValue const &x, TypedValue const &y) {
        return Compare(x, y);
      });
}

}  // namespace memgraph::query::relations::orderability
