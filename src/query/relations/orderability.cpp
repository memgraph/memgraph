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

std::partial_ordering CompareOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b) {
  return std::lexicographical_compare_three_way(
      a.begin(), a.end(), b.begin(), b.end(), [](TypedValue const &x, TypedValue const &y) { return Compare(x, y); });
}

}  // namespace memgraph::query::relations::orderability
