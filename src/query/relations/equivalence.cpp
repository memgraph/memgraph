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

#include "query/relations/equivalence.hpp"

#include <cstddef>

namespace memgraph::query::relations::equivalence {

bool EquivalentOfContainers(const TypedValue &lhs, const TypedValue &rhs) {
  if (lhs.type() == TypedValue::Type::List) {
    const auto &list_lhs = lhs.ValueList();
    const auto &list_rhs = rhs.ValueList();
    if (list_lhs.size() != list_rhs.size()) return false;
    for (size_t i = 0; i != list_lhs.size(); ++i) {
      if (!Equivalent(list_lhs[i], list_rhs[i])) return false;
    }
    return true;
  }

  const auto &map_lhs = lhs.ValueMap();
  const auto &map_rhs = rhs.ValueMap();
  if (map_lhs.size() != map_rhs.size()) return false;
  for (const auto &kv_lhs : map_lhs) {
    auto found_rhs_it = map_rhs.find(kv_lhs.first);
    if (found_rhs_it == map_rhs.end()) return false;
    if (!Equivalent(kv_lhs.second, found_rhs_it->second)) return false;
  }
  return true;
}

}  // namespace memgraph::query::relations::equivalence
