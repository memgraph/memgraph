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

#include "query/relations/equality.hpp"

#include <algorithm>

namespace memgraph::query::relations::equality {

TypedValue EqualOfContainers(const TypedValue &a, const TypedValue &b) {
  switch (a.type()) {
    case TypedValue::Type::List: {
      // A list is equal only to a list of the same length holding equal
      // elements, so `2 = [2]` is false however deeply either side is nested.
      // Neo4j differs here, answering true for `2 = [2]` and for
      // `[[2]] = [[[[[[2]]]]]]`.
      const auto &list_a = a.ValueList();
      const auto &list_b = b.ValueList();
      if (list_a.size() != list_b.size()) return TypedValue(false, a.get_allocator());
      // Elements are compared by equivalence rather than by equality, which is
      // two-valued, so a list comparison never answers Null:
      //    [1] == [null] -> false
      //    [null] == [null] -> true
      return TypedValue(std::equal(list_a.begin(), list_a.end(), list_b.begin(), TypedValue::BoolEqual{}),
                        a.get_allocator());
    }
    case TypedValue::Type::Map: {
      const auto &map_a = a.ValueMap();
      const auto &map_b = b.ValueMap();
      if (map_a.size() != map_b.size()) return TypedValue(false, a.get_allocator());
      for (const auto &kv_a : map_a) {
        auto found_b_it = map_b.find(kv_a.first);
        if (found_b_it == map_b.end()) return TypedValue(false, a.get_allocator());
        TypedValue comparison = Equal(kv_a.second, found_b_it->second);
        if (comparison.IsNull() || !comparison.ValueBool()) return TypedValue(false, a.get_allocator());
      }
      return TypedValue(true, a.get_allocator());
    }
    default:
      LOG_FATAL("Unhandled container comparison");
  }
}

}  // namespace memgraph::query::relations::equality
