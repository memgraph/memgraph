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

bool HoldsANull(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return true;
    case TypedValue::Type::List:
      return std::ranges::any_of(value.UnsafeValueList(), [](auto const &element) { return HoldsANull(element); });
    case TypedValue::Type::Map:
      return std::ranges::any_of(value.UnsafeValueMap(), [](auto const &entry) { return HoldsANull(entry.second); });
    default:
      return false;
  }
}

bool HoldsANull(const storage::PropertyValue &value) {
  switch (value.type()) {
    case storage::PropertyValueType::Null:
      return true;
    case storage::PropertyValueType::List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return HoldsANull(element); });
    case storage::PropertyValueType::Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &entry) { return HoldsANull(entry.second); });
    default:
      // The packed numeric representations of a list have no way to hold a Null.
      return false;
  }
}

TypedValue EqualOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b, TypedValue::allocator_type alloc) {
  // A list is equal only to a list of the same length holding equal elements,
  // so `2 = [2]` is false however deeply either side is nested. Neo4j differs
  // here, answering true for `2 = [2]` and for `[[2]] = [[[[[[2]]]]]]`.
  if (a.size() != b.size()) return TypedValue(false, alloc);

  // A part that differs settles the whole, whatever the rest holds: `[null, 1]`
  // and `[null, 2]` differ however the first element turns out. So a difference
  // answers at once, and an undecided part survives only to the end.
  auto undecided = false;
  for (size_t i = 0; i != a.size(); ++i) {
    auto const element = Equal(a[i], b[i]);
    if (element.IsNull()) {
      undecided = true;
    } else if (!element.UnsafeValueBool()) {
      return TypedValue(false, alloc);
    }
  }
  return undecided ? TypedValue(alloc) : TypedValue(true, alloc);
}

TypedValue EqualOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b, TypedValue::allocator_type alloc) {
  if (a.size() != b.size()) return TypedValue(false, alloc);

  auto undecided = false;
  for (auto const &[key, value_a] : a) {
    // Which keys a map holds is known, so a key the other side lacks settles it
    // however the values would have compared.
    auto const found = b.find(key);
    if (found == b.end()) return TypedValue(false, alloc);
    auto const value = Equal(value_a, found->second);
    if (value.IsNull()) {
      undecided = true;
    } else if (!value.UnsafeValueBool()) {
      return TypedValue(false, alloc);
    }
  }
  return undecided ? TypedValue(alloc) : TypedValue(true, alloc);
}

}  // namespace memgraph::query::relations::equality
