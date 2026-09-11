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

#include <algorithm>

namespace memgraph::query::relations::equivalence {

bool EquivalentOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b) {
  return a.size() == b.size() &&
         std::equal(
             a.begin(), a.end(), b.begin(), [](TypedValue const &x, TypedValue const &y) { return Equivalent(x, y); });
}

bool EquivalentOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b) {
  if (a.size() != b.size()) return false;
  return std::ranges::all_of(a, [&b](auto const &entry) {
    auto const found = b.find(entry.first);
    return found != b.end() && Equivalent(entry.second, found->second);
  });
}

bool EquivalentOfContainersHoldingANull(const TypedValue &a, const TypedValue &b) {
  DMG_ASSERT(a.type() == b.type(), "Equality answers Null only over a pair holding the same type");
  switch (a.type()) {
    case TypedValue::Type::List:
      return EquivalentOfLists(a.UnsafeValueList(), b.UnsafeValueList());
    case TypedValue::Type::Map:
      return EquivalentOfMaps(a.UnsafeValueMap(), b.UnsafeValueMap());
    default:
      LOG_FATAL("Equality answered Null for a pair holding no Null");
  }
}

}  // namespace memgraph::query::relations::equivalence
