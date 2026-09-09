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
#include <cstddef>
#include <ranges>

namespace memgraph::query::relations::equality {

namespace {

/// Folds the pairwise comparisons of two containers already known to share a shape.
///
/// A pair that is decided unequal decides the whole container, so it wins over any Null seen before or
/// after it. Only once every pair has been looked at does an undecided one leave the answer Null. A
/// Null therefore cannot short-circuit, which is the whole reason this is a fold and not a
/// `std::equal`.
class PairwiseEquality {
 public:
  /// @return false once the answer is settled and the remaining pairs cannot change it.
  bool Fold(const TypedValue &a, const TypedValue &b) {
    TypedValue const comparison = Equal(a, b);
    if (comparison.IsNull()) {
      undecided_ = true;
      return true;
    }
    if (!comparison.ValueBool()) {
      unequal_ = true;
      return false;
    }
    return true;
  }

  TypedValue Answer(TypedValue::allocator_type alloc) const {
    if (unequal_) return TypedValue(false, alloc);
    if (undecided_) return TypedValue(alloc);
    return TypedValue(true, alloc);
  }

 private:
  bool unequal_{false};
  bool undecided_{false};
};

}  // namespace

TypedValue EqualOfContainers(const TypedValue &a, const TypedValue &b) {
  switch (a.type()) {
    case TypedValue::Type::List: {
      // A list is equal only to a list of the same length holding equal elements, so `2 = [2]` is
      // false however deeply either side is nested.
      const auto &list_a = a.ValueList();
      const auto &list_b = b.ValueList();
      // A length that differs decides the two unequal, whatever the elements are.
      if (list_a.size() != list_b.size()) return TypedValue(false, a.get_allocator());
      // Elements are compared by equality, which reaches this relation again and is three-valued, so
      // a Null element leaves the comparison undecided rather than answering it:
      //    [1] = [null]    -> Null
      //    [null] = [null] -> Null
      //    [1, null] = [2, null] -> false, decided by the pair that differs
      PairwiseEquality equality;
      for (size_t i = 0; i != list_a.size(); ++i) {
        if (!equality.Fold(list_a[i], list_b[i])) break;
      }
      return equality.Answer(a.get_allocator());
    }
    case TypedValue::Type::Map: {
      const auto &map_a = a.ValueMap();
      const auto &map_b = b.ValueMap();
      // Keys that differ decide the two unequal, including keys holding a Null. Equal sizes plus every
      // key of `a` found in `b` is the two holding the same key set.
      if (map_a.size() != map_b.size()) return TypedValue(false, a.get_allocator());
      PairwiseEquality equality;
      for (const auto &kv_a : map_a) {
        auto found_b_it = map_b.find(kv_a.first);
        if (found_b_it == map_b.end()) return TypedValue(false, a.get_allocator());
        if (!equality.Fold(kv_a.second, found_b_it->second)) break;
      }
      return equality.Answer(a.get_allocator());
    }
    default:
      LOG_FATAL("Unhandled container comparison");
  }
}

bool DecidedByEquality(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return false;
    case TypedValue::Type::List:
      return std::ranges::all_of(value.ValueList(), DecidedByEquality);
    case TypedValue::Type::Map:
      return std::ranges::all_of(value.ValueMap(), [](auto const &kv) { return DecidedByEquality(kv.second); });
    default:
      return true;
  }
}

}  // namespace memgraph::query::relations::equality
