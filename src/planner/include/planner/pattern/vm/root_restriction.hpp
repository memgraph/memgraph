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

#include <cassert>
#include <type_traits>

#include <boost/unordered/unordered_flat_set.hpp>

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::pattern::vm {

/// How a matcher run iterates its root-symbol candidates: either MatchAll (every
/// candidate of the root symbol) or RestrictTo a set of e-classes (only those).
///
/// A RestrictTo over an *empty* set restricts to nothing - a distinct, meaningful
/// state (a settled sparse graph), not "match all". That is the confusion this
/// type removes: a raw `set const *` conflated "no restriction" (nullptr) with
/// "restrict to nothing" (non-null empty). Here the two are named states and the
/// empty case is simply `RestrictTo({})`, handled like any other set.
///
/// A pattern whose root has no symbol iterates every e-class regardless, so it
/// ignores the restriction entirely - the executor consults it only while
/// iterating a symbol root. Zero-overhead: one pointer, trivially copyable.
class RootRestriction {
 public:
  using Set = boost::unordered_flat_set<EClassId>;

  /// Default is MatchAll.
  constexpr RootRestriction() = default;

  [[nodiscard]] static constexpr auto MatchAll() -> RootRestriction { return RootRestriction{}; }

  [[nodiscard]] static constexpr auto RestrictTo(Set const &roots) -> RootRestriction {
    return RootRestriction{&roots};
  }

  [[nodiscard]] constexpr auto matches_all() const -> bool { return roots_ == nullptr; }

  /// The candidate roots to iterate. Precondition: !matches_all().
  [[nodiscard]] constexpr auto restricted_roots() const -> Set const & {
    assert(!matches_all() && "restricted_roots() on a MatchAll restriction");
    return *roots_;
  }

 private:
  explicit constexpr RootRestriction(Set const *roots) : roots_(roots) {}

  Set const *roots_ = nullptr;
};

static_assert(std::is_trivially_copyable_v<RootRestriction>);

}  // namespace memgraph::planner::core::pattern::vm
