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

#include "planner/core/enode.hpp"

#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"
import memgraph.planner.core.union_find;

namespace memgraph::planner::core::detail {

auto ENodeBase::canonicalize(UnionFind &uf) const -> ENodeBase {
  // Check if canonicalization is actually needed (quick optimization)
  bool needs_canonicalization = false;
  for (auto child : children_) {
    if (canonical_eclass(uf, child) != child) {
      needs_canonicalization = true;
      break;
    }
  }

  if (!needs_canonicalization) {
    // Children are already canonical, just copy the existing
    return *this;
  }

  // Only create new vector if canonicalization is actually needed
  auto canonical_children = utils::small_vector<EClassId>{};
  canonical_children.reserve(children_.size());

  for (const auto child : children_) {
    // NOTE: UnionFind is mutable, so we are using the path halving optimization here
    canonical_children.emplace_back(canonical_eclass(uf, child));
  }
  return ENodeBase{disambiguator_, std::move(canonical_children)};
}

auto ENodeBase::canonicalize_in_place(UnionFind &uf) -> bool {
  if (children_.empty()) {
    // Leaf nodes are always canonical (disambiguator doesn't change)
    return false;
  }

  bool changed = false;
  for (auto &child : children_) {
    auto canonical_child = canonical_eclass(uf, child);
    if (canonical_child != child) {
      child = canonical_child;
      changed = true;
    }
  }
  return changed;
}

auto ENodeBase::canonicalize(UnionFind &uf, ENodeContext &ctx) const -> ENodeBase {
  if (children_.empty()) {
    return *this;
  }

  // Single pass: gather canonical children and check if canonicalization is needed
  auto &canonical_children = ctx.canonical_children_buffer;
  canonical_children.clear();
  canonical_children.reserve(children_.size());

  bool needs_canonicalization = false;
  for (auto child : children_) {
    auto canonical_child = canonical_eclass(uf, child);
    canonical_children.emplace_back(canonical_child);
    if (canonical_child != child) {
      needs_canonicalization = true;
    }
  }

  if (!needs_canonicalization) {
    // Children are already canonical, just copy the existing vector
    return *this;
  }

  // Move the canonical children from buffer (they've been computed in the single pass)
  return ENodeBase{disambiguator_, utils::small_vector<EClassId>(canonical_children.begin(), canonical_children.end())};
}

auto ENodeBase::compute_hash() const -> std::size_t {
  auto seed = std::size_t{};
  boost::hash_combine(seed, disambiguator_);
  for (auto const &child : children_) {
    boost::hash_combine(seed, child);
  }
  return seed;
}
}  // namespace memgraph::planner::core::detail
