// Copyright 2025 Memgraph Ltd.
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

#include "planner/core/processing_context.hpp"
#include "planner/core/union_find.hpp"

namespace memgraph::planner::core::detail {

auto ENodeBase::canonicalize(UnionFind &uf) const -> ENodeBase {
  if (children_.empty()) {
    // Leaf node with copy over disambiguator
    return ENodeBase{disambiguator_};
  }

  // Check if canonicalization is actually needed (quick optimization)
  bool needs_canonicalization = false;
  for (auto child : children_) {
    if (uf.Find(child) != child) {
      needs_canonicalization = true;
      break;
    }
  }

  if (!needs_canonicalization) {
    // Children are already canonical, just copy the existing vector
    return ENodeBase{children_};
  }

  // Only create new vector if canonicalization is actually needed
  auto canonical_children = utils::small_vector<EClassId>{};
  canonical_children.reserve(children_.size());

  for (auto child : children_) {
    // NOTE: UnionFind is mutable, so we are using the path halving optimization here
    canonical_children.emplace_back(uf.Find(child));
  }
  // Non-leaf node
  return ENodeBase{std::move(canonical_children)};
}

auto ENodeBase::canonicalize_in_place(UnionFind &uf) -> bool {
  if (children_.empty()) {
    // Leaf nodes are always canonical (disambiguator doesn't change)
    return false;
  }

  bool changed = false;
  for (auto &child : children_) {
    auto canonical_child = uf.Find(child);
    if (canonical_child != child) {
      child = canonical_child;
      changed = true;
    }
  }
  return changed;
}

auto ENodeBase::canonicalize(UnionFind &uf, BaseProcessingContext &ctx) const -> ENodeBase {
  if (children_.empty()) {
    // Leaf node with copy over disambiguator
    return ENodeBase{disambiguator_};
  }

  // Single pass: gather canonical children and check if canonicalization is needed
  auto &canonical_children = ctx.canonical_children_buffer;
  canonical_children.clear();
  canonical_children.reserve(children_.size());

  bool needs_canonicalization = false;
  for (auto child : children_) {
    auto canonical_child = uf.Find(child);
    canonical_children.emplace_back(canonical_child);
    if (canonical_child != child) {
      needs_canonicalization = true;
    }
  }

  if (!needs_canonicalization) {
    // Children are already canonical, just copy the existing vector
    return ENodeBase{children_};
  }

  // Move the canonical children from buffer (they've been computed in the single pass)
  return ENodeBase{utils::small_vector<EClassId>(canonical_children.begin(), canonical_children.end())};
}

auto ENodeBase::compute_hash() const -> std::size_t {
  auto seed = std::size_t{};

  if (children_.empty()) {
    // leaf
    boost::hash_combine(seed, disambiguator_);
  } else {
    // non-leaf
    for (size_t i = 0; i < children_.size(); ++i) {
      boost::hash_combine(seed, children_[i]);
    }
  }
  return seed;
}
}  // namespace memgraph::planner::core::detail
