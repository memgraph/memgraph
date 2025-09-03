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

#include "planner/core/union_find.hpp"

namespace memgraph::planner::core::detail {

auto ENodeBase::canonicalize(UnionFind &uf) const -> ENodeBase {
  if (children_.empty()) {
    // Leaf node with copy over disambiguator
    return ENodeBase{disambiguator_};
  }

  auto canonical_children = utils::small_vector<EClassId>{};
  canonical_children.reserve(children_.size());

  for (auto child : children_) {
    // NOTE: UnionFind is mutable, so we are using the path halving optimization here
    canonical_children.push_back(uf.Find(child));
  }
  // Non-leaf node
  return ENodeBase{std::move(canonical_children)};
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
