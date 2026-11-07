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

#include <algorithm>
#include <cstddef>

#include "absl/container/inlined_vector.h"
#include "storage/v2/edge_ref.hpp"

namespace memgraph::versioning {

// The fork edges of one expansion that the branch deleted, as the EdgeRefs the fork's adjacency holds: a handful is
// searched linearly, a hub's many are sorted once.
class HiddenForkEdges {
 public:
  bool empty() const noexcept { return refs_.empty(); }

  size_t size() const noexcept { return refs_.size(); }

  // Call before the Adds; `count` is an upper bound on how many follow.
  void Reserve(size_t count) { refs_.reserve(count); }

  void Add(storage::EdgeRef ref) { refs_.push_back(ref); }

  // After the last Add.
  void Seal() {
    if (refs_.size() <= kLinearMax) return;
    std::ranges::sort(refs_);
    sorted_ = true;
  }

  bool Contains(storage::EdgeRef ref) const noexcept {
    if (sorted_) return std::ranges::binary_search(refs_, ref);
    return std::ranges::find(refs_, ref) != refs_.end();
  }

 private:
  static constexpr size_t kLinearMax = 8;

  absl::InlinedVector<storage::EdgeRef, kLinearMax> refs_;
  bool sorted_{false};
};

}  // namespace memgraph::versioning
