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

#include <memory>
#include <vector>

#include <range/v3/algorithm/all_of.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>

#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

/// Drops entries from `all_indices` that nothing but `all_indices` still holds.
///
/// `refcount_of` maps an entry to the use count of the shared_ptr it carries; entries differ per
/// index type (a struct member, a pair's second, or the pointer itself), the reaping rule does not.
///
/// A shared_ptr held anywhere else keeps its entry in the list until that holder releases it.
/// An index must appear in `all_indices` at most once, or the duplicate is never reapable.
template <typename Entry, typename Proj>
void CleanupAllIndices(
    utils::Synchronized<std::shared_ptr<std::vector<Entry> const>, utils::WritePrioritizedRWLock> &all_indices,
    Proj refcount_of) {
  all_indices.WithLock([&](std::shared_ptr<std::vector<Entry> const> &indices) {
    auto keep_condition = [&](Entry const &entry) { return refcount_of(entry) != 1; };
    if (!ranges::all_of(*indices, keep_condition)) {
      indices =
          std::make_shared<std::vector<Entry>>(*indices | ranges::views::filter(keep_condition) | ranges::to_vector);
    }
  });
}

}  // namespace memgraph::storage
