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
#include <cstdint>
#include <iterator>
#include <span>

namespace memgraph::storage {

/// The timestamps an aborting batch wrote its index entries at: its start, and the stamps raised since, ascending.
struct BatchTimestamps {
  uint64_t start;
  std::span<uint64_t const> stamps;

  uint64_t Last() const { return stamps.empty() ? start : stamps.back(); }

  bool Contains(uint64_t timestamp) const {
    return timestamp == start || std::ranges::binary_search(stamps, timestamp);
  }
};

/// Removes every entry the batch wrote for one key; `entry_at(ts)` is that key's entry at `ts`. Entries order by
/// timestamp after the key, so one walk from the start to the last stamp finds them all.
template <typename TAccessor, typename TEntryAt>
void RemoveBatchEntries(TAccessor &acc, BatchTimestamps const &batch, TEntryAt const &entry_at) {
  auto last = entry_at(batch.Last());
  for (auto it = acc.find_equal_or_greater(entry_at(batch.start)); it != acc.end() && !(last < *it);) {
    auto const next = std::next(it);
    if (batch.Contains(it->timestamp)) acc.remove(*it);
    it = next;
  }
}

}  // namespace memgraph::storage
