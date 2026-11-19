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
#include <atomic>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <span>
#include <vector>

#include "utils/spin_lock.hpp"

namespace memgraph::storage {

/// An index entry sorts by its batch start; the 2^16 values above it are that batch's stamps.
inline constexpr uint64_t kIndexStampBits = 16;

inline constexpr uint64_t kIndexStampMask = (uint64_t{1} << kIndexStampBits) - 1;
/// A timestamp from here on loses its high bits in IndexTimestamp, so no transaction may start at one.
inline constexpr uint64_t kMaxIndexedTimestamp = uint64_t{1} << (64 - kIndexStampBits);

constexpr uint64_t IndexTimestamp(uint64_t timestamp) { return timestamp << kIndexStampBits; }

/// A batch that used up its stamps goes on in the span of a fresh timestamp. This maps each such span to the batch
/// start that owns it, so a scan compares those entries by their owner. One record per 2^16 stamps, never pruned.
class StampSpanOwners {
 public:
  /// `span` is a timestamp no transaction starts at.
  void Add(uint64_t span, uint64_t owner) {
    auto const guard = std::lock_guard{lock_};
    records_.insert(std::ranges::upper_bound(records_, span, {}, &Record::span), Record{.span = span, .owner = owner});
    newest_.store(std::max(newest_.load(std::memory_order_relaxed), span), std::memory_order_release);
  }

  /// The batch start that owns the span of the index timestamp `timestamp`.
  uint64_t Owner(uint64_t timestamp) const {
    auto const span = timestamp >> kIndexStampBits;
    if (span > newest_.load(std::memory_order_acquire)) return span;
    auto const guard = std::lock_guard{lock_};
    auto const it = std::ranges::lower_bound(records_, span, {}, &Record::span);
    return it != records_.end() && it->span == span ? it->owner : span;
  }

  /// For a clock rewind: a recorded span may become a transaction's start.
  void Clear() {
    auto const guard = std::lock_guard{lock_};
    records_.clear();
    newest_.store(0, std::memory_order_release);
  }

 private:
  struct Record {
    uint64_t span;
    uint64_t owner;
  };

  mutable utils::SpinLock lock_;
  std::vector<Record> records_;
  std::atomic<uint64_t> newest_{0};
};

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
