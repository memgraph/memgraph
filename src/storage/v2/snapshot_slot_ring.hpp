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
#include <limits>
#include <memory>

#include "storage/v2/transaction_constants.hpp"

namespace memgraph::storage {

// Maps active SI start_ts -> frozen snapshot_ts so GC gets min(active snapshot_ts) without walking txns.
// Publish() MUST run in the same engine_lock hold that minted start_ts and read snapshot_ts, else GC can over-reclaim.
class SnapshotSlotRing {
 public:
  // NOLINTNEXTLINE(modernize-avoid-c-arrays) — heap array by design: std::array would place ~1 MiB inline.
  SnapshotSlotRing() : slots_(std::make_unique<Slot[]>(kSlots)) {}

  SnapshotSlotRing(SnapshotSlotRing const &) = delete;
  SnapshotSlotRing &operator=(SnapshotSlotRing const &) = delete;
  SnapshotSlotRing(SnapshotSlotRing &&) = delete;
  SnapshotSlotRing &operator=(SnapshotSlotRing &&) = delete;

  // Invalidate-first: tag=kEmpty precedes the release store of snap, so a reader that acquires the new snap
  // sees kEmpty or the new start_ts, never the stale owner. tag is stored last as the commit point.
  void Publish(uint64_t start_ts, uint64_t snapshot_ts) noexcept {
    auto &slot = slots_[start_ts % kSlots];
    slot.tag.store(kEmpty, std::memory_order_relaxed);
    slot.snap.store(snapshot_ts, std::memory_order_release);
    slot.tag.store(start_ts, std::memory_order_release);
  }

  // Returns the oldest active txn's snapshot_ts on a tag match (advancing the monotone floor), else the floor.
  uint64_t VisibilityHorizon(uint64_t raw_oldest_active, bool no_active_txns) noexcept {
    auto const &slot = slots_[raw_oldest_active % kSlots];
    uint64_t const snap = slot.snap.load(std::memory_order_acquire);
    uint64_t const tag = slot.tag.load(std::memory_order_acquire);
    if (tag == raw_oldest_active) {
      uint64_t cur = floor_.load(std::memory_order_acquire);
      while (snap > cur &&
             !floor_.compare_exchange_weak(cur, snap, std::memory_order_release, std::memory_order_acquire)) {
      }
      return std::max(snap, cur);
    }
    // Not `raw > last_committed` for "no active": a leapfrogged reader has raw < last_committed but is live.
    if (no_active_txns) return raw_oldest_active;
    return floor_.load(std::memory_order_acquire);
  }

  // Must be called quiescent (Clear / recovery): no concurrent GC or transaction threads.
  void Reset() noexcept {
    floor_.store(kTimestampInitialId, std::memory_order_release);
    for (size_t i = 0; i < kSlots; ++i) {
      slots_[i].tag.store(kEmpty, std::memory_order_relaxed);
      slots_[i].snap.store(0, std::memory_order_relaxed);
    }
  }

 private:
  static constexpr uint64_t kEmpty = std::numeric_limits<uint64_t>::max();

  // Correctness is independent of size (slot access is tag-validated); larger only reduces collisions.
  static constexpr size_t kSlots = 1ULL << 16;

  struct Slot {
    std::atomic<uint64_t> tag{kEmpty};  // owning start_timestamp; kEmpty = unoccupied
    std::atomic<uint64_t> snap{0};
  };

  // NOLINTNEXTLINE(modernize-avoid-c-arrays) — heap array by design: std::array would place ~1 MiB inline.
  std::unique_ptr<Slot[]> slots_;
  std::atomic<uint64_t> floor_{kTimestampInitialId};
};

}  // namespace memgraph::storage
