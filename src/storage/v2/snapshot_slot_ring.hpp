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

// SnapshotSlotRing — tag-validated ring + monotone non-regressing floor.
//
// Maps each active transaction's start_timestamp → its frozen snapshot_ts so that
// CollectGarbage can compute min(active snapshot_ts) — the true GC visibility horizon
// when commit-lock narrowing is ON — without walking all live transactions.
//
// PRECONDITION: Publish() MUST be called under the SAME engine_lock hold that minted
// start_ts and read snapshot_ts.  The class cannot enforce this; the caller must.
// Splitting the mint and the Publish across two engine_lock holds allows the
// lowest-slot transaction to publish last with a higher watermark, which lets GC
// over-reclaim a version a live reader still needs (use-after-free).
class SnapshotSlotRing {
 public:
  // NOLINTNEXTLINE(modernize-avoid-c-arrays) — heap array by design: std::array would place ~1 MiB inline.
  SnapshotSlotRing() : slots_(std::make_unique<Slot[]>(kSlots)) {}

  // Not copyable or movable — the slots_ array is identity-pinned.
  SnapshotSlotRing(SnapshotSlotRing const &) = delete;
  SnapshotSlotRing &operator=(SnapshotSlotRing const &) = delete;
  SnapshotSlotRing(SnapshotSlotRing &&) = delete;
  SnapshotSlotRing &operator=(SnapshotSlotRing &&) = delete;

  // Publish the frozen snapshot_ts of a new SI transaction.
  //
  // Invalidate-first ordering: write the sentinel into tag before writing snap, so GC
  // can never pair the new snap with the old owner.  If GC's acquire-load of snap
  // observes the new value it synchronizes-with the release store below, so it also
  // sees tag == kEmpty or the final start_ts — never the stale predecessor.
  // tag is written last as the commit point.
  void Publish(uint64_t start_ts, uint64_t snapshot_ts) noexcept {
    auto &slot = slots_[start_ts % kSlots];
    slot.tag.store(kEmpty, std::memory_order_relaxed);        // invalidate old owner
    slot.snap.store(snapshot_ts, std::memory_order_release);  // carries the invalidation
    slot.tag.store(start_ts, std::memory_order_release);      // publish, tag last
  }

  // Compute the GC visibility horizon given the raw oldest-active start_timestamp.
  //
  // Returns the oldest active txn's snapshot_ts when the slot tag matches (exact case),
  // advances the monotone floor, and falls back to the floor on any tag mismatch (slot
  // recycled or not yet published).  When there are no active transactions the raw
  // oldest-active value is returned directly.
  uint64_t VisibilityHorizon(uint64_t raw_oldest_active, bool no_active_txns) noexcept {
    auto const &slot = slots_[raw_oldest_active % kSlots];
    uint64_t const snap = slot.snap.load(std::memory_order_acquire);
    uint64_t const tag = slot.tag.load(std::memory_order_acquire);
    if (tag == raw_oldest_active) {
      // The oldest active txn owns this slot: min(active snapshot_ts) == its snapshot. Advance the floor.
      uint64_t cur = floor_.load(std::memory_order_acquire);
      while (snap > cur &&
             !floor_.compare_exchange_weak(cur, snap, std::memory_order_release, std::memory_order_acquire)) {
      }
      return std::max(snap, cur);
    }
    // Tag mismatch: slot recycled or not yet published. Fall back to the monotone floor unless no active txns
    // (raw >= timestamp_); NOT `raw > last_committed` — a leapfrogged reader has raw < last_committed but is live.
    if (no_active_txns) return raw_oldest_active;
    return floor_.load(std::memory_order_acquire);
  }

  // Reset the ring to its initial state.
  //
  // PRECONDITION: must be called in a quiescent context (e.g., Clear / recovery) where
  // no GC or transaction threads are concurrently accessing the ring.
  void Reset() noexcept {
    floor_.store(kTimestampInitialId, std::memory_order_release);
    for (size_t i = 0; i < kSlots; ++i) {
      slots_[i].tag.store(kEmpty, std::memory_order_relaxed);
      slots_[i].snap.store(0, std::memory_order_relaxed);
    }
  }

 private:
  static constexpr uint64_t kEmpty = std::numeric_limits<uint64_t>::max();

  // Ring size; correctness is independent of this value because every slot access is
  // tag-validated.  A larger ring reduces collision probability under high concurrency.
  static constexpr size_t kSlots = 1ULL << 16;

  struct Slot {
    std::atomic<uint64_t> tag{kEmpty};  // owning start_timestamp; kEmpty = unoccupied
    std::atomic<uint64_t> snap{0};      // that txn's snapshot_ts
  };

  // NOLINTNEXTLINE(modernize-avoid-c-arrays) — heap array by design: std::array would place ~1 MiB inline.
  std::unique_ptr<Slot[]> slots_;
  std::atomic<uint64_t> floor_{kTimestampInitialId};
};

}  // namespace memgraph::storage
