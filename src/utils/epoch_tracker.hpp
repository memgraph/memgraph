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

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>

#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::utils {

/// Epoch-based safe-drain tracker for deferred reclamation.
///
/// Readers call Acquire() on entry and Release(id) on exit.  A producer
/// records `guard_epoch = CurrentEpoch()` when an object becomes eligible for
/// reclamation.  The object may be safely freed once IsSafeToFree(guard_epoch)
/// returns true — meaning every reader that was alive at the time of the
/// guard_epoch snapshot has since called Release().
///
/// The implementation is a simplified variant of the SkipListGc bitmap
/// approach: accessor IDs are packed into 64-bit fields inside fixed-size
/// Blocks linked in a doubly-linked list.  Unlike SkipListGc, fully-dead
/// blocks are never reclaimed, so the implementation is free of the tricky
/// races that accompany block removal.
///
/// Memory bound: each block holds 4096 IDs and costs ~512 bytes.  A server
/// processing 1 billion unique edge-index scans will retain ~122 MB of tracker
/// memory for the lifetime of the storage instance.  If this becomes a concern
/// in practice, consider adding safe block reclamation (TODO).
///
/// Teardown precondition: ~EpochTracker() / Clear() free all Blocks WITHOUT
/// waiting for outstanding readers. The caller MUST guarantee that every id
/// handed out by Acquire() has been Release()d (equivalently: no reader holds a
/// live id) before the tracker is destroyed or Clear()ed. Destroying with live
/// readers is undefined behaviour (a subsequent Release() would touch freed
/// memory).
///
/// Dead-prefix watermark cache (amortized O(1) rescan):
///   The dead prefix is monotonic — bits in field[] are only ever set (never
///   cleared) until Clear().  ComputeLastDead() caches (cached_last_dead_,
///   scan_resume_) so subsequent calls resume from the stall point instead of
///   re-walking the entire block list from tail_.  The cache is NOT thread-safe
///   under *concurrent* IsSafeToFree() / ComputeLastDead() calls: it is a
///   multi-step read-modify of two non-atomic fields, so callers must serialise
///   these two methods externally.  The only caller is the light-edge graveyard
///   drain (DrainLightEdgeGraveyard); note it does NOT run on a single dedicated
///   thread — FreeMemory reaches it from the periodic GC runner, a FREE MEMORY
///   query, and SetStorageMode.  Serialisation there is provided not by a thread
///   identity but by the drain itself: it swaps the whole graveyard into a
///   thread-local list under a lock, so a second concurrent caller swaps an empty
///   list and returns before ever touching the tracker — at most one thread
///   executes the scan at a time.  (Cross-drain visibility of the cache fields
///   comes from that same graveyard mutex; and even a stale read would be benign
///   because the dead prefix is monotonic — resuming earlier only re-scans, never
///   frees prematurely.)  Do NOT add an IsSafeToFree call site outside that
///   swap-serialised drain without adding your own external serialisation.

// TODO This is just the skiplist allocator's epoch tracker. Can we move the skiplist's out and use this there as well?
// Audit to make sure the logic is EXACTLY the same.
class EpochTracker {
 private:
  static constexpr uint64_t kIdsInField = sizeof(uint64_t) * 8;        // 64 bits per field
  static constexpr uint64_t kBlockFields = 64;                         // 64 fields per block
  static constexpr uint64_t kIdsInBlock = kBlockFields * kIdsInField;  // 4096 IDs per block

  struct Block {
    std::atomic<Block *> prev{nullptr};
    std::atomic<Block *> succ{nullptr};
    uint64_t const first_id;  // immutable after construction; safe to read without lock
    // Placed on its own cache line to prevent false sharing with prev/succ/first_id.
    alignas(64) std::atomic<uint64_t> field[kBlockFields];

    explicit Block(uint64_t id) : first_id{id}, field{} {}
  };

  Block *AllocateBlock(Block *expected_head) {
    auto guard = std::lock_guard{lock_};
    // relaxed: the lock provides the necessary acquire/release ordering here.
    Block *curr_head = head_.load(std::memory_order_relaxed);
    if (curr_head != expected_head) return curr_head;  // someone else allocated while we waited
    // Release() runs in noexcept contexts (RAII guard destructors). A throwing
    // allocation would terminate without a named site. Deferring the failure
    // after Acquire()'s fetch_add would create a never-releasable ghost ID that
    // permanently stalls ComputeLastDead.  Abort here with a named assertion
    // instead — no ghost ID is possible because AllocateBlock is only called
    // when the id is already committed.
    auto *block = new (std::nothrow) Block{last_id_};
    MG_ASSERT(block != nullptr, "EpochTracker: out of memory allocating an epoch block");
    // relaxed: subsumed by the release store to head_ below (:90); any reader
    // that acquires head_ will also observe this prev pointer correctly.
    block->prev.store(curr_head, std::memory_order_relaxed);
    // succ is already nullptr from the Block ctor zero-init; no store needed.
    last_id_ += kIdsInBlock;
    if (curr_head == nullptr) {
      tail_.store(block, std::memory_order_release);
    } else {
      curr_head->succ.store(block, std::memory_order_release);
    }
    head_.store(block, std::memory_order_release);
    return block;
  }

 public:
  EpochTracker() = default;

  ~EpochTracker() { Clear(); }

  EpochTracker(const EpochTracker &) = delete;
  EpochTracker &operator=(const EpochTracker &) = delete;
  EpochTracker(EpochTracker &&) = delete;
  EpochTracker &operator=(EpochTracker &&) = delete;

  /// Acquire a unique epoch ID for a new reader. Must be paired with Release().
  /// The returned id MUST be passed to Release(); discarding it creates a
  /// permanent ghost that stalls ComputeLastDead and prevents reclamation.
  [[nodiscard("discarded id leaks a permanent ghost into the epoch tracker, permanently stalling ComputeLastDead")]]
  uint64_t Acquire() noexcept {
    // relaxed: the counter publishes no payload; the safety channel is the
    // bitmap (field[]) written with release in Release().
    return epoch_.fetch_add(1, std::memory_order_relaxed);
  }

  /// Mark the reader with the given ID as done.
  void Release(uint64_t id) noexcept {
    Block *head = head_.load(std::memory_order_acquire);
    if (head == nullptr) head = AllocateBlock(head);
    while (true) {
      MG_ASSERT(head != nullptr, "EpochTracker: missing block for id {}", id);
      if (id < head->first_id) {
        head = head->prev.load(std::memory_order_acquire);
      } else if (id >= head->first_id + kIdsInBlock) {
        head = AllocateBlock(head);
      } else {
        uint64_t local_id = id - head->first_id;
        uint64_t field_idx = local_id / kIdsInField;
        uint64_t bit = local_id % kIdsInField;
        uint64_t mask = uint64_t{1} << bit;
        // release: publishes the reader's completed critical section to
        // ComputeLastDead's acquire loads; the acquire half is not needed here.
        auto prev_val = head->field[field_idx].fetch_or(mask, std::memory_order_release);
        MG_ASSERT(!(prev_val & mask), "EpochTracker: id {} released twice", id);
        return;
      }
    }
  }

  /// Returns the epoch value that will be returned by the next Acquire() call.
  /// Record this as the guard_epoch when an object becomes reclaimable.
  uint64_t CurrentEpoch() const noexcept {
    // relaxed: epoch_ has no paired release write (fetch_add in Acquire is also
    // relaxed); callers only need a snapshot of the counter, not synchronisation.
    return epoch_.load(std::memory_order_relaxed);
  }

  /// Returns true if all readers with id < guard_epoch have called Release().
  ///
  /// NOT thread-safe vs concurrent IsSafeToFree() calls (see class-level doc).
  /// Sole caller is the swap-serialised light-edge graveyard drain
  /// (DrainLightEdgeGraveyard), which guarantees at most one thread scans at a time.
  bool IsSafeToFree(uint64_t guard_epoch) const noexcept {
    if (guard_epoch == 0) return true;  // no readers existed before the guard was recorded
    uint64_t last_dead = ComputeLastDead();
    if (last_dead == kNoDeadPrefix) return false;  // no contiguous dead prefix yet
    return last_dead >= guard_epoch - 1;
  }

 private:
  // Sentinel: all bits set — signals "no contiguous dead prefix established yet".
  // Used in IsSafeToFree and ComputeLastDead.
  static constexpr uint64_t kNoDeadPrefix = std::numeric_limits<uint64_t>::max();
  // All 64 bits set: every ID in the field has been released.
  static constexpr uint64_t kFieldFull = std::numeric_limits<uint64_t>::max();

  /// Returns the highest ID N such that all IDs in [0, N] have been released,
  /// or kNoDeadPrefix if no contiguous dead prefix has been established yet
  /// (i.e., ID 0 has not been released, or nothing has been released at all).
  ///
  /// Resume optimisation: the dead prefix is monotonically non-decreasing
  /// (bits in field[] are set, never cleared, until Clear()).  We cache the
  /// stall point (scan_resume_, cached_last_dead_) so the scan restarts from
  /// the block where the prefix last stalled rather than from tail_ each time.
  /// This makes repeated calls amortized O(newly-freed IDs) instead of O(all
  /// freed IDs).  Resume granularity is one Block: all fields within the resume
  /// block are re-examined (some may have been partially filled since the last
  /// call) — this is safe and simpler than tracking per-field position.
  ///
  /// NOT thread-safe vs concurrent calls (see class-level doc and IsSafeToFree).
  uint64_t ComputeLastDead() const noexcept {
    // Determine where to start scanning.
    // If scan_resume_ is set, the prefix up to cached_last_dead_ is proven dead
    // forever (bits only grow); resume from the stalled block.  Otherwise start
    // from the oldest block (tail_).
    Block *block;
    uint64_t last_dead;
    if (scan_resume_ != nullptr) {
      block = scan_resume_;
      last_dead = cached_last_dead_;
    } else {
      block = tail_.load(std::memory_order_acquire);
      last_dead = kNoDeadPrefix;  // sentinel: no dead prefix yet
    }

    while (block != nullptr) {
      for (uint64_t pos = 0; pos < kBlockFields; ++pos) {
        uint64_t bits = block->field[pos].load(std::memory_order_acquire);
        if (bits != kFieldFull) {
          if (bits == 0) {
            // No IDs in this field are dead; the dead prefix stalls here.
            // Update the resume cache to this block (re-scanning its fields next
            // time is cheap and correct — partial fields may advance further).
            scan_resume_ = block;
            cached_last_dead_ = last_dead;
            return last_dead;
          }
          // Partial field: first still-alive id = first zero bit of bits =
          // countr_zero(~bits).  ~bits != 0 here: bits is neither 0 (caught
          // above) nor all-ones (caught by kFieldFull check), so at least one
          // bit in ~bits is set and countr_zero will never return 64.
          auto where_alive = static_cast<uint64_t>(std::countr_zero(~bits));
          uint64_t base = block->first_id + pos * kIdsInField;
          // base + where_alive - 1: when where_alive==0 and base==0 this wraps to
          // kNoDeadPrefix, which correctly signals "no dead prefix" (bit 0 is alive).
          uint64_t result = base + where_alive - 1;
          // Stall point is inside this block; cache it so next call re-scans
          // from here (the partial field may fill further).
          scan_resume_ = block;
          cached_last_dead_ = last_dead;
          return result;
        }
        // Full field: all 64 IDs in this field are dead.
        last_dead = block->first_id + (pos + 1) * kIdsInField - 1;
      }
      // Entire block is dead; advance to successor and update the cache.
      Block *next = block->succ.load(std::memory_order_acquire);
      // The resume pointer moves to the next block (or stays on the last one if
      // next==nullptr, meaning the entire tracked range is dead so far).
      scan_resume_ = (next != nullptr) ? next : block;
      cached_last_dead_ = last_dead;
      block = next;
    }
    return last_dead;
  }

  void Clear() noexcept {
    // Precondition: all ids from Acquire() have been Release()d (see class doc).
    // We free every Block unconditionally; a concurrent/late Release() here is UB.
    Block *head = head_.load(std::memory_order_acquire);
    while (head != nullptr) {
      Block *prev = head->prev.load(std::memory_order_acquire);
      delete head;
      head = prev;
    }
    epoch_.store(0, std::memory_order_relaxed);
    head_.store(nullptr, std::memory_order_relaxed);
    tail_.store(nullptr, std::memory_order_relaxed);
    last_id_ = 0;
    // CRITICAL: reset the watermark cache.  scan_resume_ points into a Block
    // that is now freed; leaving it set would cause a dangling-pointer
    // dereference on the next ComputeLastDead() call.
    cached_last_dead_ = kNoDeadPrefix;
    scan_resume_ = nullptr;
  }

  RWSpinLock lock_;
  // Placed on its own cache line: Acquire() hot-path increments epoch_ without
  // taking lock_; false sharing with lock_/head_/tail_ would serialise readers.
  alignas(64) std::atomic<uint64_t> epoch_{0};
  std::atomic<Block *> head_{nullptr};
  std::atomic<Block *> tail_{nullptr};
  uint64_t last_id_{0};

  // Dead-prefix watermark cache (see ComputeLastDead() doc).
  //
  // These are NOT atomic.  Thread-safety relies on external serialisation of
  // IsSafeToFree / ComputeLastDead: the sole caller is the light-edge graveyard
  // drain, which swaps the graveyard out under a lock so only one thread ever
  // executes the scan at a time (see class-level doc — the drain is NOT a single
  // dedicated thread).  Making these atomic would add RMW overhead on every GC
  // call and would not be sufficient on its own (ComputeLastDead performs a
  // multi-step read-modify of both fields, which requires external serialisation
  // regardless).
  //
  // Lifecycle: initialized to "no cache" state; updated by ComputeLastDead();
  // MUST be reset to "no cache" state in Clear() because scan_resume_ points
  // into a Block that Clear() deallocates (dangling pointer otherwise).
  mutable uint64_t cached_last_dead_{kNoDeadPrefix};
  mutable Block *scan_resume_{nullptr};
};

}  // namespace memgraph::utils
