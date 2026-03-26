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
#include <cstdint>
#include <cstring>
#include <limits>

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
/// blocks are never reclaimed (blocks are small and very few in practice), so
/// the implementation is free of the tricky races that accompany block removal.
class EpochTracker {
 private:
  static constexpr uint64_t kIdsInField = sizeof(uint64_t) * 8;        // 64 bits per field
  static constexpr uint64_t kBlockFields = 64;                         // 64 fields per block
  static constexpr uint64_t kIdsInBlock = kBlockFields * kIdsInField;  // 4096 IDs per block

  struct Block {
    std::atomic<Block *> prev{nullptr};
    std::atomic<Block *> succ{nullptr};
    uint64_t first_id{0};
    std::atomic<uint64_t> field[kBlockFields];

    Block() { std::memset(field, 0, sizeof(field)); }
  };

  Block *AllocateBlock(Block *expected_head) {
    auto guard = std::lock_guard{lock_};
    Block *curr_head = head_.load(std::memory_order_acquire);
    if (curr_head != expected_head) return curr_head;  // someone else allocated while we waited
    auto *block = new Block{};
    block->prev.store(curr_head, std::memory_order_release);
    block->succ.store(nullptr, std::memory_order_release);
    block->first_id = last_id_;
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
  uint64_t Acquire() { return epoch_.fetch_add(1, std::memory_order_acq_rel); }

  /// Mark the reader with the given ID as done.
  void Release(uint64_t id) {
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
        auto prev_val = head->field[field_idx].fetch_or(mask, std::memory_order_acq_rel);
        MG_ASSERT(!(prev_val & mask), "EpochTracker: id {} released twice", id);
        return;
      }
    }
  }

  /// Returns the epoch value that will be returned by the next Acquire() call.
  /// Record this as the guard_epoch when an object becomes reclaimable.
  uint64_t CurrentEpoch() const { return epoch_.load(std::memory_order_acquire); }

  /// Returns true if all readers with id < guard_epoch have called Release().
  /// Safe to call from any thread.
  bool IsSafeToFree(uint64_t guard_epoch) const {
    if (guard_epoch == 0) return true;  // no readers existed before the guard was recorded
    uint64_t last_dead = ComputeLastDead();
    if (last_dead == uint64_t(-1)) return false;  // no contiguous dead prefix yet
    return last_dead >= guard_epoch - 1;
  }

 private:
  /// Returns the highest ID N such that all IDs in [0, N] have been released,
  /// or uint64_t(-1) if no contiguous dead prefix has been established yet
  /// (i.e., ID 0 has not been released, or nothing has been released at all).
  uint64_t ComputeLastDead() const {
    Block *block = tail_.load(std::memory_order_acquire);
    uint64_t last_dead = uint64_t(-1);  // sentinel: no dead prefix yet
    while (block != nullptr) {
      for (uint64_t pos = 0; pos < kBlockFields; ++pos) {
        uint64_t field = block->field[pos].load(std::memory_order_acquire);
        if (field != std::numeric_limits<uint64_t>::max()) {
          if (field == 0) return last_dead;  // no IDs in this field are dead
          // Partial field: find the first zero bit (first still-alive ID).
          // __builtin_ffsl returns 1-indexed position of LSB; subtract 1 for 0-indexed.
          int where_alive = __builtin_ffsl(~field) - 1;
          uint64_t base = block->first_id + pos * kIdsInField;
          // base + where_alive - 1: when where_alive==0 and base==0 this wraps to
          // uint64_t(-1), which correctly signals "no dead prefix" (bit 0 is alive).
          return base + static_cast<uint64_t>(where_alive) - 1;
        }
        // Full field: all 64 IDs in this field are dead.
        last_dead = block->first_id + (pos + 1) * kIdsInField - 1;
      }
      block = block->succ.load(std::memory_order_acquire);
    }
    return last_dead;
  }

  void Clear() {
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
  }

  RWSpinLock lock_;
  std::atomic<uint64_t> epoch_{0};
  std::atomic<Block *> head_{nullptr};
  std::atomic<Block *> tail_{nullptr};
  uint64_t last_id_{0};
};

}  // namespace memgraph::utils
