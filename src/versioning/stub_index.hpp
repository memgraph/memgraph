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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>

#include "storage/v2/vertex.hpp"

namespace memgraph::versioning {

// Gid-keyed table of a branch's fork-vertex stubs: for each fork gid that has a stub, the stub's diff-engine Vertex*
// and the fork (main) Vertex* it shadows. Replaces a skiplist lookup per read with a few probes, and no lock.
//
// Structure: an insert-only list of open-addressing segments (linear probing, Fibonacci hash). Segment i holds
// kFirstCapacity << i slots and is allocated when the newest segment reaches 3/4 load (or a probe run exceeds
// kMaxProbe). Segments are never rehashed, moved or freed before ~StubIndex, so readers need no lock and no
// reclamation; entries never change once set, so Insert is idempotent.
//
// Memory is proportional to the number of recorded gids: a slot is 24 B, every full segment is >=3/4 loaded and
// capacities double, so N entries cost ~32 B/entry (newest segment at its load limit) to ~64 B/entry (newest
// segment just opened), plus a 24 KiB floor. It does not depend on graph size or how the gids are spread.
//
// Ordering: Insert claims a slot's key with an acq_rel CAS and publishes each pointer with a release store/CAS;
// segments are published with release before newest_ advances past them. Readers load newest_, segment pointers,
// keys and pointers with acquire, so a reader that sees a pointer also sees the pointee's construction, and a reader
// ordered after an Insert (happens-before) finds it.
//
// A null result only means "not recorded here (yet)"; callers must fall back to the skiplist, which stays
// authoritative. The pointers are stable for the owning BranchCore's lifetime: stubs are never erased from the diff
// engine, and a fork vertex visible at fork_ts cannot be freed while the fork pin is held.
class StubIndex {
 public:
  StubIndex() = default;

  StubIndex(const StubIndex &) = delete;
  StubIndex &operator=(const StubIndex &) = delete;
  StubIndex(StubIndex &&) = delete;
  StubIndex &operator=(StubIndex &&) = delete;

  ~StubIndex() {
    for (auto &seg : segments_) delete seg.load(std::memory_order_relaxed);
  }

  // Records non-null pointers for `gid`. Concurrent-safe.
  // @throw std::bad_alloc
  void Insert(uint64_t gid, storage::Vertex *stub, storage::Vertex *fork) {
    if (stub == nullptr && fork == nullptr) return;
    const uint64_t key = gid + 1;  // 0 marks an empty slot; real gids never reach UINT64_MAX

    uint32_t newest = newest_.load(std::memory_order_acquire);
    // Fill an existing entry (e.g. the stub was recorded first, the fork comes later).
    for (uint32_t i = newest + 1; i-- > 0;) {
      Segment *seg = segments_[i].load(std::memory_order_acquire);
      if (seg == nullptr) continue;
      if (Slot *slot = Probe(*seg, key)) {
        Fill(*slot, stub, fork);
        return;
      }
    }

    // Not found anywhere: claim a slot in the newest segment, growing as needed. A racing insert of the same gid may
    // land in another segment; both carry identical pointers, so the duplicate is harmless.
    for (;;) {
      Segment *seg = EnsureSegment(newest);
      if (seg->count.load(std::memory_order_relaxed) < seg->limit) {
        if (Slot *slot = Claim(*seg, key)) {
          Fill(*slot, stub, fork);
          return;
        }
      }
      if (newest + 1 >= kMaxSegments) throw std::bad_alloc{};
      EnsureSegment(newest + 1);
      newest_.compare_exchange_strong(newest, newest + 1, std::memory_order_release, std::memory_order_acquire);
      newest = newest_.load(std::memory_order_acquire);
    }
  }

  storage::Vertex *FindStub(uint64_t gid) const noexcept { return Find<&Slot::stub>(gid); }

  storage::Vertex *FindFork(uint64_t gid) const noexcept { return Find<&Slot::fork>(gid); }

 private:
  static constexpr uint32_t kFirstShift = 10;
  static constexpr size_t kFirstCapacity = size_t{1} << kFirstShift;
  static constexpr uint32_t kMaxSegments = 40;
  static constexpr size_t kMaxProbe = 128;

  struct Slot {
    std::atomic<uint64_t> key{0};
    std::atomic<storage::Vertex *> stub{nullptr};
    std::atomic<storage::Vertex *> fork{nullptr};
  };

  struct Segment {
    explicit Segment(uint32_t index)
        : shift(64 - (kFirstShift + index)),
          mask((kFirstCapacity << index) - 1),
          limit(static_cast<uint32_t>(((mask + 1) / 4) * 3)),
          slots(std::make_unique<Slot[]>(mask + 1)) {}

    // Read-mostly part, kept off the line of the contended counter.
    const uint32_t shift;
    const size_t mask;
    const uint32_t limit;
    const std::unique_ptr<Slot[]> slots;
    alignas(64) std::atomic<uint32_t> count{0};
  };

  static size_t Home(const Segment &seg, uint64_t key) noexcept {
    return static_cast<size_t>((key * 0x9E3779B97F4A7C15ULL) >> seg.shift);
  }

  // Slot holding `key`, or nullptr at the first empty slot / after kMaxProbe slots. Inserts never place a key beyond
  // that bound, so this is exact.
  static Slot *Probe(const Segment &seg, uint64_t key) noexcept {
    size_t idx = Home(seg, key);
    for (size_t n = 0; n < kMaxProbe; ++n, idx = (idx + 1) & seg.mask) {
      Slot &slot = seg.slots[idx];
      const uint64_t k = slot.key.load(std::memory_order_acquire);
      if (k == key) return &slot;
      if (k == 0) return nullptr;
    }
    return nullptr;
  }

  // Slot for `key` within the probe bound (existing or newly claimed), or nullptr if the run is full.
  static Slot *Claim(Segment &seg, uint64_t key) noexcept {
    size_t idx = Home(seg, key);
    for (size_t n = 0; n < kMaxProbe; ++n, idx = (idx + 1) & seg.mask) {
      Slot &slot = seg.slots[idx];
      uint64_t k = slot.key.load(std::memory_order_acquire);
      if (k == 0) {
        if (slot.key.compare_exchange_strong(k, key, std::memory_order_acq_rel, std::memory_order_acquire)) {
          seg.count.fetch_add(1, std::memory_order_relaxed);
          return &slot;
        }
        // Lost the race; k now holds the winner's key.
      }
      if (k == key) return &slot;
    }
    return nullptr;
  }

  // Sets still-null pointers; set pointers are never changed. Release publishes the pointee to readers.
  static void Fill(Slot &slot, storage::Vertex *stub, storage::Vertex *fork) noexcept {
    storage::Vertex *expected = nullptr;
    if (fork != nullptr) {
      slot.fork.compare_exchange_strong(expected, fork, std::memory_order_release, std::memory_order_relaxed);
    }
    expected = nullptr;
    if (stub != nullptr) {
      slot.stub.compare_exchange_strong(expected, stub, std::memory_order_release, std::memory_order_relaxed);
    }
  }

  // Installs segment `index` if absent (loser of the CAS frees its allocation) and returns it.
  Segment *EnsureSegment(uint32_t index) {
    Segment *seg = segments_[index].load(std::memory_order_acquire);
    if (seg != nullptr) return seg;
    auto fresh = std::make_unique<Segment>(index);
    if (segments_[index].compare_exchange_strong(
            seg, fresh.get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
      return fresh.release();
    }
    return seg;
  }

  // Newest to oldest. A recorded-but-unfilled entry (null pointer) is skipped: another segment may hold the pointer,
  // and null is a valid "fall back to the skiplist" answer anyway.
  template <std::atomic<storage::Vertex *> Slot::*Member>
  storage::Vertex *Find(uint64_t gid) const noexcept {
    const uint64_t key = gid + 1;
    const uint32_t newest = newest_.load(std::memory_order_acquire);
    for (uint32_t i = newest + 1; i-- > 0;) {
      const Segment *seg = segments_[i].load(std::memory_order_acquire);
      if (seg == nullptr) continue;
      if (const Slot *slot = Probe(*seg, key)) {
        if (auto *v = (slot->*Member).load(std::memory_order_acquire)) return v;
      }
    }
    return nullptr;
  }

  alignas(64) std::atomic<uint32_t> newest_{0};
  std::array<std::atomic<Segment *>, kMaxSegments> segments_{};
};

}  // namespace memgraph::versioning
