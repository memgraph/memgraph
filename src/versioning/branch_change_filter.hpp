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
#include <cstddef>
#include <cstdint>
#include <memory>

#include "storage/v2/id_types.hpp"
#include "versioning/branch_change_kind.hpp"

namespace memgraph::versioning {

// Branch-side change filters. On a branch, reading a MAIN-resident vertex with the global `branched()`
// bit set costs a diff-engine FindVertex (BranchContext::ResolveVertex); that bit is global across
// branches and coarse. A per-branch gid-keyed filter per change KIND answers "did this branch change
// this kind on gid G?" first: clear => read the fork copy (impl_); set => resolve (a false positive
// only wastes a resolve).
//
// Correctness rests on no false negatives: every branch mutation of gid MUST insert into the matching
// kind filter.

// Per-kind change filters of one branch, owned by BranchCore (branch_engine.hpp) and shared by every
// session on it. Starts empty and only grows. `label_`, `edge_`, `property_`, `deleted_` are per-gid; the fine
// `property_field_` is keyed by (gid, property_id) and gates point reads `GetProperty(gid, pid)`,
// while `property_` gates whole-map `Properties()`.
//
// For the fine filter: EVERY property mutation must record (gid, pid) for each affected pid --
// SetProperty (one), Init/UpdateProperties (the map's keys), ClearProperties (the cleared map's keys).
// A miss is a stale read.
//
// There is deliberately no `any_change` catch-all: every value/adjacency read on a branch MUST gate
// on a dedicated kind filter that ALL of that kind's mutation sites populate.
//
// Size is fixed per branch (a bloom cannot resize); saturation only raises the false-positive rate,
// degrading toward all-resolve, never toward incorrectness.
class BranchChangeFilters {
 public:
  // 32 KiB per filter, 160 KiB per branch. ~8k keys at ~1% false-positive, ~20k at ~10%. Power of two (mask).
  static constexpr size_t kFilterWords = 4096;
  static_assert((kFilterWords & (kFilterWords - 1)) == 0, "kFilterWords must be a power of two");

  BranchChangeFilters()
      : property_(kFilterWords),
        label_(kFilterWords),
        edge_(kFilterWords),
        property_field_(kFilterWords),
        deleted_(kFilterWords) {}

  BranchChangeFilters(const BranchChangeFilters &) = delete;
  BranchChangeFilters &operator=(const BranchChangeFilters &) = delete;
  BranchChangeFilters(BranchChangeFilters &&) = delete;
  BranchChangeFilters &operator=(BranchChangeFilters &&) = delete;
  ~BranchChangeFilters() = default;

  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept {
    (kind == BranchChangeKind::kLabel ? label_ : property_).Insert(gid);
  }

  // Keyed by a mix of (gid, pid); populated at every property mutation site alongside `property_`.
  void RecordPropertyField(storage::Gid gid, storage::PropertyId pid) noexcept {
    property_field_.Insert(storage::Gid::FromUint(CombineGidProp(gid, pid)));
  }

  [[nodiscard]] bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    return property_field_.MayContain(storage::Gid::FromUint(CombineGidProp(gid, pid)));
  }

  // Records `endpoint_gid`'s adjacency as possibly changed. Call for BOTH endpoints of a changed edge,
  // since expansion from either side reads incident edges keyed by that vertex's gid.
  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { edge_.Insert(endpoint_gid); }

  // Records that this branch marked fork vertex `gid` deleted. Call before the marker write.
  void RecordVertexDeleted(storage::Gid gid) noexcept { deleted_.Insert(gid); }

  [[nodiscard]] bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return deleted_.MayContain(gid); }

  [[nodiscard]] bool MayHaveLabelChange(storage::Gid gid) const noexcept { return label_.MayContain(gid); }

  [[nodiscard]] bool MayHavePropertyChange(storage::Gid gid) const noexcept { return property_.MayContain(gid); }

  [[nodiscard]] bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return edge_.MayContain(gid); }

 private:
  // Monotonic (insert-only) lock-free approximate-membership filter keyed by gid. Register-blocked
  // bloom, k=2: one gid maps to one 64-bit word with two bits set.
  //
  // A bit not yet visible to a reader is a stale read, so visibility matters. Insert is a release
  // fetch_or and MayContain an acquire load: a reader that sees the bit also sees the setting query's
  // writes, independent of thread identity (Bolt sessions share an io_context pool). Concurrent
  // same-word inserts lose no update. The bit is set before the owning transaction commits, and a
  // rolled-back transaction leaves it set (false positive, never stale). Parallel plans are read-only
  // (plan/rewrite/parallel_rewrite.hpp), so no write races a read fan-out within one query.
  class MonotonicGidFilter {
   public:
    // `num_words` MUST be a power of two (static_assert on kFilterWords).
    explicit MonotonicGidFilter(size_t num_words)
        : index_mask_(num_words - 1), words_(std::make_unique<std::atomic<uint64_t>[]>(num_words)) {}

    void Insert(storage::Gid gid) noexcept {
      auto const [idx, bits] = Locate(gid);
      words_[idx].fetch_or(bits, std::memory_order_release);
    }

    [[nodiscard]] bool MayContain(storage::Gid gid) const noexcept {
      auto const [idx, bits] = Locate(gid);
      return (words_[idx].load(std::memory_order_acquire) & bits) == bits;
    }

   private:
    struct Slot {
      size_t idx;
      uint64_t bits;
    };

    [[nodiscard]] Slot Locate(storage::Gid gid) const noexcept {
      // Fibonacci hash; word index from the high bits, the two bit positions from disjoint low 6-bit fields.
      uint64_t const h = gid.AsUint() * 0x9E3779B97F4A7C15ULL;
      size_t const idx = (h >> 40U) & index_mask_;
      uint64_t const bit_a = 1ULL << (h & 63U);
      uint64_t const bit_b = 1ULL << ((h >> 6U) & 63U);
      return {idx, bit_a | bit_b};
    }

    size_t index_mask_;  // num_words - 1; num_words is a power of two, so this masks the word index.
    std::unique_ptr<std::atomic<uint64_t>[]> words_;
  };

  // Folds (gid, pid) into one high-entropy 64-bit key; collisions are bloom false positives.
  [[nodiscard]] static uint64_t CombineGidProp(storage::Gid gid, storage::PropertyId pid) noexcept {
    return gid.AsUint() ^ (static_cast<uint64_t>(pid.AsUint()) * 0xD1B54A32D192ED03ULL);
  }

  MonotonicGidFilter property_;
  MonotonicGidFilter label_;
  MonotonicGidFilter edge_;
  MonotonicGidFilter property_field_;
  MonotonicGidFilter deleted_;
};

}  // namespace memgraph::versioning
