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

#include "absl/hash/hash.h"
#include "storage/v2/id_types.hpp"

namespace memgraph::versioning {

// Branch-side change filters. On a branch, reading a MAIN-resident vertex with the global `branched()`
// bit set costs a diff-engine FindVertex (BranchContext::ResolveVertex); that bit is global across
// branches and coarse. A per-branch gid-keyed filter per change KIND answers "did this branch change
// this kind on gid G?" first: clear => read the fork copy (impl_); set => resolve (a false positive
// only wastes a resolve).
//
// Correctness rests on no false negatives: every branch mutation of gid MUST insert into the matching
// kind filter.
//
// Coverage: the filters answer only for FORK vertices and fork edges (gids below the diff engine's gid floor).
// A vertex or edge created on the branch has a native gid at or above the floor, lives in the diff engine alone, and
// is never resolved through these filters, so its creation records nothing. What a branch does to a fork object is
// recorded per kind: label and property writes, edge creation and deletion at both endpoints (RecordEdgeChange),
// vertex deletion (RecordVertexDeleted), and fork-edge property writes and deletion (RecordForkEdge*).

// Per-kind change filters of one branch, owned by BranchCore (branch_engine.hpp) and shared by every
// session on it. Starts empty and only grows. `label_`, `edge_`, `property_`, `deleted_` are per-gid; the fine
// `property_field_` is keyed by (gid, property_id) and gates point reads `GetProperty(gid, pid)`,
// while `property_` gates whole-map `Properties()`.
//
// For the fine filter: EVERY property mutation must record (gid, pid) for each affected pid --
// SetProperty (one), Init/UpdateProperties (the map's keys), ClearProperties (the cleared map's keys).
// A miss is a stale read. A pid bitset gates it first, so a never-written pid skips the probe even once
// the bloom saturates.
//
// There is deliberately no `any_change` catch-all: every value/adjacency read on a branch MUST gate
// on a dedicated kind filter that ALL of that kind's mutation sites populate.
//
// Size is fixed per branch (a bloom cannot resize); saturation only raises the false-positive rate,
// degrading toward all-resolve, never toward incorrectness. InsertedKeys() is the saturation gauge: compare each
// count with the capacity noted on kFilterWords.
class BranchChangeFilters {
 public:
  // Per filter, how many inserts set at least one new bit: an approximate (lower-bound) count of distinct keys.
  struct InsertedKeyCounts {
    uint64_t label{0};
    uint64_t property{0};
    uint64_t edge{0};
    uint64_t property_field{0};
    uint64_t deleted{0};
    uint64_t fork_edge_property{0};
    uint64_t fork_edge_deleted{0};
  };

  // 32 KiB per filter, 224 KiB per branch. ~8k keys at ~1% false-positive, ~20k at ~10%. Power of two (mask).
  static constexpr size_t kFilterWords = 4096;
  static_assert((kFilterWords & (kFilterWords - 1)) == 0, "kFilterWords must be a power of two");

  BranchChangeFilters()
      : property_(kFilterWords),
        label_(kFilterWords),
        edge_(kFilterWords),
        property_field_(kFilterWords),
        deleted_(kFilterWords),
        fork_edge_property_(kFilterWords),
        fork_edge_deleted_(kFilterWords) {}

  BranchChangeFilters(const BranchChangeFilters &) = delete;
  BranchChangeFilters &operator=(const BranchChangeFilters &) = delete;
  BranchChangeFilters(BranchChangeFilters &&) = delete;
  BranchChangeFilters &operator=(BranchChangeFilters &&) = delete;
  ~BranchChangeFilters() = default;

  void RecordLabelChange(storage::Gid gid) noexcept { label_.Insert(gid); }

  void RecordPropertyChange(storage::Gid gid) noexcept { property_.Insert(gid); }

  // Keyed by a mix of (gid, pid); populated at every property mutation site alongside `property_`.
  void RecordPropertyField(storage::Gid gid, storage::PropertyId pid) noexcept {
    pid_gate_[PidWord(pid)].fetch_or(PidBit(pid), std::memory_order_release);
    property_field_.Insert(storage::Gid::FromUint(CombineGidProp(gid, pid)));
  }

  [[nodiscard]] bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    if ((pid_gate_[PidWord(pid)].load(std::memory_order_acquire) & PidBit(pid)) == 0) return false;
    return property_field_.MayContain(storage::Gid::FromUint(CombineGidProp(gid, pid)));
  }

  // Records `endpoint_gid`'s adjacency as possibly changed. Call for BOTH endpoints of a changed edge,
  // since expansion from either side reads incident edges keyed by that vertex's gid.
  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { edge_.Insert(endpoint_gid); }

  // Records that this branch marked fork vertex `gid` deleted. Call before the marker write.
  void RecordVertexDeleted(storage::Gid gid) noexcept {
    any_vertex_deleted_.store(true, std::memory_order_release);
    deleted_.Insert(gid);
  }

  [[nodiscard]] bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return deleted_.MayContain(gid); }

  // False until the branch first marks any vertex deleted: lets a per-edge check skip even loading the neighbour.
  [[nodiscard]] bool MayHaveAnyVertexDeleted() const noexcept {
    return any_vertex_deleted_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool MayHaveLabelChange(storage::Gid gid) const noexcept { return label_.MayContain(gid); }

  [[nodiscard]] bool MayHavePropertyChange(storage::Gid gid) const noexcept { return property_.MayContain(gid); }

  [[nodiscard]] bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return edge_.MayContain(gid); }

  // Fork-edge stub writes are keyed by the EDGE's gid, a different gid space from the vertex-keyed filters
  // above, so they get their own filters and pid gate.
  void RecordForkEdgeProperty(storage::Gid edge_gid, storage::PropertyId pid) noexcept {
    edge_pid_gate_[PidWord(pid)].fetch_or(PidBit(pid), std::memory_order_release);
    fork_edge_property_.Insert(edge_gid);
  }

  void RecordForkEdgeDeleted(storage::Gid edge_gid) noexcept { fork_edge_deleted_.Insert(edge_gid); }

  [[nodiscard]] bool MayHaveForkEdgePropertyChange(storage::Gid edge_gid) const noexcept {
    return fork_edge_property_.MayContain(edge_gid);
  }

  [[nodiscard]] bool MayHaveForkEdgePropertyFieldChange(storage::Gid edge_gid, storage::PropertyId pid) const noexcept {
    if ((edge_pid_gate_[PidWord(pid)].load(std::memory_order_acquire) & PidBit(pid)) == 0) return false;
    return fork_edge_property_.MayContain(edge_gid);
  }

  [[nodiscard]] bool MayHaveForkEdgeDeleted(storage::Gid edge_gid) const noexcept {
    return fork_edge_deleted_.MayContain(edge_gid);
  }

  [[nodiscard]] InsertedKeyCounts InsertedKeys() const noexcept {
    return {.label = label_.inserted_keys(),
            .property = property_.inserted_keys(),
            .edge = edge_.inserted_keys(),
            .property_field = property_field_.inserted_keys(),
            .deleted = deleted_.inserted_keys(),
            .fork_edge_property = fork_edge_property_.inserted_keys(),
            .fork_edge_deleted = fork_edge_deleted_.inserted_keys()};
  }

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
      auto const before = words_[idx].fetch_or(bits, std::memory_order_release);
      if ((before & bits) != bits) inserted_keys_.fetch_add(1, std::memory_order_relaxed);
    }

    // Inserts that set a new bit; a gauge, so relaxed.
    [[nodiscard]] uint64_t inserted_keys() const noexcept { return inserted_keys_.load(std::memory_order_relaxed); }

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
      // Kept over a full-avalanche hash on purpose: vertex gids are dense and sequential, and a multiplicative hash
      // maps consecutive gids to distinct bit patterns, so a dense run of gids fills the filter with the fewest
      // collisions. Slicing one 64-bit hash into word index plus two bit positions costs one multiply per probe.
      uint64_t const h = gid.AsUint() * 0x9E3779B97F4A7C15ULL;
      size_t const idx = (h >> 40U) & index_mask_;
      uint64_t const bit_a = 1ULL << (h & 63U);
      uint64_t const bit_b = 1ULL << ((h >> 6U) & 63U);
      return {idx, bit_a | bit_b};
    }

    size_t index_mask_;  // num_words - 1; num_words is a power of two, so this masks the word index.
    std::unique_ptr<std::atomic<uint64_t>[]> words_;
    std::atomic<uint64_t> inserted_keys_{0};
  };

  // Folds (gid, pid) into one 64-bit key; collisions are bloom false positives. A real hash of the pair, not an XOR:
  // XOR of two small structured integers collides systematically. The seed is per process, which is fine for an
  // in-memory filter. The result is re-mixed by MonotonicGidFilter::Locate.
  [[nodiscard]] static uint64_t CombineGidProp(storage::Gid gid, storage::PropertyId pid) noexcept {
    return absl::HashOf(gid.AsUint(), pid.AsUint());
  }

  // Aliased pids (pid mod 256) only yield false positives.
  static constexpr size_t kPidGateBits = 256;

  [[nodiscard]] static size_t PidWord(storage::PropertyId pid) noexcept { return (pid.AsUint() % kPidGateBits) >> 6U; }

  [[nodiscard]] static uint64_t PidBit(storage::PropertyId pid) noexcept { return 1ULL << (pid.AsUint() & 63U); }

  std::array<std::atomic<uint64_t>, kPidGateBits / 64> pid_gate_{};
  std::array<std::atomic<uint64_t>, kPidGateBits / 64> edge_pid_gate_{};
  std::atomic<bool> any_vertex_deleted_{false};
  MonotonicGidFilter property_;
  MonotonicGidFilter label_;
  MonotonicGidFilter edge_;
  MonotonicGidFilter property_field_;
  MonotonicGidFilter deleted_;
  MonotonicGidFilter fork_edge_property_;
  MonotonicGidFilter fork_edge_deleted_;
};

}  // namespace memgraph::versioning
