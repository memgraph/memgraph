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

// Branch-side change filters. Reading a MAIN-resident vertex with the global, coarse `branched()` bit set costs a
// diff-engine FindVertex (BranchContext::ResolveVertex). A per-branch, per-kind gid filter answers "did this branch
// change this kind on gid G?" first: clear => read the fork copy; set => resolve (a false positive only wastes one).
// Correctness rests on no false negatives: every branch mutation of gid MUST insert into the matching kind filter.
//
// Coverage: only FORK vertices and fork edges (gids below the diff engine's gid floor) are answered here. Branch-native
// gids (at or above the floor) live in the diff engine alone and are never resolved through these filters, so their
// creation records nothing. Fork-object writes record per kind: label and property writes, edge creation/deletion at
// both endpoints (RecordEdgeChange), vertex deletion (RecordVertexDeleted), fork-edge property writes and deletion
// (RecordForkEdge*).

// Per-kind change filters of one branch, owned by BranchCore (branch_engine.hpp) and shared by its sessions;
// insert-only. `property_field_` is keyed by (gid, pid) and gates point reads GetProperty(gid, pid); `property_`
// gates whole-map Properties(). A pid bitset gates the fine filter first, so a never-written pid skips the probe
// even once the bloom saturates.
//
// EVERY property mutation must record (gid, pid) for each affected pid: SetProperty (one), Init/UpdateProperties (the
// map's keys), ClearProperties (the cleared map's keys). A miss is a stale read.
//
// There is deliberately no `any_change` catch-all: every value/adjacency read on a branch MUST gate on a dedicated kind
// filter that ALL of that kind's mutation sites populate.
//
// Size is fixed (a bloom cannot resize); saturation only raises the false-positive rate toward all-resolve, never
// toward incorrectness. InsertedKeys() is the saturation gauge: compare each count with the capacity noted on
// kFilterWords.
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

  // 32 KiB per filter, 224 KiB per branch. ~12k keys at ~1% false-positive, ~45k at ~10% (uniform-hash estimate).
  // Power of two (mask).
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

  // Fork-edge writes are keyed by the EDGE's gid (a different gid space), so they get their own filters and pid gate.
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
  // Monotonic (insert-only) lock-free approximate-membership filter keyed by gid. Register-blocked bloom, k=2: one gid
  // maps to one 64-bit word with two bits set.
  //
  // Insert is a release fetch_or and MayContain an acquire load, so a reader that sees the bit also sees the setting
  // query's writes regardless of thread (Bolt sessions share an io_context pool); concurrent same-word inserts lose no
  // update. The bit is set before the owning transaction commits and survives a rollback (false positive, never stale).
  // Parallel plans only fan out read-only paths (IsPathReadOnly, plan/rewrite/parallel_rewrite.hpp), so no write races
  // a read fan-out within one query.
  class MonotonicGidFilter {
   public:
    explicit MonotonicGidFilter(size_t num_words)
        : index_mask_(num_words - 1), words_(std::make_unique<std::atomic<uint64_t>[]>(num_words)) {}

    void Insert(storage::Gid gid) noexcept {
      auto const [idx, bits] = Locate(gid);
      auto const before = words_[idx].fetch_or(bits, std::memory_order_release);
      if ((before & bits) != bits) inserted_keys_.fetch_add(1, std::memory_order_relaxed);
    }

    // Gauge only, hence relaxed.
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
      // Fibonacci hash, sliced: word index from the high bits, two bit positions from disjoint low 6-bit fields.
      // Kept over a full-avalanche hash on purpose: gids are dense and sequential, and a multiplicative hash maps
      // consecutive gids to distinct bit patterns (fewest collisions on a dense run) for one multiply per probe.
      uint64_t const h = gid.AsUint() * 0x9E3779B97F4A7C15ULL;
      size_t const idx = (h >> 40U) & index_mask_;
      uint64_t const bit_a = 1ULL << (h & 63U);
      uint64_t const bit_b = 1ULL << ((h >> 6U) & 63U);
      return {idx, bit_a | bit_b};
    }

    size_t index_mask_;
    std::unique_ptr<std::atomic<uint64_t>[]> words_;
    std::atomic<uint64_t> inserted_keys_{0};
  };

  // Folds (gid, pid) into one 64-bit key; collisions are bloom false positives. A real hash, not an XOR: XOR of two
  // small structured integers collides systematically. The per-process seed is fine for an in-memory filter.
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
