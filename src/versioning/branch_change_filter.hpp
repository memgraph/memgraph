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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/hash/hash.h"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::versioning {

// Per-kind gid bloom filters of one branch, shared by its sessions. A miss means the branch never changed that kind on
// that base object, so every mutation site must Record before writing. Insert-only; a rolled-back write leaves its bit.
class BranchChangeFilters {
 public:
  // 32 KiB per filter; power of two.
  static constexpr size_t kFilterWords = 4096;
  static_assert((kFilterWords & (kFilterWords - 1)) == 0, "kFilterWords must be a power of two");

  BranchChangeFilters()
      : property_(kFilterWords),
        label_(kFilterWords),
        edge_(kFilterWords),
        property_field_(kFilterWords),
        deleted_(kFilterWords),
        base_edge_property_(kFilterWords),
        base_edge_deleted_(kFilterWords) {}

  BranchChangeFilters(const BranchChangeFilters &) = delete;
  BranchChangeFilters &operator=(const BranchChangeFilters &) = delete;
  BranchChangeFilters(BranchChangeFilters &&) = delete;
  BranchChangeFilters &operator=(BranchChangeFilters &&) = delete;
  ~BranchChangeFilters() = default;

  void RecordLabelChange(storage::Gid gid) noexcept { label_.Insert(gid); }

  void RecordPropertyChange(storage::Gid gid) noexcept { property_.Insert(gid); }

  // Label gates (id mod 256, no gid): "touched" = the label was written on a base vertex, "marked" = Mark(label) was
  // written on an overlay. A clear bit lets readers skip the per-gid work.
  void RecordLabelTouched(storage::LabelId label) noexcept {
    label_touched_gate_[GateWord(label.AsUint())].fetch_or(GateBit(label.AsUint()), std::memory_order_release);
  }

  void RecordLabelMarked(storage::LabelId label) noexcept {
    label_marked_gate_[GateWord(label.AsUint())].fetch_or(GateBit(label.AsUint()), std::memory_order_release);
  }

  [[nodiscard]] bool MayHaveLabelTouched(storage::LabelId label) const noexcept {
    return (label_touched_gate_[GateWord(label.AsUint())].load(std::memory_order_acquire) & GateBit(label.AsUint())) !=
           0;
  }

  [[nodiscard]] bool MayHaveLabelMarked(storage::LabelId label) const noexcept {
    return (label_marked_gate_[GateWord(label.AsUint())].load(std::memory_order_acquire) & GateBit(label.AsUint())) !=
           0;
  }

  void RecordPropertyField(storage::Gid gid, storage::PropertyId pid) noexcept {
    pid_gate_[PidWord(pid)].fetch_or(PidBit(pid), std::memory_order_release);
    property_field_.Insert(storage::Gid::FromUint(CombineGidProp(gid, pid)));
  }

  // False if no property field `pid` was ever recorded on this branch.
  [[nodiscard]] bool MayHavePropertyTouched(storage::PropertyId pid) const noexcept {
    return (pid_gate_[PidWord(pid)].load(std::memory_order_acquire) & PidBit(pid)) != 0;
  }

  [[nodiscard]] bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    if (!MayHavePropertyTouched(pid)) return false;
    return property_field_.MayContain(storage::Gid::FromUint(CombineGidProp(gid, pid)));
  }

  // Call for BOTH endpoints of a changed edge: the from-vertex with OUT, the to-vertex with IN. A direction is a key of
  // its own, so an OUT read does not probe a vertex that only gained an IN edge.
  void RecordEdgeChange(storage::Gid endpoint_gid, storage::EdgeDirection direction) noexcept {
    edge_.Insert(EdgeKey(endpoint_gid, direction));
  }

  void RecordVertexDeleted(storage::Gid gid) noexcept {
    any_vertex_deleted_.store(true, std::memory_order_release);
    deleted_.Insert(gid);
  }

  [[nodiscard]] bool MayHaveVertexDeleted(storage::Gid gid) const noexcept {
    return any_vertex_deleted_.load(std::memory_order_acquire) && deleted_.MayContain(gid);
  }

  [[nodiscard]] bool MayHaveAnyVertexDeleted() const noexcept {
    return any_vertex_deleted_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool MayHaveLabelChange(storage::Gid gid) const noexcept { return label_.MayContain(gid); }

  [[nodiscard]] bool MayHavePropertyChange(storage::Gid gid) const noexcept { return property_.MayContain(gid); }

  [[nodiscard]] bool MayHaveEdgeChange(storage::Gid gid, storage::EdgeDirection direction) const noexcept {
    return edge_.MayContain(EdgeKey(gid, direction));
  }

  // Keyed by the EDGE's gid (a different gid space): own filters and pid gate.
  void RecordBaseEdgeProperty(storage::Gid edge_gid, storage::PropertyId pid) noexcept {
    edge_pid_gate_[PidWord(pid)].fetch_or(PidBit(pid), std::memory_order_release);
    base_edge_property_.Insert(edge_gid);
  }

  void RecordBaseEdgeDeleted(storage::Gid edge_gid) noexcept { base_edge_deleted_.Insert(edge_gid); }

  [[nodiscard]] bool MayHaveBaseEdgePropertyChange(storage::Gid edge_gid) const noexcept {
    return base_edge_property_.MayContain(edge_gid);
  }

  [[nodiscard]] bool MayHaveBaseEdgePropertyFieldChange(storage::Gid edge_gid, storage::PropertyId pid) const noexcept {
    if ((edge_pid_gate_[PidWord(pid)].load(std::memory_order_acquire) & PidBit(pid)) == 0) return false;
    return base_edge_property_.MayContain(edge_gid);
  }

  [[nodiscard]] bool MayHaveBaseEdgeDeleted(storage::Gid edge_gid) const noexcept {
    return base_edge_deleted_.MayContain(edge_gid);
  }

 private:
  // Lock-free register-blocked bloom, k=2. Insert is a release fetch_or, MayContain an acquire load.
  class MonotonicGidFilter {
   public:
    explicit MonotonicGidFilter(size_t num_words)
        : index_shift_(64U - static_cast<unsigned>(std::bit_width(num_words) - 1)),
          words_(std::make_unique<std::atomic<uint64_t>[]>(num_words)) {}

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
      // Fibonacci hash; word and both bit positions come from disjoint high bits of the product.
      uint64_t const h = gid.AsUint() * 0x9E3779B97F4A7C15ULL;
      size_t const idx = h >> index_shift_;
      uint64_t const bit_a = 1ULL << ((h >> 46U) & 63U);
      uint64_t const bit_b = 1ULL << ((h >> 40U) & 63U);
      return {idx, bit_a | bit_b};
    }

    unsigned index_shift_;
    std::unique_ptr<std::atomic<uint64_t>[]> words_;
  };

  [[nodiscard]] static storage::Gid EdgeKey(storage::Gid gid, storage::EdgeDirection direction) noexcept {
    return storage::Gid::FromUint((gid.AsUint() << 1U) | (direction == storage::EdgeDirection::IN ? 1U : 0U));
  }

  // A real hash, not an XOR: XOR of small structured integers collides systematically.
  [[nodiscard]] static uint64_t CombineGidProp(storage::Gid gid, storage::PropertyId pid) noexcept {
    return absl::HashOf(gid.AsUint(), pid.AsUint());
  }

  // Aliased ids (id mod 256) only yield false positives.
  static constexpr size_t kPidGateBits = 256;

  [[nodiscard]] static size_t GateWord(uint64_t id) noexcept { return (id % kPidGateBits) >> 6U; }

  [[nodiscard]] static uint64_t GateBit(uint64_t id) noexcept { return 1ULL << (id & 63U); }

  [[nodiscard]] static size_t PidWord(storage::PropertyId pid) noexcept { return GateWord(pid.AsUint()); }

  [[nodiscard]] static uint64_t PidBit(storage::PropertyId pid) noexcept { return GateBit(pid.AsUint()); }

  std::array<std::atomic<uint64_t>, kPidGateBits / 64> pid_gate_{};
  std::array<std::atomic<uint64_t>, kPidGateBits / 64> edge_pid_gate_{};
  std::array<std::atomic<uint64_t>, kPidGateBits / 64> label_touched_gate_{};
  std::array<std::atomic<uint64_t>, kPidGateBits / 64> label_marked_gate_{};
  std::atomic<bool> any_vertex_deleted_{false};
  MonotonicGidFilter property_;
  MonotonicGidFilter label_;
  MonotonicGidFilter edge_;
  MonotonicGidFilter property_field_;
  MonotonicGidFilter deleted_;
  MonotonicGidFilter base_edge_property_;
  MonotonicGidFilter base_edge_deleted_;
};

}  // namespace memgraph::versioning
