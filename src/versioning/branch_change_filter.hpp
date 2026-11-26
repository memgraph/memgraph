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
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/hash/hash.h"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex_row_filter.hpp"

namespace memgraph::versioning {

// Per-kind gid bloom filters of one branch, shared by its sessions. A miss means the branch never changed that kind on
// that base object, so every mutation site must Record before writing. Insert-only; a rolled-back write leaves its bit.
// Each filter's words are allocated on its first Insert, so every Record* can throw std::bad_alloc, with no overlay
// write done yet; a branch that changed nothing holds none of them.
class BranchChangeFilters {
 public:
  // 32 KiB per filter; power of two.
  static constexpr size_t kFilterWords = 4096;
  static_assert((kFilterWords & (kFilterWords - 1)) == 0, "kFilterWords must be a power of two");

  // The property-field filter is the one that grows with the graph: it holds one entry per (vertex, property) written.
  static constexpr size_t kMaxPropertyFieldWords = size_t{1} << 20;  // 8 MiB
  static_assert((kMaxPropertyFieldWords & (kMaxPropertyFieldWords - 1)) == 0,
                "kMaxPropertyFieldWords must be a power of two");
  static_assert(kMaxPropertyFieldWords >= kFilterWords);

  [[nodiscard]] static constexpr size_t PropertyFieldWords(uint64_t base_vertex_count) noexcept {
    size_t const wanted = std::bit_ceil(static_cast<size_t>(base_vertex_count / 2) | 1U);
    return std::clamp(wanted, kFilterWords, kMaxPropertyFieldWords);
  }

  BranchChangeFilters() : BranchChangeFilters(0) {}

  explicit BranchChangeFilters(uint64_t base_vertex_count)
      : property_(kFilterWords),
        label_(kFilterWords),
        edge_(kFilterWords),
        property_field_(PropertyFieldWords(base_vertex_count)),
        deleted_(kFilterWords),
        vertex_change_(kFilterWords),
        vertex_dropped_(kFilterWords),
        base_edge_property_(kFilterWords),
        base_edge_deleted_(kFilterWords) {}

  BranchChangeFilters(const BranchChangeFilters &) = delete;
  BranchChangeFilters &operator=(const BranchChangeFilters &) = delete;
  BranchChangeFilters(BranchChangeFilters &&) = delete;
  BranchChangeFilters &operator=(BranchChangeFilters &&) = delete;
  ~BranchChangeFilters() = default;

  void RecordLabelChange(storage::Gid gid) {
    label_.Insert(gid);
    vertex_change_.Insert(gid);
    vertex_dropped_.Insert(gid);
  }

  void RecordPropertyChange(storage::Gid gid) { property_.Insert(gid); }

  // Gate (id mod 256, no gid): the label was added to or removed from some base vertex. A clear bit lets label scans
  // and HasLabel skip the per-gid work. A removal needs no gate of its own: an overlay carries the base's full label
  // set.
  void RecordLabelTouched(storage::LabelId label) noexcept {
    label_touched_gate_[GateWord(label.AsUint())].fetch_or(GateBit(label.AsUint()), std::memory_order_release);
  }

  [[nodiscard]] bool MayHaveLabelTouched(storage::LabelId label) const noexcept {
    return (label_touched_gate_[GateWord(label.AsUint())].load(std::memory_order_acquire) & GateBit(label.AsUint())) !=
           0;
  }

  void RecordPropertyField(storage::Gid gid, storage::PropertyId pid) {
    property_field_.Insert(storage::Gid::FromUint(CombineGidProp(gid, pid)));
    pid_gate_[PidWord(pid)].fetch_or(PidBit(pid), std::memory_order_release);
    vertex_change_.Insert(gid);
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
  void RecordEdgeChange(storage::Gid endpoint_gid, storage::EdgeDirection direction) {
    edge_.Insert(EdgeKey(endpoint_gid, direction));
  }

  void RecordVertexDeleted(storage::Gid gid) {
    deleted_.Insert(gid);
    vertex_change_.Insert(gid);
    vertex_dropped_.Insert(gid);
    any_vertex_deleted_.store(true, std::memory_order_release);
  }

  [[nodiscard]] bool MayHaveVertexDeleted(storage::Gid gid) const noexcept {
    return any_vertex_deleted_.load(std::memory_order_acquire) && deleted_.MayContain(gid);
  }

  [[nodiscard]] bool MayHaveAnyVertexDeleted() const noexcept {
    return any_vertex_deleted_.load(std::memory_order_acquire);
  }

  // The views of `MayHaveVertexChange` and `MayHaveVertexDropped` for a scan's inline probe.
  [[nodiscard]] storage::GidBloomProbe VertexDroppedProbe() const noexcept { return vertex_dropped_.Probe(); }

  [[nodiscard]] storage::GidBloomProbe VertexChangeProbe() const noexcept { return vertex_change_.Probe(); }

  // False if the branch never deleted the base vertex, changed its labels or wrote any of its properties: one probe
  // that stands in for the deleted, label and property-field filters. Insert-only, like them.
  [[nodiscard]] bool MayHaveVertexChange(storage::Gid gid) const noexcept { return vertex_change_.MayContain(gid); }

  // False if the branch never deleted the base vertex nor changed its labels: the rows a label scan can lose. A
  // property write does not feed it, so it stays sparse when a branch only rewrites properties.
  [[nodiscard]] bool MayHaveVertexDropped(storage::Gid gid) const noexcept { return vertex_dropped_.MayContain(gid); }

  [[nodiscard]] bool MayHaveLabelChange(storage::Gid gid) const noexcept { return label_.MayContain(gid); }

  [[nodiscard]] bool MayHavePropertyChange(storage::Gid gid) const noexcept { return property_.MayContain(gid); }

  [[nodiscard]] bool MayHaveEdgeChange(storage::Gid gid, storage::EdgeDirection direction) const noexcept {
    return edge_.MayContain(EdgeKey(gid, direction));
  }

  // Keyed by the EDGE's gid (a different gid space): own filters and pid gate.
  void RecordBaseEdgeProperty(storage::Gid edge_gid, storage::PropertyId pid) {
    base_edge_property_.Insert(edge_gid);
    edge_pid_gate_[PidWord(pid)].fetch_or(PidBit(pid), std::memory_order_release);
  }

  void RecordBaseEdgeDeleted(storage::Gid edge_gid) { base_edge_deleted_.Insert(edge_gid); }

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
  using LazyGidFilter = storage::LazyGidFilter;

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
  std::atomic<bool> any_vertex_deleted_{false};
  LazyGidFilter property_;
  LazyGidFilter label_;
  LazyGidFilter edge_;
  LazyGidFilter property_field_;
  LazyGidFilter deleted_;
  LazyGidFilter vertex_change_;
  LazyGidFilter vertex_dropped_;
  LazyGidFilter base_edge_property_;
  LazyGidFilter base_edge_deleted_;
};

}  // namespace memgraph::versioning
