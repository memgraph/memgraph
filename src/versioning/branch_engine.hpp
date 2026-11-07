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
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "memory/db_arena_fwd.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/db_aware_allocator.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/edge_stub_index.hpp"
#include "versioning/index_key_parts.hpp"
#include "versioning/stub_index.hpp"

namespace memgraph::versioning {

// Shared per-branch state: one per live branch, shared by every session that has it checked out. Session-mutated
// state lives in the diff engine (MVCC) or behind an atomic / mutex here.
//
// The diff engine shares main's NameIdMapper: fork objects read from main@fork_ts carry main's ids, so every name has
// the same id on both sides. The stub markers' reserved ids (stub_state.hpp) are not names: the mapper cannot resolve
// them, and the WAL encoder strips them before it looks a name up. Enum-type ids differ (the enum store is mirrored,
// not shared).
//
// A branch's changes to fork vertices and fork edges (labels, properties, deletes) are written as stock MVCC deltas
// on stubs: empty Vertex / Edge objects owned by the StubIndex / EdgeStubIndex, not entries of the diff engine's
// vertices_ / edges_.
//
// Branch-native creates take gids from main's counters, so gids are unique across main and every branch. `Build`
// records main's counters as floors: a gid below the vertex (edge) floor is a fork vertex (edge), at or above it is
// native.
//
// Must not outlive the Database's storage (the owning registry is destroyed before storage_).
class BranchCore {
 public:
  // Build fails if fork_ts is no longer pinned.
  struct BuildError {
    std::string message;
  };

  // Gid floors recorded before a restart; Build uses them instead of main's current counters.
  struct RecordedFloors {
    uint64_t vertex_gid_floor;
    uint64_t edge_gid_floor;
  };

  // Builds an empty, non-durable diff engine, records the gid floors (main's counters unless `recorded` supplies them)
  // and mirrors main's enum definitions. The engine always has properties_on_edges: fork-edge stubs are heavy Edge
  // objects whatever main's config says, so any code that interprets an EdgeRef read from main must consult MAIN's
  // config (`main().config_`), not the diff engine's.
  static std::expected<std::shared_ptr<BranchCore>, BuildError> Build(
      storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<RecordedFloors> recorded = std::nullopt);

  BranchCore(const BranchCore &) = delete;
  BranchCore &operator=(const BranchCore &) = delete;
  BranchCore(BranchCore &&) = delete;
  BranchCore &operator=(BranchCore &&) = delete;
  ~BranchCore();

  storage::InMemoryStorage &diff_engine() { return *diff_engine_; }

  storage::InMemoryStorage &main() { return *main_; }

  uint64_t fork_ts() const { return fork_ts_; }

  // Resolves text a user typed on a branch, on main's mapper (the diff engine's is the same one).
  storage::LabelId NameToLabel(std::string_view name) { return main_->NameToLabel(name); }

  storage::PropertyId NameToProperty(std::string_view name) { return main_->NameToProperty(name); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) { return main_->NameToEdgeType(name); }

  // Main's vertex/edge counters at Build: gids below are fork objects, gids at or above are branch-native.
  uint64_t vertex_gid_floor() const { return vertex_gid_floor_; }

  uint64_t edge_gid_floor() const { return edge_gid_floor_; }

  // The parts of the label+property keys of the indexes the diff engine mirrors; empty while it mirrors none.
  const IndexKeyParts &key_parts() const noexcept { return key_parts_; }

  // Per-kind change filters (branch_change_filter.hpp); lock-free, safe for concurrent sessions.
  void RecordLabelChange(storage::Gid gid) noexcept { change_filters_.RecordLabelChange(gid); }

  void RecordPropertyChange(storage::Gid gid) noexcept { change_filters_.RecordPropertyChange(gid); }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { change_filters_.RecordEdgeChange(endpoint_gid); }

  // Also feeds the exact deleted-gid set read by LabelTouchedSnapshot. Call before the stub write.
  void RecordVertexDeleted(storage::Gid gid);

  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return change_filters_.MayHaveVertexDeleted(gid); }

  bool MayHaveAnyVertexDeleted() const noexcept { return change_filters_.MayHaveAnyVertexDeleted(); }

  // Lock-free emptiness gates for the touched sets (monotonic bloom over id mod 256). A txn visible to a reader set the
  // bit (release, in Record*Touched before its commit), and the reader observes that commit only via timestamp
  // synchronisation, so a clear bit (acquire) equals an empty snapshot. False "maybe" only costs the snapshot path.
  bool MayHaveLabelTouched(storage::LabelId label) const noexcept {
    return (label_touched_gate_[GateWord(label.AsUint())].load(std::memory_order_acquire) & GateBit(label.AsUint())) !=
           0;
  }

  bool MayHavePropertyTouched(storage::PropertyId pid) const noexcept {
    return (property_touched_gate_[GateWord(pid.AsUint())].load(std::memory_order_acquire) & GateBit(pid.AsUint())) !=
           0;
  }

  // Exact, insert-only record of fork gids whose membership of `label` may differ on this branch. Call before the
  // stub write; a rolled-back write leaves the gid recorded (superset, never a miss).
  void RecordLabelTouched(storage::LabelId label, storage::Gid gid);

  // Immutable set of fork gids that a scan of `label` must resolve: the label's touched gids plus every deleted gid.
  // Cached per label until the next insert, so repeated scans share one copy.
  using LabelTouchedSet =
      absl::flat_hash_set<uint64_t, absl::Hash<uint64_t>, std::equal_to<>, memory::DbAwareAllocator<uint64_t>>;
  std::shared_ptr<const LabelTouchedSet> LabelTouchedSnapshot(storage::LabelId label);

  void RecordPropertyFieldChange(storage::Gid gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordPropertyField(gid, pid);
  }

  // Exact, insert-only record of fork gids whose value of `pid` the branch may have written. Call before the stub
  // write; a rolled-back write leaves the gid recorded.
  void RecordPropertyTouched(storage::PropertyId pid, storage::Gid gid);

  // Immutable set of fork gids that a scan on `pid` must resolve, cached per property until the next insert.
  std::shared_ptr<const LabelTouchedSet> PropertyTouchedSnapshot(storage::PropertyId pid);

  bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    return change_filters_.MayHavePropertyFieldChange(gid, pid);
  }

  bool MayHaveLabelChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveLabelChange(gid); }

  bool MayHavePropertyChange(storage::Gid gid) const noexcept { return change_filters_.MayHavePropertyChange(gid); }

  bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveEdgeChange(gid); }

  // Fork-edge filters (keyed by edge gid); call the Record* before the stub write.
  void RecordForkEdgeProperty(storage::Gid edge_gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordForkEdgeProperty(edge_gid, pid);
  }

  void RecordForkEdgeDeleted(storage::Gid edge_gid) noexcept { change_filters_.RecordForkEdgeDeleted(edge_gid); }

  bool MayHaveForkEdgePropertyChange(storage::Gid edge_gid) const noexcept {
    return change_filters_.MayHaveForkEdgePropertyChange(edge_gid);
  }

  bool MayHaveForkEdgePropertyFieldChange(storage::Gid edge_gid, storage::PropertyId pid) const noexcept {
    return change_filters_.MayHaveForkEdgePropertyFieldChange(edge_gid, pid);
  }

  bool MayHaveForkEdgeDeleted(storage::Gid edge_gid) const noexcept {
    return change_filters_.MayHaveForkEdgeDeleted(edge_gid);
  }

  // Exact, insert-only record of the fork edges whose value of `pid` the branch may have written, each with its FROM
  // vertex gid (to find the fork edge). Call before the stub write; a rolled-back write leaves the edge recorded.
  void RecordEdgePropertyTouched(storage::PropertyId pid, storage::Gid edge_gid, storage::Gid from_gid);

  // Immutable edge -> FROM vertex map that a scan on `pid` must resolve, cached per property until the next insert.
  using EdgeTouchedMap = absl::flat_hash_map<uint64_t, uint64_t, absl::Hash<uint64_t>, std::equal_to<>,
                                             memory::DbAwareAllocator<std::pair<const uint64_t, uint64_t>>>;
  std::shared_ptr<const EdgeTouchedMap> EdgePropertyTouchedSnapshot(storage::PropertyId pid);

  StubIndex &stub_index() { return stub_index_; }

  EdgeStubIndex &edge_stub_index() { return edge_stub_index_; }

 private:
  BranchCore(storage::InMemoryStorage *main, uint64_t fork_ts, std::unique_ptr<storage::InMemoryStorage> diff_engine,
             uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

  // Emplaced by ~BranchCore and destroyed last: the stubs go back to main's arenas whichever thread drops the last
  // reference.
  std::optional<memory::DbArenaScope> teardown_scope_;
  storage::InMemoryStorage *main_;  // Not owned.
  uint64_t fork_ts_;
  uint64_t vertex_gid_floor_;
  uint64_t edge_gid_floor_;
  // Set once by Build, before the core is shared.
  IndexKeyParts key_parts_;
  std::unique_ptr<storage::InMemoryStorage> diff_engine_;

  BranchChangeFilters change_filters_;

  // Exact per-label touched gids and deleted gids; memory grows with the number of recorded changes. Writes are rare
  // next to scans, hence a plain mutex. Every insert bumps `touched_version_`, invalidating the snapshot caches.
  struct CachedLabelSnapshot {
    uint64_t version;
    std::shared_ptr<const LabelTouchedSet> set;
  };

  static constexpr size_t kTouchedGateBits = 256;

  static size_t GateWord(uint64_t id) noexcept { return (id % kTouchedGateBits) >> 6U; }

  static uint64_t GateBit(uint64_t id) noexcept { return 1ULL << (id & 63U); }

  std::array<std::atomic<uint64_t>, kTouchedGateBits / 64> label_touched_gate_{};
  std::array<std::atomic<uint64_t>, kTouchedGateBits / 64> property_touched_gate_{};

  std::mutex touched_mutex_;
  absl::flat_hash_map<storage::LabelId, LabelTouchedSet> label_touched_;
  LabelTouchedSet deleted_exact_;
  uint64_t touched_version_{1};
  absl::flat_hash_map<storage::LabelId, CachedLabelSnapshot> label_snapshots_;
  absl::flat_hash_map<storage::PropertyId, LabelTouchedSet> property_touched_;
  absl::flat_hash_map<storage::PropertyId, CachedLabelSnapshot> property_snapshots_;

  struct CachedEdgeSnapshot {
    uint64_t version;
    std::shared_ptr<const EdgeTouchedMap> map;
  };

  absl::flat_hash_map<storage::PropertyId, EdgeTouchedMap> edge_property_touched_;
  absl::flat_hash_map<storage::PropertyId, CachedEdgeSnapshot> edge_property_snapshots_;

  // Own the stubs the diff engine's deltas and indexes point at. Declared after diff_engine_ and teardown_scope_, so
  // they are destroyed before the scope and after the engine, which ~BranchCore resets first.
  StubIndex stub_index_;
  EdgeStubIndex edge_stub_index_;
};

}  // namespace memgraph::versioning
