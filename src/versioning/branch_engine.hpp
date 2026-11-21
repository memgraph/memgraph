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

#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "memory/db_arena_fwd.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/edge_overlay_index.hpp"
#include "versioning/overlay_index.hpp"

namespace memgraph::versioning {

// The diff engine's config: main's, with durability, auto-creation and per-commit extras switched off.
// `live_mode` is main's current storage mode, which may differ from its startup config.
storage::Config DiffEngineConfig(const storage::Config &main_config, storage::StorageMode live_mode);

// Shared per-branch state, one per live branch and shared by every session that has it checked out. Session writes live
// in the diff engine (MVCC) or in the lock-free filters here. The diff engine shares main's NameIdMapper, so names have
// one id on both sides. Changes to base objects are MVCC deltas on overlays owned by the overlay indexes, not entries
// of the diff engine's vertices_/edges_. Native gids come from main's counters (plain fetch_add); the branch's
// ForkPoint floors split the gid space: below = base object, at or above = native. Must not outlive the Database's
// storage.
class BranchCore {
 public:
  // Build fails if fork_ts is no longer pinned.
  struct BuildError {
    std::string message;
  };

  // Builds an empty, non-durable diff engine whose gid floors are `fork`'s, and mirrors main's enum definitions. The
  // engine has properties_on_edges and heavy edges whatever main's config says, so code that interprets an EdgeRef read
  // from main must consult MAIN's config (`main().config_`). `fork_ts` is the branch's runtime fork pin
  // (Branch::fork_ts()).
  static std::expected<std::shared_ptr<BranchCore>, BuildError> Build(storage::InMemoryStorage &main,
                                                                      const ForkPoint &fork, uint64_t fork_ts);

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

  const ForkPoint &fork() const { return fork_; }

  // Gids below are base objects, gids at or above are branch-native.
  uint64_t vertex_gid_floor() const { return fork_.vertex_gid_floor; }

  uint64_t edge_gid_floor() const { return fork_.edge_gid_floor; }

  // Per-kind change filters (branch_change_filter.hpp); lock-free, safe for concurrent sessions.
  void RecordLabelChange(storage::Gid gid) noexcept { change_filters_.RecordLabelChange(gid); }

  void RecordPropertyChange(storage::Gid gid) noexcept { change_filters_.RecordPropertyChange(gid); }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { change_filters_.RecordEdgeChange(endpoint_gid); }

  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return change_filters_.MayHaveVertexDeleted(gid); }

  bool MayHaveAnyVertexDeleted() const noexcept { return change_filters_.MayHaveAnyVertexDeleted(); }

  // Clear: the branch never added or removed the label on a base vertex.
  void RecordLabelTouched(storage::LabelId label) noexcept { change_filters_.RecordLabelTouched(label); }

  bool MayHaveLabelTouched(storage::LabelId label) const noexcept { return change_filters_.MayHaveLabelTouched(label); }

  // Clear: no property field `pid` was ever recorded on this branch.
  bool MayHavePropertyTouched(storage::PropertyId pid) const noexcept {
    return change_filters_.MayHavePropertyTouched(pid);
  }

  bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    return change_filters_.MayHavePropertyFieldChange(gid, pid);
  }

  bool MayHaveLabelChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveLabelChange(gid); }

  bool MayHavePropertyChange(storage::Gid gid) const noexcept { return change_filters_.MayHavePropertyChange(gid); }

  bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveEdgeChange(gid); }

  // Base-edge filters, keyed by edge gid.
  bool MayHaveBaseEdgePropertyChange(storage::Gid edge_gid) const noexcept {
    return change_filters_.MayHaveBaseEdgePropertyChange(edge_gid);
  }

  bool MayHaveBaseEdgePropertyFieldChange(storage::Gid edge_gid, storage::PropertyId pid) const noexcept {
    return change_filters_.MayHaveBaseEdgePropertyFieldChange(edge_gid, pid);
  }

  bool MayHaveBaseEdgeDeleted(storage::Gid edge_gid) const noexcept {
    return change_filters_.MayHaveBaseEdgeDeleted(edge_gid);
  }

  OverlayIndex &overlay_index() { return overlay_index_; }

  EdgeOverlayIndex &edge_overlay_index() { return edge_overlay_index_; }

 private:
  BranchCore(storage::InMemoryStorage *main, const ForkPoint &fork, uint64_t fork_ts,
             std::unique_ptr<storage::InMemoryStorage> diff_engine);

  // Emplaced by ~BranchCore and destroyed last: the overlays go back to main's arenas whichever thread drops the last
  // reference.
  std::optional<memory::DbArenaScope> teardown_scope_;
  storage::InMemoryStorage *main_;  // Not owned.
  ForkPoint fork_;
  uint64_t fork_ts_;
  std::unique_ptr<storage::InMemoryStorage> diff_engine_;

  BranchChangeFilters change_filters_;

  // Own the overlays the diff engine's deltas and indexes point at. Declared after diff_engine_ and teardown_scope_, so
  // they are destroyed before the scope and after the engine, which ~BranchCore resets first.
  OverlayIndex overlay_index_;
  EdgeOverlayIndex edge_overlay_index_;
};

}  // namespace memgraph::versioning
