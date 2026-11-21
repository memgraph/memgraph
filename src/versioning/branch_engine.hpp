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
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "memory/db_arena_fwd.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/edge_overlay_index.hpp"
#include "versioning/hidden_base_edges.hpp"
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
//
// Glossary:
//  - base object: a vertex or edge that existed on main at the fork point (gid below the ForkPoint floors), read
//    through main's historical accessor.
//  - overlay: the branch-local sparse copy in the diff engine holding only the branch's changes to a base object.
//  - native object: created on the branch, at or above the floors, living only in the diff engine.
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

  // Call the Record* before the overlay write; a rolled-back write leaves the bit set (superset, never a miss).
  void RecordVertexDeleted(storage::Gid gid) noexcept { change_filters_.RecordVertexDeleted(gid); }

  void RecordPropertyFieldChange(storage::Gid gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordPropertyField(gid, pid);
  }

  void RecordBaseEdgeProperty(storage::Gid edge_gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordBaseEdgeProperty(edge_gid, pid);
  }

  void RecordBaseEdgeDeleted(storage::Gid edge_gid) noexcept { change_filters_.RecordBaseEdgeDeleted(edge_gid); }

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

// One session's view of a branch: a shared BranchCore plus per-transaction state. Reads are the base state (main at
// fork_ts, historical(), View::OLD) with the branch's overlays merged over it; none hands out an overlay as the object.
// Holds main's READ storage guard while a transaction is open. One transaction at a time per context.
class BranchContext {
 public:
  using BuildError = BranchCore::BuildError;

  // Unit tests; production attaches to the registry's core.
  static std::expected<std::unique_ptr<BranchContext>, BuildError> BuildFromFork(storage::InMemoryStorage &main,
                                                                                 uint64_t fork_ts);

  explicit BranchContext(std::shared_ptr<BranchCore> core) : core_(std::move(core)) {}

  BranchContext(const BranchContext &) = delete;
  BranchContext &operator=(const BranchContext &) = delete;
  BranchContext(BranchContext &&) = delete;
  BranchContext &operator=(BranchContext &&) = delete;

  ~BranchContext() { EndTransaction(); }

  const std::shared_ptr<BranchCore> &core() const { return core_; }

  storage::InMemoryStorage &main() const { return core_->main(); }

  storage::InMemoryStorage &diff_engine() const { return core_->diff_engine(); }

  uint64_t fork_ts() const { return core_->fork_ts(); }

  // ---- Per-transaction lifecycle

  using HistoricalAccessor = std::unique_ptr<storage::Storage::Accessor>;

  // Ends any open transaction, then opens main at fork_ts; fails, leaving nothing open, if it is no longer pinned.
  // Call BEFORE opening the diff-engine accessor (lock order: main READ -> diff engine), then pass both to
  // BeginTransaction.
  std::expected<HistoricalAccessor, BuildError> OpenHistorical();

  // `diff_txn` is the session's diff-engine accessor, not owned; it must outlive the transaction.
  void BeginTransaction(HistoricalAccessor historical, storage::Storage::Accessor &diff_txn);

  // OpenHistorical, then BeginTransaction, for a caller whose diff-engine accessor already exists.
  std::expected<void, BuildError> BeginTransaction(storage::Storage::Accessor *diff_txn);

  // Idempotent; the caller ends the diff-engine accessor itself.
  void EndTransaction() noexcept { txn_.reset(); }

  bool in_transaction() const { return txn_.has_value(); }

  storage::Storage::Accessor *current_diff_txn() const { return txn_ ? txn_->diff : nullptr; }

  storage::Storage::Accessor &historical() { return *txn().historical; }

  // Below the gid floor: maybe a base object, or main's post-fork one that main@fork_ts lacks. At or above: native.
  bool IsBaseCandidateVertex(storage::Gid gid) const { return gid.AsUint() < core_->vertex_gid_floor(); }

  bool IsBaseCandidateEdge(storage::Gid gid) const { return gid.AsUint() < core_->edge_gid_floor(); }

  // ---- Reads of a vertex or edge accessor, as the branch sees it at `view`

  // `vertex` is a base vertex (historical()) or a native one; a base vertex the branch deleted is DELETED_OBJECT.
  storage::Result<storage::VertexKey> Labels(const storage::VertexAccessor &vertex, storage::View view);
  storage::Result<bool> HasLabel(const storage::VertexAccessor &vertex, storage::LabelId label, storage::View view);
  storage::Result<bool> HasAnyLabel(const storage::VertexAccessor &vertex, storage::View view);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> Properties(
      const storage::VertexAccessor &vertex, storage::View view);
  storage::Result<storage::PropertyValue> GetProperty(const storage::VertexAccessor &vertex, storage::PropertyId key,
                                                      storage::View view);
  storage::Result<uint64_t> GetPropertySize(const storage::VertexAccessor &vertex, storage::PropertyId key,
                                            storage::View view);

  // A base edge hidden on the branch (deleted, or an endpoint deleted) is DELETED_OBJECT.
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> Properties(const storage::EdgeAccessor &edge,
                                                                                    storage::View view);
  storage::Result<storage::PropertyValue> GetProperty(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                                      storage::View view);
  storage::Result<uint64_t> GetPropertySize(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                            storage::View view);

  // Base vertex `gid` deleted on the branch at `view`. False for native gids.
  bool IsVertexDeleted(storage::Gid gid, storage::View view);

  // Skips the overlay probe unless the branch may have deleted something.
  bool IsBaseVertexDeleted(const storage::VertexAccessor &base_vertex, storage::View view);

  // Deleted on the branch, or either endpoint deleted. False for a native edge. `overlay_out` receives the edge overlay
  // if the lookup found one.
  bool IsBaseEdgeHidden(const storage::EdgeAccessor &base_edge, storage::View view,
                        std::optional<storage::EdgeAccessor> *overlay_out = nullptr);

  // A base gid resolves to the base accessor unless the branch deleted it, a native gid to the diff engine's vertex.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // A base gid resolves to the base edge unless hidden, a native gid to the diff engine's edge. Needs the FROM vertex:
  // an edge id names no endpoints and main has no fork-time index by edge id.
  std::optional<storage::EdgeAccessor> ResolveEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Base-state vertex (View::OLD). Null for a native gid, or a candidate main lacks at fork_ts.
  std::optional<storage::VertexAccessor> FindBaseVertex(storage::Gid gid);

  // The base edge out of base vertex `from_gid`, found in its base-state out-adjacency: O(out-degree).
  std::optional<storage::EdgeAccessor> FindBaseEdge(storage::Gid edge_gid, storage::Gid from_gid);

  // Diff-engine-only lookup of a branch-native edge (on a native vertex or an overlay). Null for a base edge gid.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::View view);
  // Same, walking the FROM vertex's adjacency.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Base vertices (minus branch-deleted ones), then native ones, in gid order.
  class UnionVerticesIterable {
   public:
    class Iterator {
     public:
      using difference_type = std::ptrdiff_t;
      using value_type = storage::VertexAccessor;

      Iterator() = default;
      Iterator(storage::VerticesIterable::Iterator hist_it, storage::VerticesIterable::Iterator hist_end,
               storage::VerticesIterable::Iterator diff_it, storage::VerticesIterable::Iterator diff_end,
               BranchContext *ctx, storage::View view);

      storage::VertexAccessor operator*() const { return *current_; }

      Iterator &operator++();

      // Only "reached end" is comparable.
      bool operator==(const Iterator &other) const { return done_ == other.done_; }

     private:
      void SeekNext();

      std::optional<storage::VerticesIterable::Iterator> hist_it_;
      std::optional<storage::VerticesIterable::Iterator> hist_end_;
      std::optional<storage::VerticesIterable::Iterator> diff_it_;
      std::optional<storage::VerticesIterable::Iterator> diff_end_;
      std::optional<storage::VertexAccessor> current_;
      BranchContext *ctx_{nullptr};
      storage::View view_{storage::View::NEW};
      bool done_{true};
    };

    Iterator begin();

    Iterator end() { return Iterator{}; }

   private:
    friend class BranchContext;

    UnionVerticesIterable(storage::VerticesIterable hist_vertices, storage::VerticesIterable diff_vertices,
                          BranchContext *ctx, storage::View view);

    storage::VerticesIterable hist_vertices_;
    storage::VerticesIterable diff_vertices_;
    BranchContext *ctx_;
    storage::View view_;
  };

  UnionVerticesIterable Vertices(storage::View view);

  // All the branch's edges of `vertex`, unfiltered. DELETED_OBJECT when the vertex is deleted at `view`.
  storage::Result<std::vector<storage::EdgeAccessor>> OutEdges(const storage::VertexAccessor &vertex,
                                                               storage::View view);
  storage::Result<std::vector<storage::EdgeAccessor>> InEdges(const storage::VertexAccessor &vertex,
                                                              storage::View view);

  // Base edges (View::OLD) minus those the overlay hides, plus native ones. A deleted base vertex yields none and sets
  // `*vertex_deleted`. `base_hint`, when given, is the held base accessor: a clear branched bit expands it directly.
  // By gid alone, a native vertex deleted in this transaction is not found: use OutEdges/InEdges with an accessor.
  std::vector<storage::EdgeAccessor> ResolveEdges(storage::Gid vertex_gid, storage::EdgeDirection direction,
                                                  storage::View view, bool *vertex_deleted = nullptr,
                                                  const storage::VertexAccessor *base_hint = nullptr);

  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return core_->MayHaveVertexDeleted(gid); }

  bool MayHaveAnyVertexDeleted() const noexcept { return core_->MayHaveAnyVertexDeleted(); }

 private:
  struct Txn {
    Txn(HistoricalAccessor historical_accessor, storage::Storage::Accessor &diff_accessor)
        : historical(std::move(historical_accessor)), diff(&diff_accessor) {}

    HistoricalAccessor historical;     // Owned, non-null.
    storage::Storage::Accessor *diff;  // Not owned, non-null.
  };

  Txn &txn() {
    MG_ASSERT(in_transaction(), "BranchContext: no branch transaction is open.");
    return *txn_;
  }

  storage::InMemoryStorage::InMemoryAccessor &diff() {
    return static_cast<storage::InMemoryStorage::InMemoryAccessor &>(*txn().diff);
  }

  OverlayIndex &overlays() const { return core_->overlay_index(); }

  EdgeOverlayIndex &edge_overlays() const { return core_->edge_overlay_index(); }

  bool IsOverlay(const storage::VertexAccessor &vertex) const {
    return vertex.storage_ == &diff_engine() && IsBaseCandidateVertex(vertex.Gid());
  }

  bool IsOverlay(const storage::EdgeAccessor &edge) const {
    return edge.storage_ == &diff_engine() && IsBaseCandidateEdge(edge.Gid());
  }

  // Main's base vertex `gid` at View::OLD, looked up in the historical accessor.
  std::optional<storage::VertexAccessor> BaseAccessorByGid(storage::Gid gid);

  // Main's base vertex at the already-known `base_vertex` at View::OLD.
  std::optional<storage::VertexAccessor> BaseAccessorByPtr(storage::Vertex *base_vertex);

  // Not probed for visibility: an overlay is never created or deleted by a delta.
  storage::VertexAccessor OverlayAccessor(storage::Vertex *overlay);

  std::optional<storage::VertexAccessor> ReadOverlay(storage::Gid gid);

  bool OverlayDeleted(storage::Vertex *overlay, storage::View view);

  // The overlay a read of `vertex` must consult; null when the filters (`may_differ`, delete) say it cannot matter.
  template <typename MayDiffer>
  std::optional<storage::VertexAccessor> OverlayToRead(const storage::VertexAccessor &vertex, MayDiffer &&may_differ);

  // The base edge's overlay with the base endpoints, or null. Not probed for visibility.
  std::optional<storage::EdgeAccessor> FindEdgeOverlay(const storage::EdgeAccessor &base_edge);

  bool IsBaseEdgeDeleted(const storage::EdgeAccessor &base_edge, storage::View view,
                         std::optional<storage::EdgeAccessor> *overlay_out = nullptr);

  struct EdgeRead {
    enum class Kind : uint8_t { kDirect, kHidden, kOverlay };
    Kind kind{Kind::kDirect};
    std::optional<storage::EdgeAccessor> overlay;
  };

  EdgeRead ResolveEdgeRead(const storage::EdgeAccessor &edge, std::optional<storage::PropertyId> field,
                           storage::View view);

  std::optional<storage::EdgeAccessor> FindEdgeInAnchors(storage::Gid edge_gid, storage::View view);

  storage::Result<std::vector<storage::EdgeAccessor>> Edges(const storage::VertexAccessor &vertex,
                                                            storage::EdgeDirection direction, storage::View view);

  // `overlay`'s native edges in `direction` at `view`.
  std::vector<storage::EdgeAccessor> ReadOverlayNatives(storage::Vertex *overlay, storage::EdgeDirection direction,
                                                        storage::View view);

  // The base edges `candidates` hide at `view`: those whose edge overlay is deleted. A stale candidate (an aborted or
  // not-yet-visible delete) hides nothing. No index lock is held.
  HiddenBaseEdges ReadHiddenBaseEdges(std::span<storage::Edge *const> candidates, storage::View view);

  std::shared_ptr<BranchCore> core_;
  std::optional<Txn> txn_;
};

}  // namespace memgraph::versioning
