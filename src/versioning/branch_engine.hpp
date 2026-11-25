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
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "memory/db_arena_fwd.hpp"
#include "storage/v2/commit_args.hpp"
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

  // Builds an empty diff engine whose gid floors are `fork`'s, and mirrors main's enum definitions. Durable only with a
  // `branch_id` and a WAL-enabled main: its WAL is then under <main storage dir>/branches/<branch_id>/wal/. The
  // engine has properties_on_edges and heavy edges whatever main's config says, so code that interprets an EdgeRef read
  // from main must consult MAIN's config (`main().config_`). `fork_ts` is the branch's runtime fork pin
  // (Branch::fork_ts()).
  static std::expected<std::shared_ptr<BranchCore>, BuildError> Build(storage::InMemoryStorage &main,
                                                                      const ForkPoint &fork, uint64_t fork_ts,
                                                                      std::optional<uint64_t> branch_id = std::nullopt);

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

  // The base vertices and edges deleted by the committed branch transactions so far.
  ObjectCounts BaseDeletes() const noexcept {
    return {.vertices = deleted_base_vertices_.load(std::memory_order_relaxed),
            .edges = deleted_base_edges_.load(std::memory_order_relaxed)};
  }

  // Adds what a committed branch transaction deleted.
  void AddBaseDeletes(ObjectCounts deleted) noexcept {
    deleted_base_vertices_.fetch_add(deleted.vertices, std::memory_order_relaxed);
    deleted_base_edges_.fetch_add(deleted.edges, std::memory_order_relaxed);
  }

  // Gids below are base objects, gids at or above are branch-native.
  uint64_t vertex_gid_floor() const { return fork_.vertex_gid_floor; }

  uint64_t edge_gid_floor() const { return fork_.edge_gid_floor; }

  // Per-kind change filters (branch_change_filter.hpp); lock-free, safe for concurrent sessions.
  void RecordLabelChange(storage::Gid gid) noexcept { change_filters_.RecordLabelChange(gid); }

  void RecordPropertyChange(storage::Gid gid) noexcept { change_filters_.RecordPropertyChange(gid); }

  void RecordEdgeChange(storage::Gid endpoint_gid, storage::EdgeDirection direction) noexcept {
    change_filters_.RecordEdgeChange(endpoint_gid, direction);
  }

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

  bool MayHaveEdgeChange(storage::Gid gid, storage::EdgeDirection direction) const noexcept {
    return change_filters_.MayHaveEdgeChange(gid, direction);
  }

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

  std::atomic<uint64_t> deleted_base_vertices_{0};
  std::atomic<uint64_t> deleted_base_edges_{0};

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
  static std::expected<std::unique_ptr<BranchContext>, BuildError> BuildFromFork(
      storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<uint64_t> branch_id = std::nullopt);

  // `name` and `id`: the branch this context was attached through (empty/0 for a context attached to none);
  // AttachBranchOf reuses them and refuses a context whose id is not the live branch's.
  BranchContext(std::shared_ptr<BranchCore> core, std::string name, uint64_t id)
      : core_(std::move(core)), name_(std::move(name)), id_(id) {}

  BranchContext(const BranchContext &) = delete;
  BranchContext &operator=(const BranchContext &) = delete;
  BranchContext(BranchContext &&) = delete;
  BranchContext &operator=(BranchContext &&) = delete;

  ~BranchContext() { EndTransaction(); }

  const std::shared_ptr<BranchCore> &core() const { return core_; }

  storage::InMemoryStorage &main() const { return core_->main(); }

  const std::string &name() const { return name_; }

  uint64_t id() const { return id_; }

  storage::InMemoryStorage &diff_engine() const { return core_->diff_engine(); }

  uint64_t fork_ts() const { return core_->fork_ts(); }

  // ---- Per-transaction lifecycle

  using HistoricalAccessor = std::unique_ptr<storage::Storage::Accessor>;

  // Ends any open transaction, then opens main at fork_ts; fails, leaving nothing open, if it is no longer pinned.
  // Call BEFORE opening the diff-engine accessor (lock order: main READ -> diff engine), then pass both to
  // BeginTransaction. `timeout` bounds the wait for main's READ guard (see InMemoryStorage::HistoricalAccess): a
  // session opening a second branch transaction under an open one must pass it, or it can wait forever behind a queued
  // UNIQUE.
  std::expected<HistoricalAccessor, BuildError> OpenHistorical(
      std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  // `diff_txn` is the session's diff-engine accessor, not owned; it must outlive the transaction.
  void BeginTransaction(HistoricalAccessor historical, storage::Storage::Accessor &diff_txn);

  // OpenHistorical, then BeginTransaction, for a caller whose diff-engine accessor already exists.
  std::expected<void, BuildError> BeginTransaction(storage::Storage::Accessor *diff_txn);

  // Idempotent; the caller ends the diff-engine accessor itself.
  void EndTransaction() noexcept { txn_.reset(); }

  // Commits the diff transaction; once committed, the base vertices and edges it deleted join the core's counts. Same
  // results as Accessor::PrepareForCommitPhase.
  std::expected<void, storage::StorageManipulationError> CommitTransaction(storage::CommitArgs commit_args);

  // As Accessor::PeriodicCommit: each batch publishes only the deletes made since the previous one.
  std::expected<void, storage::StorageManipulationError> PeriodicCommitTransaction(storage::CommitArgs commit_args);

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

  // Skips the overlay probe unless the vertex is branched and the filter says the branch may have deleted it.
  bool IsBaseVertexDeleted(const storage::VertexAccessor &base_vertex, storage::View view);

  // Deleted on the branch, or either endpoint deleted. False for a native edge. `overlay_out` receives the edge overlay
  // if the lookup found one.
  bool IsBaseEdgeHidden(const storage::EdgeAccessor &base_edge, storage::View view,
                        std::optional<storage::EdgeAccessor> *overlay_out = nullptr);

  // ---- Writes of a vertex accessor
  //
  // A native vertex takes the stock mutator; a base vertex is written through its overlay (created on first touch,
  // holding the base's labels), DELETED_OBJECT if deleted on the branch. Labels are stock calls on the overlay; a
  // property write is overlay::WriteProperty (a removal stores an explicit null), so the whole-set ops decompose per
  // key. A multi-key failure poisons the diff transaction: abort it.
  storage::Result<bool> AddLabel(storage::VertexAccessor &vertex, storage::LabelId label);
  storage::Result<bool> RemoveLabel(storage::VertexAccessor &vertex, storage::LabelId label);
  storage::Result<storage::PropertyValue> SetProperty(storage::VertexAccessor &vertex, storage::PropertyId key,
                                                      const storage::PropertyValue &value);
  storage::Result<bool> InitProperties(storage::VertexAccessor &vertex,
                                       std::map<storage::PropertyId, storage::PropertyValue> &properties);
  // As storage's: the changes are (key, old, new), and the untouched old properties are merged into `properties`.
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(storage::VertexAccessor &vertex, std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties(
      storage::VertexAccessor &vertex);

  // Marks base vertex `gid` deleted on the branch: false if already, NONEXISTENT_OBJECT if the base state has no such
  // vertex. Without `detach`, as stock DeleteVertex: VERTEX_HAS_EDGES, before anything is written, if a visible base
  // edge or a native edge is attached. With `detach`, each visible base edge is deleted first (DeleteBaseEdgeOnBranch),
  // then the native edges, as stock DetachDeleteVertex. SERIALIZATION_ERROR (poisoning the transaction) if the vertex
  // or an edge is held by a transaction this one cannot write over. The deleted vertex and the edges it hid count as
  // deleted at commit.
  storage::Result<bool> DeleteBaseVertex(storage::Gid gid, bool detach);

  // ---- Writes of an edge accessor
  //
  // `edge` is a base edge (main at fork_ts) or a branch-native one: a native edge takes the stock mutator, a base edge
  // is written through its overlay (created on first touch, with its endpoints' overlays) with the vertex writes'
  // contract. DELETED_OBJECT for a base edge hidden on the branch; PROPERTIES_DISABLED if MAIN has no edge properties
  // (the diff engine always has them, so only main's config says).
  storage::Result<storage::PropertyValue> SetProperty(storage::EdgeAccessor &edge, storage::PropertyId key,
                                                      const storage::PropertyValue &value);
  storage::Result<bool> InitProperties(storage::EdgeAccessor &edge,
                                       std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(const storage::EdgeAccessor &edge,
                   std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties(storage::EdgeAccessor &edge);

  // Deletes base edge `base_edge` on the branch: false if it was already hidden. Only the edge overlay is written, so
  // it conflicts with a concurrent write of the same edge and not with one of an endpoint or of another edge at it.
  // Counts as deleted at commit.
  storage::Result<bool> DeleteBaseEdge(const storage::EdgeAccessor &base_edge);

  // A refusal to write that the user is told about.
  struct BranchWriteError {
    std::string message;
  };

  // The diff engine's object for `gid`: a native vertex, or the overlay of a base vertex (created on first touch,
  // branched bit set) to attach a native edge to. Hand an overlay only to branch writers. BranchWriteError if the
  // vertex was deleted on the branch or is not there at all.
  std::expected<storage::VertexAccessor, BranchWriteError> EnsureDiffVertex(storage::Gid gid);

  // Creates a branch-native vertex in the diff engine at a gid taken from main's counter; the diff engine's own counter
  // is raised past it.
  storage::VertexAccessor CreateNativeVertex();

  // Creates a branch-native edge at a gid taken from main's counter; the endpoints are native vertices or overlays from
  // EnsureDiffVertex. The stock refusals apply: DELETED_OBJECT for a marked-deleted endpoint, SERIALIZATION_ERROR
  // (poisoning the transaction) against a writer of an endpoint.
  storage::Result<storage::EdgeAccessor> CreateNativeEdge(storage::VertexAccessor &from, storage::VertexAccessor &to,
                                                          storage::EdgeTypeId edge_type);

  // CreateNativeEdge at the gid a WAL record names (recovery); same refusals and records.
  storage::Result<storage::EdgeAccessor> CreateNativeEdgeAt(storage::Gid gid, storage::VertexAccessor &from,
                                                            storage::VertexAccessor &to, storage::EdgeTypeId edge_type);

  // A base gid resolves to the base accessor unless the branch deleted it, a native gid to the diff engine's vertex.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // A base gid resolves to the base edge unless hidden, a native gid to the diff engine's edge. Needs the FROM vertex:
  // an edge id names no endpoints and main has no fork-time index by edge id.
  std::optional<storage::EdgeAccessor> ResolveEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Base-state vertex (View::OLD). Null for a native gid, or a candidate main lacks at fork_ts.
  std::optional<storage::VertexAccessor> FindBaseVertex(storage::Gid gid);

  // Lets a caller that re-reads the same base vertices (WAL replay) skip FindBaseVertex's walk of main's post-fork
  // history: the Ptr is main's vertex (null where FindBaseVertex finds none), valid for the branch's life (fork pin).
  // BaseVertexAccessor rebuilds the accessor on the current historical transaction, which OpenHistorical replaces, so
  // never keep an accessor across transactions.
  storage::Vertex *FindBaseVertexPtr(storage::Gid gid);
  storage::VertexAccessor BaseVertexAccessor(storage::Vertex *base_vertex);

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
    // Base vertices and edges deleted since the last periodic commit; published by CommitTransaction.
    ObjectCounts base_deletes;
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

  bool IsNative(const storage::VertexAccessor &vertex) const { return vertex.storage_ == &diff_engine(); }

  bool IsNative(const storage::EdgeAccessor &edge) const { return edge.storage_ == &diff_engine(); }

  // Whether a property write on `object` is refused: an edge's is when MAIN keeps no edge properties (the diff engine
  // always does, so a native edge would take them), a vertex's never.
  bool PropertyWritesDisabled(const storage::VertexAccessor & /*vertex*/) const { return false; }

  bool PropertyWritesDisabled(const storage::EdgeAccessor & /*edge*/) const {
    return !main().config_.salient.items.properties_on_edges;
  }

  void PublishBaseDeletes();

  // Main's base vertex `gid` at View::OLD, looked up in the historical accessor.
  std::optional<storage::VertexAccessor> BaseAccessorByGid(storage::Gid gid);

  // Main's base vertex at the already-known `base_vertex` at View::OLD.
  std::optional<storage::VertexAccessor> BaseAccessorByPtr(storage::Vertex *base_vertex);

  // Not probed for visibility: an overlay is never created or deleted by a delta.
  storage::VertexAccessor OverlayAccessor(storage::Vertex *overlay);

  // Marks main's base vertex branched and returns its overlay as a diff-transaction accessor, creating it on first
  // touch with the base's labels at fork_ts (read through the historical accessor, never from main's live vertex). The
  // caller has checked the vertex is not deleted on the branch. @throw OutOfMemoryException with the overlay already
  // published (a retry finds it).
  storage::Result<storage::VertexAccessor> MaterializeOverlay(const storage::VertexAccessor &base_vertex);

  // MaterializeOverlay for a writer: DELETED_OBJECT if the branch deleted the vertex.
  storage::Result<storage::VertexAccessor> OverlayForWrite(const storage::VertexAccessor &base_vertex);

  // A base edge's overlay with the diff-engine overlays of its endpoints: what overlay::WriteProperty takes.
  struct EdgeOverlay {
    storage::Edge *edge;
    storage::Vertex *from;
    storage::Vertex *to;
    storage::EdgeTypeId type;
  };

  // Sets main's Edge branched bit (when main has edge objects), then returns the edge's overlay and its endpoints'
  // overlays, each created on first touch. @throw OutOfMemoryException with the overlays already published (a retry
  // finds them). For property writes only: a delete goes through DeleteBaseEdgeOnBranch and creates no endpoint
  // overlay.
  storage::Result<EdgeOverlay> MaterializeEdgeOverlay(const storage::EdgeAccessor &base_edge);

  // MaterializeEdgeOverlay for a property writer: PROPERTIES_DISABLED if main has no edge properties, DELETED_OBJECT if
  // the branch hid the edge.
  storage::Result<EdgeOverlay> OverlayForWrite(const storage::EdgeAccessor &base_edge);

  // Marks the base edge's overlay deleted (ListBaseEdgeDelete first), creating the edge overlay and its endpoints'
  // candidate-list entries on first touch. No endpoint overlay is created or written and no labels are read. Filters
  // and branched bits are set before the writes. `sides` are the endpoint lists to add the edge to; a caller that omits
  // one must cover it itself (DeleteBaseVertex, for its own side). False if the edge was already deleted on the branch.
  // SERIALIZATION_ERROR (poisoning the transaction) if the edge is held by a transaction this one cannot write over,
  // which includes one that already deleted this edge since this transaction began.
  storage::Result<bool> DeleteBaseEdgeOnBranch(const storage::EdgeAccessor &base_edge, ListSide sides = kBothSides);

  // The base edges of `base_vertex` the branch still shows at NEW: its base-state edges minus those whose edge overlay
  // is marked deleted, a self-loop once.
  std::vector<storage::EdgeAccessor> VisibleBaseEdges(const storage::VertexAccessor &base_vertex);

  // PrepareForWrite on the overlay under its shared lock; false (poisoning the transaction) on a conflict.
  bool PrepareOverlayForWrite(const storage::VertexAccessor &overlay);
  bool PrepareOverlayForWrite(const EdgeOverlay &overlay);

  // Writes one base vertex or edge property through its overlay (overlay::WriteProperty). Returns the old merged value:
  // the overlay's entry, else the base's.
  storage::Result<storage::PropertyValue> WriteOverlayProperty(const storage::VertexAccessor &base_vertex,
                                                               storage::VertexAccessor &overlay,
                                                               storage::PropertyId key,
                                                               const storage::PropertyValue &value);
  storage::Result<storage::PropertyValue> WriteOverlayProperty(const storage::EdgeAccessor &base_edge,
                                                               const EdgeOverlay &overlay, storage::PropertyId key,
                                                               const storage::PropertyValue &value);

  // What a write of `key` feeds the change filters, recorded before the overlay is written. `RecordPropertiesWrite` is
  // the vertex-wide part, recorded once per primitive; a base edge has only the per-key filters.
  void RecordPropertyWrite(const storage::VertexAccessor &vertex, storage::PropertyId key) {
    core_->RecordPropertyFieldChange(vertex.Gid(), key);
  }

  void RecordPropertyWrite(const storage::EdgeAccessor &edge, storage::PropertyId key) {
    core_->RecordBaseEdgeProperty(edge.Gid(), key);
  }

  void RecordPropertiesWrite(const storage::VertexAccessor &vertex) { core_->RecordPropertyChange(vertex.Gid()); }

  void RecordPropertiesWrite(const storage::EdgeAccessor & /*edge*/) {}

  // Property writes shared by vertices and edges; `Object` is a base or native accessor (const where stock's is).
  template <typename Object>
  storage::Result<storage::PropertyValue> SetPropertyAt(Object &object, storage::PropertyId key,
                                                        const storage::PropertyValue &value);
  template <typename Object>
  storage::Result<bool> InitPropertiesAt(Object &object,
                                         std::map<storage::PropertyId, storage::PropertyValue> &properties);
  template <typename Object>
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdatePropertiesAt(Object &object, std::map<storage::PropertyId, storage::PropertyValue> &properties);
  template <typename Object>
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearPropertiesAt(Object &object);

  std::optional<storage::VertexAccessor> ReadOverlay(storage::Gid gid);

  // Whether the branch deleted the base vertex `overlay` stands for, as of `view`.
  bool Deleted(storage::Vertex *overlay, storage::View view);

  // The overlay of `vertex` a read must consult, or null when the filters (`may_differ`, delete) say it cannot matter.
  template <typename MayDiffer>
  storage::Vertex *OverlayPtrToRead(const storage::VertexAccessor &vertex, MayDiffer &&may_differ);

  // OverlayPtrToRead as an accessor.
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
  std::string name_;
  uint64_t id_;
  std::optional<Txn> txn_;
};

}  // namespace memgraph::versioning
