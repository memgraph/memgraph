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
#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/branch_change_kind.hpp"
#include "versioning/stub_index.hpp"
#include "versioning/vertex_overlay.hpp"
#include "versioning/vertex_overlay_store.hpp"

namespace memgraph::versioning {

// Visits the ints of a list-valued property, stored either as a generic list or a packed int list.
template <typename PV, typename F>
void ForEachListedInt(PV const &value, F &&visit) {
  if (value.IsList()) {
    for (auto const &elem : value.ValueList()) {
      if (elem.IsInt()) visit(static_cast<int64_t>(elem.ValueInt()));
    }
  } else if (value.IsIntList()) {
    for (auto const v : value.ValueIntList()) visit(static_cast<int64_t>(v));
  }
}

template <typename PV, typename F>
void ForEachRemovedPid(PV const &value, F &&visit) {
  ForEachListedInt(value, [&](int64_t pid) { visit(storage::PropertyId::FromInt(static_cast<int32_t>(pid))); });
}

// Shared per-branch state: one per live branch, shared by every session that has it checked out. Session-mutated
// state lives in the diff engine (MVCC) or behind an atomic / mutex here; per-transaction state is BranchContext's.
//
// The diff engine shares main's NameIdMapper because not-yet-stubbed fork vertices carry main's label/property ids.
// Enum-type ids differ (the enum store is mirrored, not shared); stubs copy no fork values, so none cross over.
//
// A branch's changes to fork vertices (labels, properties, deletes, deleted incident fork edges) live in the
// VertexOverlayStore, a small MVCC of its own that shares the diff engine's clock: a branch transaction's overlay
// snapshot is its diff transaction's start timestamp. The overlay is stamped by the diff commit (a commit callback
// under engine_lock_) or, for an overlay-only transaction, by BranchContext::CommitTransaction under the same lock.
//
// Vertex stubs are empty, delta-less Vertex objects owned by the StubIndex, not entries of the diff engine's vertices_
// (which holds only branch-native vertices). They are anchors only: the adjacency of fork vertices that got a
// branch-created edge. They carry no labels, properties or markers.
//
// Edge stubs mirror vertex stubs: an empty delta-less diff edge with the fork edge's gid, holding only changed
// properties plus the `__mg_removed` pid list for removed ones.
//
// Branch-native creates take gids from main's counters, so gids are unique across main and every branch. `Build`
// records main's counters as floors: a gid below the vertex (edge) floor is a fork vertex (edge), at or above it is
// native.
//
// A deleted fork vertex is an overlay version with the delete flag; a deleted fork edge's gid is listed in the overlay
// version of both endpoints.
//
// Must not outlive the Database's storage (the owning registry is destroyed before storage_).
class BranchCore {
 public:
  // Build fails if fork_ts is no longer pinned.
  struct BuildError {
    std::string message;
  };

  // Edge stubs record removed properties in property `__mg_removed`. The `__mg_` prefix is reserved: query writes of
  // it on a branch are rejected.
  struct LabelInfo {
    bool reserved{false};
  };

  // Builds an empty diff engine, records the gid floors, and mirrors main's index and enum definitions.
  static std::expected<std::shared_ptr<BranchCore>, BuildError> Build(storage::InMemoryStorage &main, uint64_t fork_ts);

  BranchCore(const BranchCore &) = delete;
  BranchCore &operator=(const BranchCore &) = delete;
  BranchCore(BranchCore &&) = delete;
  BranchCore &operator=(BranchCore &&) = delete;
  // Destroys the diff engine before the stub index frees the stubs its deltas and index entries point at.
  ~BranchCore();

  storage::InMemoryStorage &diff_engine() { return *diff_engine_; }

  storage::InMemoryStorage &main() { return *main_; }

  uint64_t fork_ts() const { return fork_ts_; }

  LabelInfo ClassifyLabel(storage::LabelId label);
  bool IsReservedProperty(storage::PropertyId property);

  storage::PropertyId RemovedPropsKey() const { return removed_props_key_; }

  // Main's vertex/edge counters at Build: gids below are fork objects, gids at or above are branch-native.
  uint64_t vertex_gid_floor() const { return vertex_gid_floor_; }

  uint64_t edge_gid_floor() const { return edge_gid_floor_; }

  // Per-kind change filters (branch_change_filter.hpp); lock-free, safe for concurrent sessions.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept {
    change_filters_.RecordVertexChange(gid, kind);
  }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { change_filters_.RecordEdgeChange(endpoint_gid); }

  // Also feeds the exact deleted-gid set read by LabelTouchedSnapshot. Call before the overlay write.
  void RecordVertexDeleted(storage::Gid gid);

  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return change_filters_.MayHaveVertexDeleted(gid); }

  // Exact, insert-only record of fork gids whose membership of `label` may differ on this branch. Call before the
  // overlay write; a rolled-back write leaves the gid recorded (superset, never a miss).
  void RecordLabelTouched(storage::LabelId label, storage::Gid gid);

  // Labels the branch has added to or removed from a fork vertex (a rolled-back write leaves its label listed), for
  // metadata that must list labels the diff engine never sees.
  std::vector<storage::LabelId> TouchedLabels();

  // Immutable set of fork gids that a scan of `label` must resolve: the label's touched gids plus every deleted gid.
  // Cached per label until the next insert, so repeated scans share one copy.
  using LabelTouchedSet = absl::flat_hash_set<uint64_t>;
  std::shared_ptr<const LabelTouchedSet> LabelTouchedSnapshot(storage::LabelId label);

  void RecordPropertyFieldChange(storage::Gid gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordPropertyField(gid, pid);
  }

  // Exact, insert-only record of fork gids whose value of `pid` the branch may have written. Call before the overlay
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

  StubIndex &stub_index() { return stub_index_; }

  VertexOverlayStore &overlay_store() { return overlay_store_; }

  // Snapshot registry for overlay GC. A branch transaction pins a lower bound of its start timestamp BEFORE its diff
  // transaction starts, so GC never samples a horizon above a snapshot that is about to read. All three are called
  // from the session thread; the registry mutex ranks before engine_lock_ (the sampler reads the clock under it).
  uint64_t PinSnapshot();
  void RepinSnapshot(uint64_t from, uint64_t to);
  void UnpinSnapshot(uint64_t ts);

  // Counts a finished overlay-writing transaction; every kGcEveryTxns-th one runs CollectOverlayGarbage. Commit and
  // abort are the only places versions become garbage, so this keeps pace with them without a thread of its own.
  void OnOverlayTxnEnded() noexcept;
  void CollectOverlayGarbage();

 private:
  static constexpr uint64_t kGcEveryTxns = 64;

  BranchCore(storage::InMemoryStorage *main, uint64_t fork_ts, std::unique_ptr<storage::InMemoryStorage> diff_engine,
             uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

  storage::InMemoryStorage *main_;  // Not owned.
  uint64_t fork_ts_;
  uint64_t vertex_gid_floor_;
  uint64_t edge_gid_floor_;
  std::unique_ptr<storage::InMemoryStorage> diff_engine_;

  // Resolved once in the constructor so hot read paths take no lock.
  storage::PropertyId removed_props_key_;

  // Lazy caches over the shared NameIdMapper; entries are immutable once inserted.
  std::mutex marker_mutex_;
  std::unordered_map<storage::LabelId, LabelInfo> label_info_;
  std::unordered_map<storage::PropertyId, bool> reserved_property_;

  BranchChangeFilters change_filters_;

  // Exact per-label touched gids and deleted gids; memory grows with the number of recorded changes. Writes are rare
  // next to scans, hence a plain mutex. Every insert bumps `touched_version_`, invalidating `label_snapshots_`.
  struct CachedLabelSnapshot {
    uint64_t version;
    std::shared_ptr<const LabelTouchedSet> set;
  };

  std::mutex touched_mutex_;
  absl::flat_hash_map<storage::LabelId, LabelTouchedSet> label_touched_;
  LabelTouchedSet deleted_exact_;
  uint64_t touched_version_{1};
  absl::flat_hash_map<storage::LabelId, CachedLabelSnapshot> label_snapshots_;
  absl::flat_hash_map<storage::PropertyId, LabelTouchedSet> property_touched_;
  absl::flat_hash_map<storage::PropertyId, CachedLabelSnapshot> property_snapshots_;

  VertexOverlayStore overlay_store_;
  std::mutex snapshot_mutex_;
  std::multiset<uint64_t> snapshot_pins_;
  std::atomic<uint64_t> overlay_txns_ended_{0};

  // Owns the fork-vertex stubs; ~BranchCore resets diff_engine_ first, so they outlive it.
  StubIndex stub_index_;
};

// One session's view of a branch: a shared BranchCore plus per-transaction state. Reads resolve diff-engine-first,
// falling back to the historical base (main at fork_ts). A fork vertex's changes are written to the overlay store by
// gid; an edge change, or a branch-created edge on a fork vertex, first materializes an empty stub in the diff engine
// (`CowVertex`/`CowEdge`), then mutates it.
//
// The per-transaction slots (`current_diff_txn_`, `historical_`, `overlay_txn_`) are opened by
// CurrentDB::SetupDatabaseTransaction and released by CleanupDBTransaction. The overlay transaction commits only
// through CommitTransaction/PeriodicCommitTransaction; EndTransaction aborts it if it never did, so an abort, a failed
// commit and a dropped context all roll the overlay back exactly once, and a committed overlay is never rolled back.
// Owned by one session and used by one transaction at a time; concurrent sessions each hold their own BranchContext
// over the same core.
//
// historical_ holds main's READ storage guard only while a branch transaction is open.
class BranchContext {
 public:
  using BuildError = BranchCore::BuildError;
  using LabelInfo = BranchCore::LabelInfo;

  // Builds a private, unregistered core and wraps it (unit tests; production attaches to the registry's core).
  static std::expected<std::unique_ptr<BranchContext>, BuildError> BuildFromFork(storage::InMemoryStorage &main,
                                                                                 uint64_t fork_ts);

  // `name` is the branch this context was attached through (empty for test-built contexts); nested sessions use it.
  explicit BranchContext(std::shared_ptr<BranchCore> core, std::string name = {})
      : core_(std::move(core)), name_(std::move(name)) {}

  BranchContext(const BranchContext &) = delete;
  BranchContext &operator=(const BranchContext &) = delete;
  BranchContext(BranchContext &&) = delete;
  BranchContext &operator=(BranchContext &&) = delete;

  ~BranchContext() { EndTransaction(); }

  const std::shared_ptr<BranchCore> &core() const { return core_; }

  const std::string &name() const { return name_; }

  storage::InMemoryStorage &diff_engine() { return core_->diff_engine(); }

  // Valid between OpenHistorical and EndTransaction.
  storage::Storage::Accessor &historical() {
    MG_ASSERT(historical_ != nullptr, "BranchContext::historical: no branch transaction is open.");
    return *historical_;
  }

  // Opens a historical accessor on main at fork_ts (holding main's READ guard) and pins an overlay snapshot lower
  // bound. Fails, leaving nothing open, if fork_ts is no longer pinned. Call BEFORE opening the diff-engine accessor:
  // lock order is main READ -> diff engine, and the pin must precede the diff transaction's start timestamp.
  std::expected<void, BuildError> OpenHistorical();

  // Starts the overlay transaction at the diff transaction's snapshot.
  void SetCurrentDiffTxn(storage::Storage::Accessor *diff_txn);

  // OpenHistorical + SetCurrentDiffTxn, for callers that already hold the diff-engine accessor (tests).
  std::expected<void, BuildError> BeginTransaction(storage::Storage::Accessor *diff_txn) {
    if (auto opened = OpenHistorical(); !opened) return opened;
    SetCurrentDiffTxn(diff_txn);
    return {};
  }

  // Aborts the overlay transaction unless it committed, unpins its snapshot, clears both slots and releases main's
  // READ guard. Idempotent.
  void EndTransaction() noexcept;

  // Commits the diff transaction and the overlay as one unit; same results as Accessor::PrepareForCommitPhase.
  std::expected<void, storage::StorageManipulationError> CommitTransaction(storage::CommitArgs commit_args);

  // As Accessor::PeriodicCommit; on success the overlay continues in a fresh transaction at the new snapshot.
  std::expected<void, storage::StorageManipulationError> PeriodicCommitTransaction(storage::CommitArgs commit_args);

  // Fork gids this transaction has written in the overlay.
  std::vector<uint64_t> OverlayWrittenGids() const;

  storage::Storage::Accessor *current_diff_txn() const { return current_diff_txn_; }

  struct CowError {
    std::string message;
  };

  // Returns the diff engine's vertex for `gid`, materializing an empty delta-less anchor stub if none exists (a fork
  // vertex's labels/properties stay in historical_ and the overlay). CowError if deleted on the branch.
  std::expected<storage::VertexAccessor, CowError> CowVertex(storage::Gid gid);

  // Before an overlay write of fork vertex `gid`: CowError if it is deleted on the branch or missing from the fork
  // state, else sets main's `branched()` hint bit. Allocates no stub.
  std::expected<void, CowError> PrepareForkWrite(storage::Gid gid);

  // Per-session memo over the core's locked caches (entries are immutable there), so hot reads skip its mutex.
  LabelInfo ClassifyLabel(storage::LabelId label) {
    if (auto it = label_info_.find(label); it != label_info_.end()) return it->second;
    return label_info_.emplace(label, core_->ClassifyLabel(label)).first->second;
  }

  bool IsReservedProperty(storage::PropertyId property) {
    if (auto it = reserved_property_.find(property); it != reserved_property_.end()) return it->second;
    return reserved_property_.emplace(property, core_->IsReservedProperty(property)).first->second;
  }

  storage::PropertyId RemovedPropsKey() { return core_->RemovedPropsKey(); }

  // Deletion follows the overlay's MVCC at `view` (own writes, snapshots, rollback). False for native gids (their
  // deletes are ordinary diff-engine deletes) and for fork vertices the branch never deleted.
  bool IsVertexDeleted(storage::Gid gid, storage::View view);
  // `fork_vertex` is a main@fork accessor; a clear branched() bit means the overlay is empty, so the lookup is skipped.
  bool IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view);
  // True when the fork edge is deleted on the branch (gid listed on its FROM or TO overlay) or either fork endpoint is.
  // `fork_edge` may also be the edge's diff-engine copy.
  bool IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view);

  // Gid-keyed access to a fork vertex's branch-side changes. Callers hold the fork accessor (historical_, View::OLD)
  // and never see how the changes are stored; an optional on the read side is empty when the branch changed nothing.
  // Each write is one overlay write; a caller that must report a failed prepare (a delete on this branch) calls
  // PrepareForkWrite first.

  // The overlay version of a fork vertex visible to the current diff txn at `view`; null for a non-fork gid or when the
  // branch changed nothing. Valid only while the branch txn is pinned (OpenHistorical..EndTransaction): use it inside
  // the calling function and never store or return it.
  const OverlayVersion *ReadOverlayVersion(storage::Gid gid, storage::View view);
  // True when the overlay has a version of the fork vertex visible to this transaction.
  bool HasVertexOverlay(storage::Gid gid);

  // The writes return what the matching storage::VertexAccessor call returns, so the query layer wraps them 1:1.
  storage::Result<bool> OverlayAddLabel(storage::Gid gid, storage::LabelId label);
  storage::Result<bool> OverlayRemoveLabel(storage::Gid gid, storage::LabelId label);
  storage::Result<storage::PropertyValue> OverlaySetProperty(storage::Gid gid, storage::PropertyId key,
                                                             const storage::PropertyValue &value);
  storage::Result<bool> OverlayInitProperties(storage::Gid gid,
                                              std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  OverlayUpdateProperties(storage::Gid gid, std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> OverlayClearProperties(storage::Gid gid);
  // False if the vertex was already deleted on the branch. Edges are NOT touched: incident fork edges are hidden by the
  // delete flag, diff-engine edges are the caller's to delete.
  storage::Result<bool> OverlayMarkDeleted(storage::Gid gid);
  // Claims the fork vertex's overlay with a no-op version, so a concurrent delete of it conflicts with the caller.
  storage::Result<bool> OverlayTouch(storage::Gid gid);

  // The fork accessor for a stub accessor (`v` itself for anything else). Only for results that surface a stub, e.g.
  // the endpoints of a branch-native edge or hits of the diff engine's indexes.
  storage::VertexAccessor ForkIdentity(const storage::VertexAccessor &v);
  // Lists the fork edge's gid in both endpoints' overlay and deletes its diff-engine copy, if any. False if already
  // deleted.
  storage::Result<bool> DeleteForkEdge(const storage::EdgeAccessor &fork_edge);

  bool IsForkVertexGid(storage::Gid gid) const;
  bool IsForkEdgeGid(storage::Gid gid) const;

  // Creates a branch-native vertex in the diff engine at a gid taken from main's counter.
  storage::VertexAccessor CreateNativeVertex();
  // Creates a branch-native edge at a gid taken from main's counter; endpoints must already be diff-engine vertices.
  storage::Result<storage::EdgeAccessor> CreateNativeEdge(storage::VertexAccessor *from, storage::VertexAccessor *to,
                                                          storage::EdgeTypeId edge_type);
  // Fork-state vertex (historical_, View::OLD). Uses the stub index's fork pointer when an anchor recorded it.
  std::optional<storage::VertexAccessor> FindForkVertex(storage::Gid gid);
  // The current diff txn's vertex for `gid`: a fork gid resolves only through the stub index, a native gid only
  // through the diff engine's skiplist.
  std::optional<storage::VertexAccessor> FindStub(storage::Gid gid, storage::View view);
  // Diff-engine vertex (stub or native) in the current diff txn; no historical_ fallback.
  std::optional<storage::VertexAccessor> FindStubOrDiffVertex(storage::Gid gid, storage::View view);

  // Returns the diff engine's edge for the fork edge's gid, materializing an empty edge stub between the (COW'd)
  // endpoint stubs if none exists. The stub is not transactional: a rolled-back write leaves it behind harmlessly.
  // It adds no edge to the branch (adjacency emits the fork edge, see ResolveEdges), so no edge change is recorded.
  // CowError if the edge is deleted on the branch.
  std::expected<storage::EdgeAccessor, CowError> CowEdge(const storage::EdgeAccessor &fork_edge);

  // The fork edge (historical_, View::OLD) behind a diff-engine edge stub, found through its endpoints' adjacency.
  std::optional<storage::EdgeAccessor> FindForkEdge(const storage::EdgeAccessor &stub);

  template <typename Accessor>
  bool IsStub(const Accessor &diff_object) const {
    if constexpr (std::is_same_v<Accessor, storage::EdgeAccessor>) {
      return diff_object.storage_ == &core_->diff_engine() && IsForkEdgeGid(diff_object.Gid());
    } else {
      return diff_object.storage_ == &core_->diff_engine() && IsForkVertexGid(diff_object.Gid());
    }
  }

  // True when `stub` carries its own value for `property` or records its removal (the fork edge's value is stale).
  bool StubEdgeTouches(const storage::EdgeAccessor &stub, storage::PropertyId property, storage::View view);

  // Point lookup: a fork gid resolves to its fork accessor unless the branch deleted it, a native gid to the diff
  // engine's vertex.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // Diff-engine-ONLY point lookup by edge gid (no historical_ fallback).
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::View view);
  // Same lookup when the FROM endpoint's gid is known: walks that vertex's adjacency, stub or native, instead of
  // scanning every vertex for the edge.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Gid-ordered streaming union: every historical_ vertex as its fork accessor (dropped if the branch deleted it), then
  // the diff engine's vertices_, which hold only branch-native creates (gids at or above the vertex floor, above every
  // fork gid). One O(H + D) forward pass.
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

      // Only compares "reached end"; mid-iteration positions are not comparable.
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

  // Union of historical_'s incident edges (always View::OLD) with branch-native ones from the diff engine. Edge stubs
  // are skipped: the fork edge is emitted from historical_ and reads overlay the stub. Eager (degree-bounded).
  // Fork edges listed as deleted in `vertex_gid`'s overlay, or whose other endpoint is deleted at `view`, are dropped.
  // A deleted vertex yields no edges and sets `*vertex_deleted`.
  std::vector<storage::EdgeAccessor> ResolveEdges(storage::Gid vertex_gid, storage::EdgeDirection direction,
                                                  storage::View view, bool *vertex_deleted = nullptr);

  // Forwarders to the core's change filters; MayHave* let readers skip the resolve when a kind is definitely unchanged.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept { core_->RecordVertexChange(gid, kind); }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { core_->RecordEdgeChange(endpoint_gid); }

  void RecordVertexDeleted(storage::Gid gid) { core_->RecordVertexDeleted(gid); }

  void RecordLabelTouched(storage::LabelId label, storage::Gid gid) { core_->RecordLabelTouched(label, gid); }

  void RecordPropertyTouched(storage::PropertyId pid, storage::Gid gid) { core_->RecordPropertyTouched(pid, gid); }

  std::shared_ptr<const BranchCore::LabelTouchedSet> PropertyTouchedSnapshot(storage::PropertyId pid) {
    return core_->PropertyTouchedSnapshot(pid);
  }

  std::shared_ptr<const BranchCore::LabelTouchedSet> LabelTouchedSnapshot(storage::LabelId label) {
    return core_->LabelTouchedSnapshot(label);
  }

  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return core_->MayHaveVertexDeleted(gid); }

  void RecordPropertyFieldChange(storage::Gid gid, storage::PropertyId pid) noexcept {
    core_->RecordPropertyFieldChange(gid, pid);
  }

  bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    return core_->MayHavePropertyFieldChange(gid, pid);
  }

  bool MayHaveLabelChange(storage::Gid gid) const noexcept { return core_->MayHaveLabelChange(gid); }

  bool MayHavePropertyChange(storage::Gid gid) const noexcept { return core_->MayHavePropertyChange(gid); }

  bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return core_->MayHaveEdgeChange(gid); }

 private:
  // Sets the monotonic `branched()` hint bit on `main_vertex` under its lock; callers hold no diff-engine lock.
  void MarkMainObjectBranched(storage::Vertex *main_vertex);

  // The overlay version of `gid` visible to this transaction at `view`, or null. Valid until EndTransaction.
  const OverlayVersion *ReadVersion(storage::Gid gid, storage::View view);

  // One store write; a serialization error also poisons the diff transaction, as storage's PrepareForWrite does.
  template <typename Mutator>
  storage::Result<bool> WriteOverlay(storage::Gid gid, Mutator &&mutate);

  // Advances the overlay command id to the diff transaction's.
  void SyncOverlayCommand() noexcept;

  void StartOverlayTransaction();

  // Sets the `branched()` bit on a main@fork edge (its Edge* is main's live object); no-op without properties_on_edges.
  void MarkForkEdgeBranched(const storage::EdgeAccessor &fork_edge);

  std::shared_ptr<BranchCore> core_;
  std::string name_;
  // Session-local memos for ClassifyLabel/IsReservedProperty; unsynchronized (one thread at a time).
  std::unordered_map<storage::LabelId, LabelInfo> label_info_;
  std::unordered_map<storage::PropertyId, bool> reserved_property_;
  // current_diff_txn_ is not owned: it points into CurrentDB::db_transactional_accessor_ for one transaction.
  storage::Storage::Accessor *current_diff_txn_{nullptr};
  std::unique_ptr<storage::Storage::Accessor> historical_;
  // Lives from SetCurrentDiffTxn to EndTransaction; `snapshot_pin_` is its GC pin.
  std::unique_ptr<OverlayTransaction> overlay_txn_;
  std::optional<uint64_t> snapshot_pin_;
  // Last-page cache for StubIndex lookups; one session uses a context at a time. A stale cursor never yields a wrong
  // pointer; reset per transaction.
  StubIndex::Cursor stub_cursor_;
};

}  // namespace memgraph::versioning
