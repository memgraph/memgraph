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
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

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

namespace memgraph::versioning {

// Visits the ints of a list-valued marker property, stored either as a generic list or a packed int list.
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
// Edge stubs mirror vertex stubs: an empty delta-less diff edge with the fork edge's gid, holding only changed
// properties plus the `__mg_removed` pid list for removed ones.
//
// Branch-native creates take gids from main's counters, so gids are unique across main and every branch. `Build`
// records main's counters as floors: a gid below the vertex (edge) floor is a fork vertex (edge), at or above it is
// native.
//
// Deleted fork objects are recorded in the diff engine: the vertex stub carries label `__mg_deleted`; a deleted fork
// edge's gid is listed in property `__mg_deleted_edges` on both endpoint stubs.
//
// Must not outlive the Database's storage (the owning registry is destroyed before storage_).
class BranchCore {
 public:
  // Build fails if fork_ts is no longer pinned.
  struct BuildError {
    std::string message;
  };

  // Removal markers on a fork stub: label L removed => stub label `__mg_rm:L`; properties removed => stub property
  // `__mg_removed` listing their ids. The `__mg_` prefix is reserved: query writes of it on a branch are rejected.
  struct LabelInfo {
    bool reserved{false};
    // Set iff the label is a removal marker: the label it removes.
    std::optional<storage::LabelId> removed_label;
  };

  // Builds an empty diff engine, records the gid floors, and mirrors main's index and enum definitions.
  static std::expected<std::shared_ptr<BranchCore>, BuildError> Build(storage::InMemoryStorage &main, uint64_t fork_ts);

  BranchCore(const BranchCore &) = delete;
  BranchCore &operator=(const BranchCore &) = delete;
  BranchCore(BranchCore &&) = delete;
  BranchCore &operator=(BranchCore &&) = delete;
  ~BranchCore() = default;

  storage::InMemoryStorage &diff_engine() { return *diff_engine_; }

  storage::InMemoryStorage &main() { return *main_; }

  uint64_t fork_ts() const { return fork_ts_; }

  LabelInfo ClassifyLabel(storage::LabelId label);
  storage::LabelId RemovalMarker(storage::LabelId label);
  bool IsReservedProperty(storage::PropertyId property);

  storage::PropertyId RemovedPropsKey() const { return removed_props_key_; }

  storage::LabelId DeletedLabel() const { return deleted_label_; }

  storage::PropertyId DeletedEdgesKey() const { return deleted_edges_key_; }

  // Main's vertex/edge counters at Build: gids below are fork objects, gids at or above are branch-native.
  uint64_t vertex_gid_floor() const { return vertex_gid_floor_; }

  uint64_t edge_gid_floor() const { return edge_gid_floor_; }

  // Per-kind change filters (branch_change_filter.hpp); lock-free, safe for concurrent sessions.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept {
    change_filters_.RecordVertexChange(gid, kind);
  }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { change_filters_.RecordEdgeChange(endpoint_gid); }

  void RecordVertexDeleted(storage::Gid gid) noexcept { change_filters_.RecordVertexDeleted(gid); }

  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return change_filters_.MayHaveVertexDeleted(gid); }

  void RecordPropertyFieldChange(storage::Gid gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordPropertyField(gid, pid);
  }

  bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    return change_filters_.MayHavePropertyFieldChange(gid, pid);
  }

  bool MayHaveLabelChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveLabelChange(gid); }

  bool MayHavePropertyChange(storage::Gid gid) const noexcept { return change_filters_.MayHavePropertyChange(gid); }

  bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveEdgeChange(gid); }

  StubIndex &stub_index() { return stub_index_; }

 private:
  BranchCore(storage::InMemoryStorage *main, uint64_t fork_ts, std::unique_ptr<storage::InMemoryStorage> diff_engine,
             uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

  storage::InMemoryStorage *main_;  // Not owned.
  uint64_t fork_ts_;
  uint64_t vertex_gid_floor_;
  uint64_t edge_gid_floor_;
  std::unique_ptr<storage::InMemoryStorage> diff_engine_;

  // Resolved once in the constructor so hot read paths take no lock.
  storage::PropertyId removed_props_key_;
  storage::LabelId deleted_label_;
  storage::PropertyId deleted_edges_key_;

  // Lazy caches over the shared NameIdMapper; entries are immutable once inserted.
  std::mutex marker_mutex_;
  std::unordered_map<storage::LabelId, LabelInfo> label_info_;
  std::unordered_map<storage::LabelId, storage::LabelId> removal_marker_;
  std::unordered_map<storage::PropertyId, bool> reserved_property_;

  BranchChangeFilters change_filters_;

  // Declared last: destroyed before the diff engine whose Vertex objects it points into.
  StubIndex stub_index_;
};

// One session's view of a branch: a shared BranchCore plus per-transaction state. Reads resolve diff-engine-first,
// falling back to the historical base (main at fork_ts). A write first materializes an empty stub of the touched
// object in the diff engine (`CowVertex`/`CowEdge`), then mutates it.
//
// The per-transaction slots (`current_diff_txn_`, `historical_`) are opened by CurrentDB::SetupDatabaseTransaction
// and released by CleanupDBTransaction. Owned by one session and used by one transaction at a time; concurrent
// sessions each hold their own BranchContext over the same core.
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

  ~BranchContext() = default;

  const std::shared_ptr<BranchCore> &core() const { return core_; }

  const std::string &name() const { return name_; }

  storage::InMemoryStorage &diff_engine() { return core_->diff_engine(); }

  // Valid between OpenHistorical and EndTransaction.
  storage::Storage::Accessor &historical() {
    MG_ASSERT(historical_ != nullptr, "BranchContext::historical: no branch transaction is open.");
    return *historical_;
  }

  // Opens a historical accessor on main at fork_ts (holding main's READ guard). Fails, leaving nothing open, if
  // fork_ts is no longer pinned. Call BEFORE opening the diff-engine accessor: lock order is main READ -> diff engine.
  std::expected<void, BuildError> OpenHistorical();

  void SetCurrentDiffTxn(storage::Storage::Accessor *diff_txn) { current_diff_txn_ = diff_txn; }

  // OpenHistorical + SetCurrentDiffTxn, for callers that already hold the diff-engine accessor (tests).
  std::expected<void, BuildError> BeginTransaction(storage::Storage::Accessor *diff_txn) {
    if (auto opened = OpenHistorical(); !opened) return opened;
    SetCurrentDiffTxn(diff_txn);
    return {};
  }

  // Clears both slots and releases main's READ guard. Idempotent.
  void EndTransaction() noexcept {
    current_diff_txn_ = nullptr;
    historical_.reset();
  }

  storage::Storage::Accessor *current_diff_txn() const { return current_diff_txn_; }

  struct CowError {
    std::string message;
  };

  // Returns the diff engine's vertex for `gid`, materializing an empty delta-less stub if none exists (fork
  // labels/properties stay in historical_). CowError if deleted on the branch.
  std::expected<storage::VertexAccessor, CowError> CowVertex(storage::Gid gid);

  LabelInfo ClassifyLabel(storage::LabelId label) { return core_->ClassifyLabel(label); }

  storage::LabelId RemovalMarker(storage::LabelId label) { return core_->RemovalMarker(label); }

  bool IsReservedProperty(storage::PropertyId property) { return core_->IsReservedProperty(property); }

  storage::PropertyId RemovedPropsKey() { return core_->RemovedPropsKey(); }

  storage::LabelId DeletedLabel() { return core_->DeletedLabel(); }

  storage::PropertyId DeletedEdgesKey() { return core_->DeletedEdgesKey(); }

  // Read from the stub as visible at `view`, so deletion follows diff-engine MVCC (own writes, snapshots, rollback).
  bool IsStubDeleted(const storage::VertexAccessor &stub, storage::View view);
  // False for native gids (their deletes are ordinary diff-engine deletes) and for fork vertices without a stub.
  bool IsVertexDeleted(storage::Gid gid, storage::View view);
  // `fork_vertex` is a main@fork accessor; a clear branched() bit means no stub exists, so the lookup is skipped.
  bool IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view);
  // True when the fork edge is deleted on the branch (gid listed on its FROM stub) or either fork endpoint is.
  // `fork_edge` may also be the edge's diff-engine copy.
  bool IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view);
  // Ascending gids of the fork edges listed as deleted on `stub`.
  std::vector<uint64_t> DeletedEdgeGids(const storage::VertexAccessor &stub, storage::View view);
  storage::Result<bool> StoreDeletedEdgeGids(storage::VertexAccessor &stub, const std::vector<uint64_t> &gids);

  // Marks the fork vertex's stub (materialized if needed) with `__mg_deleted`; returns nullopt if already deleted.
  // Edges are NOT touched: incident fork edges are hidden by the marker, diff-engine edges are the caller's to delete.
  storage::Result<std::optional<storage::VertexAccessor>> DeleteForkVertex(storage::Gid gid);
  // Lists the fork edge's gid on both endpoint stubs and deletes its diff-engine copy, if any. False if already
  // deleted.
  storage::Result<bool> DeleteForkEdge(const storage::EdgeAccessor &fork_edge);

  bool IsForkVertexGid(storage::Gid gid) const;
  bool IsForkEdgeGid(storage::Gid gid) const;

  // Creates a branch-native vertex in the diff engine at a gid taken from main's counter.
  storage::VertexAccessor CreateNativeVertex();
  // Creates a branch-native edge at a gid taken from main's counter; endpoints must already be diff-engine vertices.
  storage::Result<storage::EdgeAccessor> CreateNativeEdge(storage::VertexAccessor *from, storage::VertexAccessor *to,
                                                          storage::EdgeTypeId edge_type);
  // Fork-state vertex (historical_, View::OLD). Uses the stub index's fork pointer when recorded, else historical_.
  std::optional<storage::VertexAccessor> FindForkVertex(storage::Gid gid);
  // The current diff txn's vertex for `gid` (stub or native), via the stub index when recorded, else the diff engine's
  // skiplist; equivalent to current_diff_txn()->FindVertex(gid, view).
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

  // Diff-engine-first point lookup, falling back to historical_; a fork vertex whose stub is deleted never resolves.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // Diff-engine-ONLY point lookup by edge gid (no historical_ fallback).
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::View view);
  // Same lookup when the FROM endpoint's gid is known: avoids the diff engine's all-vertex scan for the edge.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Gid-ordered streaming union of historical_ (a COW'd gid yields the diff engine's copy) with the diff engine's
  // vertices (branch-native creates). Both sides are gid-ordered, so it is one O(H + D) forward pass.
  class UnionVerticesIterable {
   public:
    class Iterator {
     public:
      using difference_type = std::ptrdiff_t;
      using value_type = storage::VertexAccessor;

      Iterator() = default;
      Iterator(storage::VerticesIterable::Iterator hist_it, storage::VerticesIterable::Iterator hist_end,
               storage::VerticesIterable::Iterator diff_it, storage::VerticesIterable::Iterator diff_end,
               storage::LabelId deleted_label, uint64_t vertex_gid_floor, storage::View view);

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
      storage::LabelId deleted_label_{storage::kInvalidLabelId};
      uint64_t vertex_gid_floor_{0};
      storage::View view_{storage::View::NEW};
      bool done_{true};
    };

    Iterator begin();

    Iterator end() { return Iterator{}; }

   private:
    friend class BranchContext;

    UnionVerticesIterable(storage::VerticesIterable hist_vertices, storage::VerticesIterable diff_vertices,
                          storage::LabelId deleted_label, uint64_t vertex_gid_floor, storage::View view);

    storage::VerticesIterable hist_vertices_;
    storage::VerticesIterable diff_vertices_;
    storage::LabelId deleted_label_;
    uint64_t vertex_gid_floor_;
    storage::View view_;
  };

  UnionVerticesIterable Vertices(storage::View view);

  // Union of historical_'s incident edges (always View::OLD) with branch-native ones from the diff engine. Edge stubs
  // are skipped: the fork edge is emitted from historical_ and reads overlay the stub. Eager (degree-bounded).
  // Fork edges listed as deleted on `vertex_gid`'s stub, or whose other endpoint is deleted at `view`, are dropped.
  // A deleted vertex yields no edges and sets `*vertex_deleted`.
  std::vector<storage::EdgeAccessor> ResolveEdges(storage::Gid vertex_gid, storage::EdgeDirection direction,
                                                  storage::View view, bool *vertex_deleted = nullptr);

  // Forwarders to the core's change filters; MayHave* let readers skip the resolve when a kind is definitely unchanged.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept { core_->RecordVertexChange(gid, kind); }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { core_->RecordEdgeChange(endpoint_gid); }

  void RecordVertexDeleted(storage::Gid gid) noexcept { core_->RecordVertexDeleted(gid); }

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

  // Sets the `branched()` bit on a main@fork edge (its Edge* is main's live object); no-op without properties_on_edges.
  void MarkForkEdgeBranched(const storage::EdgeAccessor &fork_edge);

  std::shared_ptr<BranchCore> core_;
  std::string name_;
  // current_diff_txn_ is not owned: it points into CurrentDB::db_transactional_accessor_ for one transaction.
  storage::Storage::Accessor *current_diff_txn_{nullptr};
  std::unique_ptr<storage::Storage::Accessor> historical_;
};

}  // namespace memgraph::versioning
