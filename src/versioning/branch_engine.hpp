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
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/branch_change_kind.hpp"

namespace memgraph::versioning {

// Visits the integers of a list-valued marker property (a list of ints; the property store may hold it as a
// generic list or a packed int list). Works for PropertyValue and ExternalPropertyValue alike.
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

// Visits the property ids encoded in a stub's `__mg_removed` value.
template <typename PV, typename F>
void ForEachRemovedPid(PV const &value, F &&visit) {
  ForEachListedInt(value, [&](int64_t pid) { visit(storage::PropertyId::FromInt(static_cast<int32_t>(pid))); });
}

// Shared, per-branch state: one per live branch, handed out as shared_ptr by VersionStore's core registry and held
// by every session that has the branch checked out. Everything a session mutates lives either inside the diff engine
// (versioned by its MVCC) or behind an atomic / mutex here; per-session and per-transaction state is BranchContext's.
//
// ONE SHARED NameIdMapper (main's own): historical (not-yet-stubbed) vertices carry main's label/property ids, so the
// diff engine must decode through the same mapper. Enum-TYPE ids are the one exception (enum_store_ is mirrored, not
// shared); stubs copy no fork values, so nothing carries a main enum-type id into the diff engine.
//
// Edge stubs mirror vertex stubs: a fork edge whose properties change gets an empty delta-less diff edge with the same
// gid between the endpoint stubs; the stub holds only changed properties plus the `__mg_removed` pid list (shared with
// vertices) for removed ones, and is never visible as such.
//
// GID COLLISION HAZARD: the diff engine's gid counters start at 0; `Build` reserves a disjoint range for branch-native
// creates (kBranchNativeGidWatermark, branch_engine.cpp).
//
// Deletes: a deleted fork object is recorded INSIDE the diff engine (MVCC-versioned like every other branch write): the
// vertex stub carries the label `__mg_deleted`, a deleted fork edge's gid sits in the list property
// `__mg_deleted_edges` on both endpoint stubs.
//
// Lifetime: must not outlive the Database's storage (the registry that owns it is destroyed before storage_).
class BranchCore {
 public:
  // Build fails if fork_ts is no longer pinned.
  struct BuildError {
    std::string message;
  };

  // Reserved-name markers recording REMOVALS on a sparse fork stub. Everything lives in the stub itself
  // (diff-engine MVCC versions it): label L removed => stub carries label `__mg_rm:L`; properties
  // removed => stub property `__mg_removed` holds a list of the removed property ids (ints).
  // Names with the `__mg_` prefix are reserved: query writes of them on a branch are rejected.
  struct LabelInfo {
    bool reserved{false};
    // Set iff the label is a removal marker: the label it removes.
    std::optional<storage::LabelId> removed_label;
  };

  // Builds the diff engine (empty, sharing main's NameIdMapper), reserves the branch-native gid range, and mirrors
  // main's index and enum definitions as of now. `commit_args` is consumed by the gid-reservation commit.
  static std::expected<std::shared_ptr<BranchCore>, BuildError> Build(storage::InMemoryStorage &main, uint64_t fork_ts,
                                                                      storage::CommitArgs commit_args);

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
  storage::PropertyId RemovedPropsKey();
  // Delete markers: label `__mg_deleted` on a deleted fork vertex's stub; list property `__mg_deleted_edges` (edge
  // gids) on both endpoint stubs of a deleted fork edge.
  storage::LabelId DeletedLabel();
  storage::PropertyId DeletedEdgesKey();

  // Branch-side per-kind change filters (branch_change_filter.hpp). Lock-free (atomic fetch_or / acquire load), so
  // concurrent sessions may record and query without further synchronisation.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept {
    change_filters_.RecordVertexChange(gid, kind);
  }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { change_filters_.RecordEdgeChange(endpoint_gid); }

  void RecordPropertyFieldChange(storage::Gid gid, storage::PropertyId pid) noexcept {
    change_filters_.RecordPropertyField(gid, pid);
  }

  bool MayHavePropertyFieldChange(storage::Gid gid, storage::PropertyId pid) const noexcept {
    return change_filters_.MayHavePropertyFieldChange(gid, pid);
  }

  bool MayHaveLabelChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveLabelChange(gid); }

  bool MayHavePropertyChange(storage::Gid gid) const noexcept { return change_filters_.MayHavePropertyChange(gid); }

  bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return change_filters_.MayHaveEdgeChange(gid); }

 private:
  BranchCore(storage::InMemoryStorage *main, uint64_t fork_ts, std::unique_ptr<storage::InMemoryStorage> diff_engine)
      : main_(main), fork_ts_(fork_ts), diff_engine_(std::move(diff_engine)) {}

  storage::InMemoryStorage *main_;  // Not owned; outlives the core (see class comment).
  uint64_t fork_ts_;
  std::unique_ptr<storage::InMemoryStorage> diff_engine_;

  // Caches over the shared NameIdMapper for the reserved-name markers above. Guarded because reads
  // classify labels lazily; entries are immutable once inserted.
  std::mutex marker_mutex_;
  std::unordered_map<storage::LabelId, LabelInfo> label_info_;
  std::unordered_map<storage::LabelId, storage::LabelId> removal_marker_;
  std::unordered_map<storage::PropertyId, bool> reserved_property_;
  std::optional<storage::PropertyId> removed_props_key_;
  std::optional<storage::LabelId> deleted_label_;
  std::optional<storage::PropertyId> deleted_edges_key_;

  BranchChangeFilters change_filters_;
};

// One session's view of a branch: a shared BranchCore plus the per-transaction state. Reads resolve diff-engine-first,
// falling back to the historical base (main time-traveled to fork_ts; `ResolveVertex`/`Vertices`). A write first
// materializes an empty stub of the touched object in the diff engine (`CowVertex`/`CowEdge`), then mutates it.
//
// The two per-transaction slots (`current_diff_txn_`, `historical_`) are opened by OpenHistorical + SetCurrentDiffTxn
// and released by EndTransaction, called by CurrentDB::SetupDatabaseTransaction / CleanupDBTransaction. They are NOT duplicated onto
// every query::VertexAccessor/DbAccessor (mgp_vertex size budget). A BranchContext belongs to exactly one session and
// is used by one transaction at a time; concurrent sessions each hold their own BranchContext over the same core.
//
// historical_ holds main's READ storage guard (and a self fork pin) only while a branch transaction is open.
class BranchContext {
 public:
  using BuildError = BranchCore::BuildError;
  using LabelInfo = BranchCore::LabelInfo;

  // Builds a private, unregistered core and wraps it (unit tests; production attaches to the registry's core).
  static std::expected<std::unique_ptr<BranchContext>, BuildError> BuildFromFork(storage::InMemoryStorage &main,
                                                                                 uint64_t fork_ts,
                                                                                 storage::CommitArgs commit_args);

  explicit BranchContext(std::shared_ptr<BranchCore> core) : core_(std::move(core)) {}

  BranchContext(const BranchContext &) = delete;
  BranchContext &operator=(const BranchContext &) = delete;
  BranchContext(BranchContext &&) = delete;
  BranchContext &operator=(BranchContext &&) = delete;

  // historical_ is declared after core_, so it is released first.
  ~BranchContext() = default;

  const std::shared_ptr<BranchCore> &core() const { return core_; }

  storage::InMemoryStorage &diff_engine() { return core_->diff_engine(); }

  // Valid between OpenHistorical and EndTransaction.
  storage::Storage::Accessor &historical() {
    MG_ASSERT(historical_ != nullptr, "BranchContext::historical: no branch transaction is open.");
    return *historical_;
  }

  // Opens a fresh historical accessor on main at fork_ts (holding main's READ guard). Fails if the fork timestamp is
  // no longer pinned (branch dropped); nothing is left open on failure. CurrentDB opens it BEFORE the diff-engine
  // accessor so the lock order is always main READ -> diff engine.
  std::expected<void, BuildError> OpenHistorical();

  // Publishes `diff_txn` (the session's diff-engine accessor) as the current transaction.
  void SetCurrentDiffTxn(storage::Storage::Accessor *diff_txn) { current_diff_txn_ = diff_txn; }

  // OpenHistorical + SetCurrentDiffTxn, for callers that already hold the diff-engine accessor (unit tests).
  std::expected<void, BuildError> BeginTransaction(storage::Storage::Accessor *diff_txn) {
    if (auto opened = OpenHistorical(); !opened) return opened;
    SetCurrentDiffTxn(diff_txn);
    return {};
  }

  // Clears both slots; releases the historical accessor (and with it main's READ guard). Idempotent.
  void EndTransaction() noexcept {
    current_diff_txn_ = nullptr;
    historical_.reset();
  }

  storage::Storage::Accessor *current_diff_txn() const { return current_diff_txn_; }

  // CowVertex/CowEdge fail when the object is already deleted on the branch or the diff engine rejects the write.
  // `storage_error` is set when the failure came from the diff engine itself (e.g. SERIALIZATION_ERROR because
  // another session's open transaction owns an endpoint stub), so the caller can raise main's own error for it.
  struct CowError {
    std::string message;
    std::optional<storage::Error> storage_error{};
  };

  // Copy-on-write: returns the diff engine's VertexAccessor for `gid` via current_diff_txn().
  //   - Idempotent: if the diff engine already has `gid` (prior COW or branch-native create),
  //     returns that copy immediately.
  //   - CowError if the vertex is deleted on the branch.
  //   - Else: materializes an EMPTY delta-less stub with the same gid in current_diff_txn();
  //     fork labels/properties stay in historical_ and are not copied.
  std::expected<storage::VertexAccessor, CowError> CowVertex(storage::Gid gid);

  // Reserved-name markers (see BranchCore); forwarded so query code keeps one entry point.
  LabelInfo ClassifyLabel(storage::LabelId label) { return core_->ClassifyLabel(label); }

  storage::LabelId RemovalMarker(storage::LabelId label) { return core_->RemovalMarker(label); }

  bool IsReservedProperty(storage::PropertyId property) { return core_->IsReservedProperty(property); }

  storage::PropertyId RemovedPropsKey() { return core_->RemovedPropsKey(); }

  storage::LabelId DeletedLabel() { return core_->DeletedLabel(); }

  storage::PropertyId DeletedEdgesKey() { return core_->DeletedEdgesKey(); }

  // Deleted-ness is read from the stub as visible to the current diff transaction at `view` (never from side
  // state), so it follows diff-engine MVCC: own writes, snapshots and rollback all behave like main's.
  bool IsStubDeleted(const storage::VertexAccessor &stub, storage::View view);
  // False for native gids (their deletes are ordinary diff-engine deletes) and for fork vertices without a stub.
  bool IsVertexDeleted(storage::Gid gid, storage::View view);
  // `fork_vertex` is a main@fork accessor; a clear branched() bit proves no stub exists, skipping the lookup.
  bool IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view);
  // True when the fork edge is deleted on the branch (gid listed on its FROM stub) or either fork endpoint is.
  // `fork_edge` may also be the edge's diff-engine copy.
  bool IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view);
  // Ascending gids of the fork edges listed as deleted on `stub`.
  std::vector<uint64_t> DeletedEdgeGids(const storage::VertexAccessor &stub, storage::View view);
  storage::Result<bool> StoreDeletedEdgeGids(storage::VertexAccessor &stub, const std::vector<uint64_t> &gids);

  // DELETE of a fork vertex: stub (materialized if needed) plus the `__mg_deleted` label. The stub is never
  // physically deleted. Returns the stub, or nullopt when already deleted. Edges are NOT touched: incident fork
  // edges are hidden by the endpoint marker, incident diff-engine edges are the caller's to delete.
  storage::Result<std::optional<storage::VertexAccessor>> DeleteForkVertex(storage::Gid gid);
  // DELETE of a fork edge: its gid goes on both endpoint stubs, and a diff-engine copy (an earlier property write
  // COW'd it) is deleted normally. Returns false when it was already deleted.
  storage::Result<bool> DeleteForkEdge(const storage::EdgeAccessor &fork_edge);

  bool IsForkGid(storage::Gid gid) const;
  // Fork-state vertex (historical_, View::OLD).
  std::optional<storage::VertexAccessor> FindForkVertex(storage::Gid gid);
  // Diff-engine vertex (stub or native) in the current diff txn; no historical_ fallback.
  std::optional<storage::VertexAccessor> FindStubOrDiffVertex(storage::Gid gid, storage::View view);

  // Copy-on-write for edges. Takes the fork edge's EdgeAccessor (or its stub) directly because historical_->FindEdge(gid)
  // is unreliable for edges (see ResolveEdge's doc-comment).
  //   - Idempotent: if edge_gid already has a diff-engine edge (prior stub or branch-native create), returns it.
  //   - CowError if the edge is deleted on the branch.
  //   - Else: COWs BOTH endpoints (CowVertex, idempotent) and materializes an EMPTY delta-less edge stub with the
  //     same gid linked between the endpoint stubs (InMemoryAccessor::MaterializeStubEdge). Fork properties stay in
  //     historical_ and are not copied; writes then land on the stub as ordinary diff-engine MVCC writes, and reads
  //     overlay the stub on the fork edge (query::EdgeAccessor). Because the stub is not transactional, a rolled-back
  //     write leaves it behind harmlessly and the next COW simply finds it. It adds no edge to the branch (adjacency
  //     reads emit the fork edge, see ResolveEdges), so it records no edge change.
  std::expected<storage::EdgeAccessor, CowError> CowEdge(const storage::EdgeAccessor &fork_edge);

  // The fork edge (historical_, View::OLD) behind a diff-engine edge stub, found through its endpoints' adjacency.
  std::optional<storage::EdgeAccessor> FindForkEdge(const storage::EdgeAccessor &stub);

  // True for a diff-engine edge that stands for a fork edge (a stub), as opposed to a branch-native edge.
  bool IsEdgeStub(const storage::EdgeAccessor &diff_edge) const;

  // True when `stub` carries its own value for `property` or records its removal, i.e. the fork edge's value of that
  // property is not the branch's. Used by index overlays to drop stale fork-side candidates.
  bool StubEdgeTouches(const storage::EdgeAccessor &stub, storage::PropertyId property, storage::View view);

  // Diff-engine-first point lookup, falling back to historical_ on a miss.
  // A fork vertex whose stub is deleted at `view` never resolves.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // Diff-engine-ONLY point lookup by edge gid (no historical_ fallback). Used for:
  //   (1) CowEdge's idempotency check — "already COW'd or branch-natively created?"
  //   (2) EdgeAccessor self-correcting property reads (GetProperty/Properties/GetPropertySize).
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::View view);
  // Same lookup when the FROM endpoint's gid is known: avoids the diff engine's all-vertex scan for the edge.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Gid-ordered streaming UNION of historical_ (every fork-state vertex, resolved: a COW'd gid
  // yields the diff engine's copy) with current_diff_txn()->Vertices(view) (branch-native creates).
  // Both sides are gid-ordered (storage::Vertex::operator<), so this is a single O(H + D) forward
  // pass — never a per-object lookup. Yields live storage::VertexAccessor directly; no eager copy.
  class UnionVerticesIterable {
   public:
    class Iterator {
     public:
      using difference_type = std::ptrdiff_t;
      using value_type = storage::VertexAccessor;

      // Default-constructed = end iterator (done_ == true).
      Iterator() = default;
      // `deleted_label`/`view`: a fork vertex whose diff-engine stub carries the delete marker at `view` is skipped.
      Iterator(storage::VerticesIterable::Iterator hist_it, storage::VerticesIterable::Iterator hist_end,
               storage::VerticesIterable::Iterator diff_it, storage::VerticesIterable::Iterator diff_end,
               storage::LabelId deleted_label, storage::View view);

      storage::VertexAccessor operator*() const { return *current_; }

      Iterator &operator++();

      // Only tests "reached end" (done_ flag); mid-iteration position comparison is not supported.
      bool operator==(const Iterator &other) const { return done_ == other.done_; }

     private:
      void SeekNext();

      std::optional<storage::VerticesIterable::Iterator> hist_it_;
      std::optional<storage::VerticesIterable::Iterator> hist_end_;
      std::optional<storage::VerticesIterable::Iterator> diff_it_;
      std::optional<storage::VerticesIterable::Iterator> diff_end_;
      std::optional<storage::VertexAccessor> current_;
      storage::LabelId deleted_label_{storage::kInvalidLabelId};
      storage::View view_{storage::View::NEW};
      bool done_{true};
    };

    Iterator begin();

    Iterator end() { return Iterator{}; }

   private:
    friend class BranchContext;

    UnionVerticesIterable(storage::VerticesIterable hist_vertices, storage::VerticesIterable diff_vertices,
                          storage::LabelId deleted_label, storage::View view);

    storage::VerticesIterable hist_vertices_;
    storage::VerticesIterable diff_vertices_;
    storage::LabelId deleted_label_;
    storage::View view_;
  };

  UnionVerticesIterable Vertices(storage::View view);

  // Per-vertex UNION of historical_'s fork-state incident edges with current_diff_txn()'s branch-native ones. An edge
  // stub in the diff adjacency is skipped: its fork edge is emitted from historical_ (once, in fork identity) and
  // reads overlay the stub. NOT a lazy streaming merge: vertex incident-edge sets are degree-bounded, so an eager
  // owned vector is simpler here.
  // historical_ is always read at View::OLD (frozen snapshot, no notion of caller-relative NEW).
  // Fork edges listed as deleted on `vertex_gid`'s stub, or whose other fork endpoint is deleted at `view`, are
  // dropped; diff-engine deletes are hidden by MVCC. A deleted vertex yields no edges and sets `*vertex_deleted`.
  std::vector<storage::EdgeAccessor> ResolveEdges(storage::Gid vertex_gid, storage::EdgeDirection direction,
                                                  storage::View view,
                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                  bool *vertex_deleted = nullptr);

  // Per-kind change filters live in the shared core (see branch_change_filter.hpp for the model).
  // Write side: RecordVertexChange fires from VertexAccessor::CowIfNeeded; RecordEdgeChange fires
  // from CowEdge and DbAccessor::InsertEdge. Read side: MayHave* let readers skip the resolve when a kind is
  // definitely unchanged.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept { core_->RecordVertexChange(gid, kind); }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { core_->RecordEdgeChange(endpoint_gid); }

  // Fine-grained per-property recording/query. Gates GetProperty/GetPropertySize fast paths;
  // Properties() (whole-map) uses the coarse MayHavePropertyChange instead.
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
  // Sets the monotonic `branched()` bit on MAIN's own Vertex for `gid`, resolved through the transaction's historical
  // accessor with the vertex's lock held. Best-effort hint, safe to call from concurrent sessions (the bit is written
  // under main's vertex lock, no diff-engine lock is held by any caller). Call site: CowVertex.
  void MarkMainObjectBranched(storage::Gid gid);

  // Sets the `branched()` bit on a main@fork edge reached directly (its Edge* is main's live object). Same
  // properties_on_edges gate as above; takes only the edge's own lock.
  void MarkForkEdgeBranched(const storage::EdgeAccessor &fork_edge);

  std::shared_ptr<BranchCore> core_;
  // Per-transaction slots; see the class comment. current_diff_txn_ is not owned: it points into
  // CurrentDB::db_transactional_accessor_ for the duration of one transaction, nullptr otherwise.
  storage::Storage::Accessor *current_diff_txn_{nullptr};
  std::unique_ptr<storage::Storage::Accessor> historical_;
};

}  // namespace memgraph::versioning
