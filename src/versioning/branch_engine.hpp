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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
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
#include "utils/db_aware_allocator.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/edge_stub_index.hpp"
#include "versioning/stub_index.hpp"

namespace memgraph::versioning {

// Shared per-branch state: one per live branch, shared by every session that has it checked out. Session-mutated
// state lives in the diff engine (MVCC) or behind an atomic / mutex here.
//
// The diff engine's NameIdMapper is a ReservedGuardMapper over main's: fork objects read from main@fork_ts carry
// main's ids, so every real name has the same id on both sides, and the stub markers' reserved ids (stub_state.hpp)
// translate to names for the engine's own code. Text a user typed must resolve on main's mapper (NameToLabel and
// friends below), never on the diff engine's, or a name that spells a marker would read as one. Enum-type ids differ
// (the enum store is mirrored, not shared).
//
// A branch's changes to fork vertices and fork edges (labels, properties, deletes) are written as stock MVCC deltas
// on stubs: empty Vertex / Edge objects owned by the StubIndex / EdgeStubIndex, not entries of the diff engine's
// vertices_ / edges_.
//
// Branch-native creates take gids from main's counters, so gids are unique across main and every branch. `Build`
// records main's counters as floors: a gid below the vertex (edge) floor is a fork CANDIDATE (the object may have been
// created by main after the fork, so whether it exists at fork_ts is for main@fork_ts to say), a gid at or above it is
// branch-native or main's later, and is only ever looked up in the diff engine.
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

  // The only way to resolve text a user typed on a branch: main's mapper, not the diff engine's guard.
  storage::LabelId NameToLabel(std::string_view name) { return main_->NameToLabel(name); }

  storage::PropertyId NameToProperty(std::string_view name) { return main_->NameToProperty(name); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) { return main_->NameToEdgeType(name); }

  // Main's vertex/edge counters when the floors were read (Build, or the recorded ones): gids below are fork
  // candidates, gids at or above are branch-native (or main's, allocated later).
  uint64_t vertex_gid_floor() const { return vertex_gid_floor_; }

  uint64_t edge_gid_floor() const { return edge_gid_floor_; }

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

// One session's view of a branch: a shared BranchCore plus per-transaction state. A branch reads as the fork state
// (main at fork_ts, through the historical accessor, always View::OLD) with the branch's own changes laid over it:
//  - a fork object the branch changed has a stub in the diff engine (stub_state.hpp), found by gid and merged over the
//    fork object (stub_merge.hpp);
//  - a branch-native object is a plain diff-engine object.
// The read primitives below are the only place that resolves this. They take the accessor a query holds (a fork object
// read from historical_, or a native one) and answer for the branch; no stub accessor is ever handed out as the object.
//
// The per-transaction state is opened by OpenHistorical + SetCurrentDiffTxn and released by EndTransaction. One
// session, one transaction at a time; concurrent sessions each hold their own BranchContext over the same core.
//
// The historical accessor holds main's READ storage guard while a branch transaction is open.
class BranchContext {
 public:
  using BuildError = BranchCore::BuildError;

  // Builds a private, unregistered core and wraps it (unit tests; production attaches to the registry's core).
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

  // Opens a historical accessor on main at fork_ts (holding main's READ guard). Fails, leaving nothing open, if
  // fork_ts is no longer pinned. Call BEFORE opening the diff-engine accessor: lock order is main READ -> diff engine.
  std::expected<void, BuildError> OpenHistorical();

  // Completes the transaction OpenHistorical started: `diff_txn` is the session's diff-engine accessor, not owned.
  void SetCurrentDiffTxn(storage::Storage::Accessor *diff_txn);

  // OpenHistorical + SetCurrentDiffTxn, for callers that already hold the diff-engine accessor (tests).
  std::expected<void, BuildError> BeginTransaction(storage::Storage::Accessor *diff_txn) {
    return OpenHistorical().transform([&] { SetCurrentDiffTxn(diff_txn); });
  }

  // Drops the transaction and releases main's READ guard. Idempotent. The caller ends the diff-engine accessor itself.
  void EndTransaction() noexcept { txn_.reset(); }

  // True between SetCurrentDiffTxn and EndTransaction.
  bool in_transaction() const { return txn_ && txn_->diff != nullptr; }

  storage::Storage::Accessor *current_diff_txn() const { return txn_ ? txn_->diff : nullptr; }

  // Valid between OpenHistorical and EndTransaction.
  storage::Storage::Accessor &historical() {
    MG_ASSERT(txn_, "BranchContext: no branch transaction is open.");
    return *txn_->historical;
  }

  // ---- Gids

  // True when `gid` is below the gid floor: the object may be a fork object, but may equally be main's, created after
  // the fork and before the floor was read, which main@fork_ts then does not have. False means branch-native (or
  // main's from after the floor), which is only ever looked up in the diff engine.
  bool IsForkCandidateVertex(storage::Gid gid) const { return gid.AsUint() < core_->vertex_gid_floor(); }

  bool IsForkCandidateEdge(storage::Gid gid) const { return gid.AsUint() < core_->edge_gid_floor(); }

  // ---- Reads of a vertex or edge accessor, as the branch sees it at `view`

  // `vertex` is a fork vertex (an accessor of main at fork_ts) or a branch-native one; a fork vertex the branch
  // deleted reports DELETED_OBJECT. A fork vertex the branch never wrote (branched() clear) is read as is.
  storage::Result<storage::VertexKey> Labels(const storage::VertexAccessor &vertex, storage::View view);
  storage::Result<bool> HasLabel(const storage::VertexAccessor &vertex, storage::LabelId label, storage::View view);
  storage::Result<bool> HasAnyLabel(const storage::VertexAccessor &vertex, storage::View view);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> Properties(
      const storage::VertexAccessor &vertex, storage::View view);
  storage::Result<storage::PropertyValue> GetProperty(const storage::VertexAccessor &vertex, storage::PropertyId key,
                                                      storage::View view);
  storage::Result<uint64_t> GetPropertySize(const storage::VertexAccessor &vertex, storage::PropertyId key,
                                            storage::View view);

  // The same for an edge: a fork edge hidden on the branch (deleted, or an endpoint deleted) reports DELETED_OBJECT.
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> Properties(const storage::EdgeAccessor &edge,
                                                                                    storage::View view);
  storage::Result<storage::PropertyValue> GetProperty(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                                      storage::View view);
  storage::Result<uint64_t> GetPropertySize(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                            storage::View view);

  // ---- Deletion

  // True when fork vertex `gid` is deleted on the branch, following the diff engine's MVCC at `view` (own writes,
  // snapshots, rollback). False for native gids (a native delete is an ordinary diff-engine delete).
  bool IsVertexDeleted(storage::Gid gid, storage::View view);

  // The same for a fork vertex accessor, skipping the stub probe unless the branch may have deleted something.
  bool IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view);

  // True when the fork edge is hidden on the branch: deleted there, or either endpoint deleted. False for a
  // branch-native edge.
  bool IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view);

  // ---- Writes of a vertex accessor
  //
  // `vertex` is a fork vertex (an accessor of main at fork_ts) or a branch-native one. A native vertex is the diff
  // engine's own object and takes the stock mutator. A fork vertex is written as direct single-key stock mutators on
  // its stub, created on first touch, so a write is one or two stock calls and never decodes the stub. Whole-set
  // operations (Init/Update/ClearProperties) decompose into per-key writes: the stock whole-set mutators would wipe the
  // stub's removal markers. Each primitive first refuses a vertex deleted on the branch (DELETED_OBJECT), then sets
  // main's `branched()` hint bit, records the change filters, and only then writes the stub. Like the stock mutators,
  // a write that changes nothing still conflicts with a concurrent writer of the same vertex.
  //
  // Multi-key writes are not atomic: once the first key is written, a failure poisons the diff transaction (it must
  // be aborted, as after a serialization error). A statement failure must therefore abort the transaction.
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

  // Marks fork vertex `gid` deleted on the branch: false if it already was, NONEXISTENT_OBJECT if the fork state has no
  // such vertex. Edges are NOT touched: incident fork edges are hidden by the marker, native edges are the caller's to
  // delete. SERIALIZATION_ERROR (and a poisoned transaction) if a branch-created edge is attached to the vertex by a
  // transaction this one cannot write over.
  storage::Result<bool> DeleteForkVertex(storage::Gid gid);

  // A refusal to write that the user is told about.
  struct BranchWriteError {
    std::string message;
  };

  // The diff engine's object for `gid`, for a writer that needs the object itself: a native vertex, or the stub of a
  // fork vertex (created on first touch, with main's `branched()` bit set) to attach a native edge to. Hand a stub to
  // no one but the branch writers. BranchWriteError if the vertex was deleted on the branch or is not there at all.
  std::expected<storage::VertexAccessor, BranchWriteError> EnsureDiffVertex(storage::Gid gid);

  // Creates a branch-native vertex in the diff engine at a gid taken from main's counter.
  storage::VertexAccessor CreateNativeVertex();

  // ---- Point lookups

  // The vertex `gid` as the branch holds it: a fork gid resolves to the fork accessor (main's Vertex, the historical
  // transaction) unless the branch deleted it, a native gid to the diff engine's vertex. Never a stub.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // The edge `edge_gid` out of `from_gid`: a fork gid resolves to the fork edge unless hidden on the branch, a native
  // gid to the diff engine's edge. Never an edge stub. An edge id says nothing of its endpoints and main keeps no
  // fork-time index by edge id, so the FROM vertex is required; finding an edge without it would scan all of main.
  std::optional<storage::EdgeAccessor> ResolveEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // Fork-state vertex (historical_, View::OLD). Null for a native gid and for a candidate main does not have at
  // fork_ts.
  std::optional<storage::VertexAccessor> FindForkVertex(storage::Gid gid);

  // The fork edge `edge_gid` (historical_, View::OLD) out of fork vertex `from_gid`: found in the vertex's fork-state
  // out-adjacency, so O(its out-degree at the fork).
  std::optional<storage::EdgeAccessor> FindForkEdge(storage::Gid edge_gid, storage::Gid from_gid);

  // The diff engine's object for `gid`, for the writers: a fork gid resolves only through the stub index (a stub
  // accessor), a native gid only through the diff engine's skiplist. Hand a stub to no one but the branch writers.
  std::optional<storage::VertexAccessor> FindStub(storage::Gid gid, storage::View view);

  // True when the branch has a stub for fork vertex `gid`. Conservative as a "was written" gate: an empty stub left
  // by an aborted write counts.
  bool HasVertexStub(storage::Gid gid);

  // Diff-engine-ONLY point lookup by edge gid (no historical_ fallback): branch-native edges, which hang off
  // diff-engine vertices or the stubs of fork vertices. Null for a fork edge gid.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::View view);
  // Same, walking the known FROM vertex's adjacency instead of scanning every vertex.
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid, storage::View view);

  // ---- Scans and adjacency

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

  // The branch's edges of `vertex` (a fork vertex or a native one), unfiltered by type and endpoint: callers filter
  // after charging hops. DELETED_OBJECT when the vertex is deleted at `view`, native vertices too, including one the
  // transaction itself just deleted (which no gid lookup can see).
  storage::Result<std::vector<storage::EdgeAccessor>> OutEdges(const storage::VertexAccessor &vertex,
                                                               storage::View view);
  storage::Result<std::vector<storage::EdgeAccessor>> InEdges(const storage::VertexAccessor &vertex,
                                                              storage::View view);

  // Union of historical_'s incident edges (always View::OLD) with branch-native ones from the diff engine. Fork edges
  // deleted on the branch, or whose other endpoint is deleted at `view`, are dropped. Eager (degree-bounded). A fork
  // vertex the branch deleted yields no edges and sets `*vertex_deleted`. By gid alone, a native vertex deleted in this
  // very transaction is simply not found (the flag stays clear): use OutEdges/InEdges when an accessor is at hand.
  std::vector<storage::EdgeAccessor> ResolveEdges(storage::Gid vertex_gid, storage::EdgeDirection direction,
                                                  storage::View view, bool *vertex_deleted = nullptr);

  // Forwarders to the core's change filters; MayHave* let readers skip the resolve when a kind is definitely unchanged.
  bool MayHaveVertexDeleted(storage::Gid gid) const noexcept { return core_->MayHaveVertexDeleted(gid); }

  bool MayHaveAnyVertexDeleted() const noexcept { return core_->MayHaveAnyVertexDeleted(); }

  bool MayHaveForkEdgeDeleted(storage::Gid edge_gid) const noexcept { return core_->MayHaveForkEdgeDeleted(edge_gid); }

  bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return core_->MayHaveEdgeChange(gid); }

 private:
  // Everything that exists only while a branch transaction is open.
  struct Txn {
    std::unique_ptr<storage::Storage::Accessor> historical;
    storage::Storage::Accessor *diff{nullptr};  // Not owned; set by SetCurrentDiffTxn.
    // Last-page cache for StubIndex lookups. A stale cursor never yields a wrong pointer.
    StubIndex::Cursor stub_cursor;
  };

  // The open transaction; asserts SetCurrentDiffTxn has run.
  Txn &txn() {
    MG_ASSERT(in_transaction(), "BranchContext: no branch transaction is open.");
    return *txn_;
  }

  // The diff transaction is an InMemoryAccessor (the diff engine is an InMemoryStorage). The Ex writers are protected
  // there, so the write path casts to ReplicationAccessor itself.
  storage::InMemoryStorage::InMemoryAccessor &diff() {
    return static_cast<storage::InMemoryStorage::InMemoryAccessor &>(*txn().diff);
  }

  StubIndex &stubs() const { return core_->stub_index(); }

  EdgeStubIndex &edge_stubs() const { return core_->edge_stub_index(); }

  // True for the stub accessor of a fork object, which must never reach a caller as the object itself.
  bool IsStub(const storage::VertexAccessor &vertex) const {
    return vertex.storage_ == &diff_engine() && IsForkCandidateVertex(vertex.Gid());
  }

  bool IsStub(const storage::EdgeAccessor &edge) const {
    return edge.storage_ == &diff_engine() && IsForkCandidateEdge(edge.Gid());
  }

  // True for a branch-native vertex: an object of the diff engine's vertices_.
  bool IsNative(const storage::VertexAccessor &vertex) const { return vertex.storage_ == &diff_engine(); }

  // The accessor of main's fork vertex `fork` (or, when the stub index has none recorded, a lookup of `gid`).
  std::optional<storage::VertexAccessor> ForkAccessor(storage::Vertex *fork, storage::Gid gid);

  // `stub` as an accessor of the diff transaction. Not probed for visibility: a stub is never created or deleted by a
  // delta, and every caller reads through the accessor, which applies `view` itself.
  storage::VertexAccessor StubAccessor(storage::Vertex *stub);

  // Marks main's fork vertex (if not yet marked) and returns its stub as an accessor of the diff transaction, creating
  // the stub on first touch. The caller has checked the vertex is not deleted on the branch. May throw
  // OutOfMemoryException after the stub is published (a retry finds it).
  storage::VertexAccessor MaterializeStub(const storage::VertexAccessor &fork);

  // MaterializeStub for a writer: DELETED_OBJECT if the branch deleted the vertex.
  storage::Result<storage::VertexAccessor> StubForWrite(const storage::VertexAccessor &fork);

  // For a no-op write: the stock mutators check for a conflict before they find nothing to do, and so does this.
  // False (poisoning the transaction) on a conflict.
  bool TouchStub(const storage::VertexAccessor &stub);

  // After a delete marker was written on `stub`: false (poisoning the transaction) if it carries a delta from a
  // branch-created edge that this transaction cannot write over, i.e. one from a transaction in flight or committed
  // after this one started. The delete marker and an edge attach are ordered by the stub's MVCC; this covers a
  // non-sequential edge delta buried under a committed older head.
  bool CanDeleteAnchor(const storage::VertexAccessor &stub);

  // One fork vertex property written through its stub, in the per-key order the reads rest on (stub_merge.hpp): a
  // removal marker goes on before the value is dropped and off after the value is set. Returns the old merged value.
  storage::Result<storage::PropertyValue> WriteStubProperty(const storage::VertexAccessor &fork,
                                                            storage::VertexAccessor &stub, storage::PropertyId key,
                                                            const storage::PropertyValue &value);

  // Records the filters a write of fork vertex `gid`'s `key` feeds (per-key gate, touched set), before the stub is
  // written. The vertex-wide property filter is recorded once per primitive.
  void RecordPropertyWrite(storage::Gid gid, storage::PropertyId key);

  // AddLabel (`add`) / RemoveLabel.
  storage::Result<bool> ToggleLabel(storage::VertexAccessor &vertex, storage::LabelId label, bool add);

  // Fork vertex `gid`'s stub accessor, or null if the branch has none.
  std::optional<storage::VertexAccessor> ReadStub(storage::Gid gid);

  // Whether the branch deleted the vertex `stub` belongs to, as seen at `view`.
  bool StubDeleted(storage::Vertex *stub, storage::View view);

  // The stub `vertex`'s read must consult: null for a native vertex, one the branch never wrote, or when `may_differ`
  // (the filter of what is being read) and the delete filter both say the stub cannot matter.
  template <typename MayDiffer>
  std::optional<storage::VertexAccessor> StubToRead(const storage::VertexAccessor &vertex, MayDiffer &&may_differ);

  // The fork edge's stub as an accessor of the diff transaction with the fork endpoints (an edge stub is never
  // attached to an adjacency list), or null. Not probed for visibility, as ReadStub.
  std::optional<storage::EdgeAccessor> FindEdgeStub(const storage::EdgeAccessor &fork_edge);

  bool IsForkEdgeDeleted(const storage::EdgeAccessor &fork_edge, storage::View view);

  // IsForkEdgeHidden, for a caller that knows more: `edge_may_be_deleted` false when it proved no deleted fork edge
  // touches this one, `live_endpoint` an endpoint it already knows is not deleted.
  bool IsForkEdgeHiddenWith(const storage::EdgeAccessor &fork_edge, storage::View view, bool edge_may_be_deleted,
                            std::optional<storage::Gid> live_endpoint);

  // What a branch read of a fork edge resolves to.
  struct EdgeRead {
    enum class Kind : uint8_t { kDirect, kHidden, kStub };
    Kind kind{Kind::kDirect};
    std::optional<storage::EdgeAccessor> stub;
  };

  // `field` narrows the property gate to one property.
  EdgeRead ResolveEdgeRead(const storage::EdgeAccessor &edge, std::optional<storage::PropertyId> field,
                           storage::View view);

  std::optional<storage::EdgeAccessor> FindEdgeInAnchors(storage::Gid edge_gid, storage::View view);

  storage::Result<std::vector<storage::EdgeAccessor>> Edges(const storage::VertexAccessor &vertex,
                                                            storage::EdgeDirection direction, storage::View view);

  std::shared_ptr<BranchCore> core_;
  std::optional<Txn> txn_;
};

}  // namespace memgraph::versioning
