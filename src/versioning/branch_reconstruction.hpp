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
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "utils/skip_list.hpp"
#include "versioning/branch_log.hpp"
#include "versioning/branch_overlay.hpp"

namespace memgraph::versioning {

// Graph Versioning CHUNK 5b (read side): the branch RECONSTRUCTION read primitive (R20).
//
// A BranchReconstruction owns exactly the two ingredients chunk 5a/4 already built:
//   - a HistoricalAccess(fork_ts) accessor (chunk 4) -- main time-traveled back to the branch's
//     fork point, read-only, self-pinned for its own lifetime (R16/HIGH-1);
//   - a BranchOverlay (chunk 5a) materialized from the branch's own change-log against that same
//     fork-state base.
// It exposes the UNION read chunk 7's cursors will eventually consume: for every gid, the
// reconciled view is the overlay's version if the branch touched it (created OR modified OR
// deleted-as-tombstone), else main's fork-state version.
//
// STREAMING MERGE, NOT A LOOKUP TABLE: `UnionVertices()` walks two ALREADY gid-ordered sequences
// in lockstep --
//   - main-as-of-fork: `historical->Vertices(View::OLD)`, backed by `InMemoryStorage`'s
//     vertices_ SkipList, which is gid-ordered because `storage::Vertex::operator<` compares gid
//     (vertex.hpp) -- the exact same property `AllVerticesIterable`'s own doc-comment leans on
//     ("skip-list is gid-ordered -> no later entry can be in scope either");
//   - the overlay: `overlay.VerticesAccessor()`, backed by the SAME `utils::SkipListDb<Vertex>`
//     alias (branch_overlay.hpp), gid-ordered for the identical reason.
// Each step advances whichever side holds the smaller pending gid (or both, on a tie), so the
// whole scan is O(N + O) total comparisons/yields (N = fork-state vertex count, O = overlay-
// resident vertex count, itself bounded by the branch's own delta-scale change-log, D5) -- NEVER a
// per-object hash/tree lookup of one side into the other. A tombstoned-but-not-recreated gid is
// detected via a single `BranchOverlay::LookupVertex` status check on the main-only side (O(1)
// against the overlay's own tombstone set, not a re-scan) and silently skipped.
//
// Reconciliation, by case:
//   - gid only in main                -> kTombstoned: skip. Else: yield main-as-of-fork (kMain).
//   - gid in both (tie)                -> overlay wins outright (it IS the branch's view of that
//                                          gid); main's fork-state copy is superseded wholesale,
//                                          never field-merged.
//   - gid only in overlay              -> branch-created: yield the overlay version (kOverlay).
//     (An overlay-resident vertex is, by chunk 5a's own construction, never simultaneously
//     tombstoned -- Materialize's delete path removes the gid from `vertices_` at the same time it
//     records the tombstone, and a later recreate does the reverse -- so no extra check is needed
//     here; see branch_overlay.cpp's WalVertexCreate/WalVertexDelete handlers.)
//
// R35 (never write main): every read below goes through `historical_` (a read-only accessor) and
// `overlay_` (a private, per-query container) -- nothing here ever touches main's live vertices_/
// edges_ SkipLists for anything but reading.
//
// Lightweight yield shape (NOT a VertexAccessor): `ReconstructedVertex` carries gid + labels +
// properties + which side produced it. A real VertexAccessor is a live cursor over ONE storage
// engine's MVCC-visible Vertex*, with `view` semantics, index bookkeeping hooks, etc. -- there is
// no such single engine here (a reconciled row may come from either side), and building that
// unified cursor abstraction is exactly the ScanAll/expand integration chunk 7 owns. Eagerly
// copying labels/properties at yield time costs nothing beyond what visiting the winning side's
// object already required (no additional lookup into a third structure), so this stays within the
// same O(N+O) budget as the merge itself.
//
// SCOPE / DEFERRED TO CHUNK 7 (flagged explicitly, not silently skipped):
//   - `UnionEdges()` (a streaming edge-scan analogous to `UnionVertices()`) is NOT implemented
//     here. Unlike vertices, `storage::Storage::Accessor` exposes no bare "all edges, gid-ordered"
//     scan primitive at all (storage.hpp's `Edges(...)` overloads are all edge-type/property-index
//     scans) -- main itself has no such global edge stream to merge against. A real edge union
//     needs to be built either per-edge-type (cross-referencing each `Edges(edge_type, view)` call
//     against the overlay) or by walking the union'd vertex set's adjacency during expansion --
//     both are expansion/ScanAll-shaped problems that belong with the rest of chunk 7's cursor
//     integration, not a bare, cursor-agnostic reconstruction primitive. What IS provided here is
//     `FindEdge(gid)`, a point lookup (overlay-first, main-as-of-fork fallback) symmetric to
//     `FindVertex` -- cheap, directly testable, and independent of the deferred scan.
//   - The write-CAPTURE seam (`CaptureBranchCommit`, below) is exercised here only against a
//     synthetically-built `storage::Transaction` (see
//     tests/unit/versioning_branch_reconstruction.cpp) -- wiring it into a REAL branch write/commit
//     path requires the branch write transaction plumbing itself, which does not exist before
//     chunk 7 (interpreter dispatch). The owner-resolution walk it performs is not test-only,
//     though: it is the same delta-owner-resolution main's own commit-time WAL append performs
//     (inmemory/storage.cpp's `append_deltas`), so this is the real capture logic, just not yet
//     invoked from a real commit.
class BranchReconstruction {
 public:
  enum class Source : uint8_t { kMain, kOverlay };

  // Lightweight, chunk-7-facing reconciled vertex -- see the class comment for why this is not a
  // VertexAccessor.
  struct ReconstructedVertex {
    storage::Gid gid;
    std::vector<storage::LabelId> labels;
    std::map<storage::PropertyId, storage::PropertyValue> properties;
    Source source;
  };

  // Symmetric shape for `FindEdge` (point lookup only -- see the class comment's deferred-scope
  // note on why the streaming edge UNION itself is chunk 7's job).
  struct ReconstructedEdge {
    storage::Gid gid;
    storage::EdgeTypeId edge_type;
    storage::Gid from_vertex;
    storage::Gid to_vertex;
    std::map<storage::PropertyId, storage::PropertyValue> properties;
    Source source;
  };

  // Opens `main.HistoricalAccess(fork_ts)` and, on success, materializes a fresh `BranchOverlay`
  // from `changelog` against it (chunk 5a's `Materialize`, unchanged). Propagates
  // `HistoricalAccessError` unchanged on failure (e.g. `ForkTimestampNotPinned`).
  //
  // `name_id_mapper` mirrors `BranchOverlay`'s own ctor contract: must outlive the returned object
  // (in practice, the same mapper `main` itself uses).
  static std::expected<std::unique_ptr<BranchReconstruction>, storage::InMemoryStorage::HistoricalAccessError> Open(
      storage::InMemoryStorage &main, uint64_t fork_ts, const std::vector<storage::durability::WalDeltaData> &changelog,
      storage::NameIdMapper *name_id_mapper);

  BranchReconstruction(const BranchReconstruction &) = delete;
  BranchReconstruction &operator=(const BranchReconstruction &) = delete;
  BranchReconstruction(BranchReconstruction &&) = delete;
  BranchReconstruction &operator=(BranchReconstruction &&) = delete;

  // RAII: dropping `historical_` releases its own HIGH-1 self-pin (HistoricalAccess's finalize
  // path) and `overlay_`'s SkipLists are simply discarded -- neither has any other cleanup to do.
  ~BranchReconstruction() = default;

  // The gid-ordered streaming UNION of main-as-of-fork with the overlay -- see the class comment
  // for the full merge algorithm and its O(N+O) budget. A single-pass forward range: `begin()` may
  // only be called once per instance (mirrors `storage::VerticesIterable`'s own single-pass
  // contract, since it wraps one of those).
  class UnionVerticesIterable {
   public:
    // Sentinel end marker (C++20 heterogeneous begin/end) -- avoids needing a fully-comparable
    // "end" of the same type as the merge's internal cursor pair.
    struct EndSentinel {};

    class Iterator {
     public:
      using difference_type = std::ptrdiff_t;
      using value_type = ReconstructedVertex;

      Iterator(storage::VerticesIterable::Iterator main_it, storage::VerticesIterable::Iterator main_end,
               utils::SkipListDb<storage::Vertex>::Iterator overlay_it,
               utils::SkipListDb<storage::Vertex>::Iterator overlay_end, BranchOverlay *overlay);

      const ReconstructedVertex &operator*() const { return current_; }

      Iterator &operator++();

      bool operator==(EndSentinel /*unused*/) const { return done_; }

     private:
      // Advances the merge state machine to the next (not-skipped) reconciled row, or sets
      // `done_` once both sides are exhausted. Called once by the ctor (to prime the first row)
      // and once per `operator++`.
      void SeekNext();

      storage::VerticesIterable::Iterator main_it_;
      storage::VerticesIterable::Iterator main_end_;
      utils::SkipListDb<storage::Vertex>::Iterator overlay_it_;
      utils::SkipListDb<storage::Vertex>::Iterator overlay_end_;
      BranchOverlay *overlay_;
      ReconstructedVertex current_{};
      bool done_{false};
    };

    Iterator begin();

    EndSentinel end() const { return {}; }

   private:
    friend class BranchReconstruction;

    UnionVerticesIterable(storage::VerticesIterable main_vertices,
                          utils::SkipListDb<storage::Vertex>::Accessor overlay_accessor, BranchOverlay *overlay);

    storage::VerticesIterable main_vertices_;
    utils::SkipListDb<storage::Vertex>::Accessor overlay_accessor_;
    BranchOverlay *overlay_;
  };

  UnionVerticesIterable UnionVertices();

  // Overlay-first point lookup: kPresent -> overlay wins (branch's own copy); kTombstoned ->
  // branch deleted it, not found; kAbsent -> falls through to main-as-of-fork via the historical
  // accessor's own FindVertex.
  std::optional<ReconstructedVertex> FindVertex(storage::Gid gid);

  // Symmetric point lookup for edges. NOT a scan -- see the class comment's deferred-scope note
  // for why the streaming edge UNION (`UnionEdges`) itself is chunk 7's job.
  std::optional<ReconstructedEdge> FindEdge(storage::Gid gid);

 private:
  BranchReconstruction(std::unique_ptr<storage::Storage::Accessor> historical, storage::NameIdMapper *name_id_mapper);

  std::unique_ptr<storage::Storage::Accessor> historical_;
  BranchOverlay overlay_;
};

// CAPTURE hook (seam for chunk 7 -- see the class comment's deferred-scope note): walks every
// delta a (now fully-applied) branch transaction produced, resolves each one's true owner (vertex
// or edge) by walking `delta.prev` past intermediate DELTA links -- the identical owner-resolution
// main's own commit-time WAL append performs (inmemory/storage.cpp's `append_deltas`) -- and
// forwards the corresponding forward record into `branch_log` via `BranchLog::AppendDelta`, then
// closes the transaction with `AppendTransactionEnd(commit_timestamp)`.
//
// ADD_IN_EDGE/REMOVE_IN_EDGE deltas are the vertex-side mirror of an edge's ADD_OUT_EDGE/
// REMOVE_OUT_EDGE half and carry no independent WAL record of their own -- skipped, mirroring
// main's own append_deltas. For an EDGE-owned delta, only SET_PROPERTY is WAL-encoded (edge
// create/delete is captured entirely through the endpoint vertices' ADD_OUT_EDGE/RECREATE_OBJECT/
// DELETE_OBJECT deltas instead); the edge's (in_vertex_gid, edge_type_id) hint pair comes from
// `transaction.GetEdgeSetPropertyInfo(edge->gid)`, the same cache main's own commit path reads.
//
// NOTE: the storage parameter is deliberately NOT named `storage` -- the function body needs
// `storage::` (the namespace) qualified names (`storage::Delta`, `storage::PreviousPtr`), and a
// parameter named `storage` would shadow that namespace for the rest of the body.
storage::durability::WalTxnEndPos CaptureBranchCommit(BranchLog &branch_log, const storage::Transaction &transaction,
                                                      storage::Storage *target_storage, uint64_t commit_timestamp);

}  // namespace memgraph::versioning
