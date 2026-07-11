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
#include <optional>
#include <string>

#include "storage/v2/commit_args.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::versioning {

// Graph Versioning v1, slice E-1 (lazy diff-context, VERTEX-ONLY): supersedes the earlier
// materialize-per-checkout `BranchEngine` (which physically copied the whole fork-state graph into
// a private engine at CHECKOUT time -- O(main's size), every time). `BranchContext` instead builds
// in O(1): an EMPTY private `storage::InMemoryStorage` ("the diff engine") plus a
// `HistoricalAccess(fork_ts)` reader ("the historical base", main time-traveled back to the fork
// point). Nothing is copied at checkout. A read for gid G resolves diff-engine-first, falling back
// to the historical base on a miss (`ResolveVertex`/`Vertices`); a write first copies-on-write the
// touched object into the diff engine (`CowVertex`), then mutates the copy. Once an object is
// COW'd, every subsequent read/write for that gid sees the diff engine's copy -- the historical
// base is never touched again for it.
//
// ONE SHARED NameIdMapper (main's own -- see BuildFromFork's own doc-comment): the query engine
// decodes a vertex's label/property ids through a SINGLE mapper per query (whichever accessor's
// GetNameIdMapper() it holds -- the diff engine's, for a branch). A historical (not-yet-COW'd)
// vertex still carries main's own ids; if diff_engine_ had its own, numerically-unrelated mapper
// (the ORIGINAL design here, and the predecessor BranchEngine's), those ids would be meaningless
// (or silently wrong) once decoded through it -- exactly the `MG_ASSERT(... "invalid ID" ...)`
// crash this design was changed to fix. Sharing means CowVertex copies ids DIRECTLY, no by-name
// translation needed (enum-TYPE ids are the one exception -- enum_store_ is NOT shared, see
// CowError).
//
// SCOPE OF THIS SLICE: vertices only (properties + labels + create). No edges/expansion (that is
// the next slice) -- COW copies props+labels only, not adjacency. No delete op yet, hence no
// tombstone concept in the read-side merge below (contrast BranchReconstruction's own
// UnionVerticesIterable, chunk 5b, which DOES need one).
//
// R35 (main is never written): `historical_` is a read-only, self-pinned accessor (its own
// HistoricalAccess doc-comment, inmemory/storage.hpp) held open for the whole checkout lifetime --
// that self-pin is what keeps the fork-state graph alive. Every mutation goes into `diff_engine_`
// instead, never into `main`.
//
// GID COLLISION HAZARD (found while implementing this slice, not in the original design sketch):
// `diff_engine_` is a brand-new `InMemoryStorage` with its own gid counter starting at 0
// (storage.hpp's `vertex_id_`). A branch-NATIVE `CREATE` (query::DbAccessor::InsertVertex, the
// auto-gid `CreateVertex()` path) run BEFORE any COW would therefore be assigned gid 0, 1, 2, ...
// -- which almost certainly collide with `historical_`'s own gids (main's fork-state graph also
// starts numbering from 0). `ResolveVertex`/`Vertices` cannot tell "branch-native vertex that
// happens to reuse gid G" apart from "COW'd copy of historical gid G" -- they'd silently conflate
// two logically distinct vertices under the same key. `BuildFromFork` closes this by reserving a
// disjoint gid range for branch-native creates before returning -- see `kBranchNativeGidWatermark`
// below for the mechanism and why a one-line fix (a public gid-counter setter) does not exist.
class BranchContext {
 public:
  // BuildFromFork can fail if `main.HistoricalAccess(fork_ts)` itself fails (fork_ts is not, or no
  // longer, pinned) -- the same failure BranchReconstruction::Open/BranchEngine::BuildFromFork
  // (the predecessor this supersedes) already surfaced. Unlike the predecessor, there is no
  // kUnsupportedEnumProperty case here anymore: with lazy COW, we only ever touch objects the
  // branch actually reads/writes, so an Enum property on some UNTOUCHED historical vertex is no
  // longer even visible to this function -- that check moves to `CowVertex`, the first place an
  // Enum-bearing object is actually copied (see `CowError`).
  struct BuildError {
    std::string message;
  };

  // `commit_args` is consumed for the one, tiny, O(1) internal transaction BuildFromFork itself
  // performs (see kBranchNativeGidWatermark's doc-comment) -- mirrors the predecessor's own
  // `seed_commit_args` parameter shape (caller builds it via `storage::CommitArgs::make_main(...)`,
  // keeping this file independent of dbms/).
  static std::expected<std::unique_ptr<BranchContext>, BuildError> BuildFromFork(storage::InMemoryStorage &main,
                                                                                 uint64_t fork_ts,
                                                                                 storage::CommitArgs commit_args);

  BranchContext(const BranchContext &) = delete;
  BranchContext &operator=(const BranchContext &) = delete;
  BranchContext(BranchContext &&) = delete;
  BranchContext &operator=(BranchContext &&) = delete;

  // `diff_engine_` tears itself down normally; `historical_` releases its own HistoricalAccess
  // self-pin on destruction (see HistoricalAccess's own doc-comment, inmemory/storage.hpp).
  ~BranchContext() = default;

  storage::InMemoryStorage &diff_engine() { return *diff_engine_; }

  storage::Storage::Accessor &historical() { return *historical_base_; }

  // Graph Versioning v1 (lazy diff-context, slice E-1) SINGLE-POINTER COLLAPSE: the per-QUERY
  // write accessor on `diff_engine()` lives here, not duplicated onto every query::VertexAccessor/
  // DbAccessor as a second raw pointer -- that would blow mgp_vertex's 64-byte C-API size budget
  // (mg_procedure_impl.hpp's kMaxMgpVertexSize static_assert) and grow the hot accessor for every
  // caller, branch or not (R6). Safe because a checked-out branch is exclusive single-writer
  // (VersionStore::TryAcquireCheckout, version_store.hpp) -- at most one session, hence at most one
  // query, is ever running against a given BranchContext at a time, so ONE current-txn slot
  // suffices; there is no concurrent-query aliasing hazard to guard against here.
  //
  // Set by CurrentDB::SetupDatabaseTransaction right after it opens `db_transactional_accessor_` on
  // `diff_engine()` for a branch query, and cleared back to nullptr by CleanupDBTransaction BEFORE
  // that accessor is reset/destroyed -- see both call sites in interpreter.cpp. CowVertex/
  // ResolveVertex/Vertices below all read it instead of taking an accessor parameter; they
  // DMG_ASSERT it is non-null (a caller reaching them without SetupDatabaseTransaction having run
  // first is a bug in the calling layer, not a legitimately reachable state).
  void set_current_diff_txn(storage::Storage::Accessor *txn) { current_diff_txn_ = txn; }

  storage::Storage::Accessor *current_diff_txn() const { return current_diff_txn_; }

  // Copy-on-write failure: only ever an Enum property -- unlike labels/properties (which now share
  // ONE id space with main, see diff_engine_'s own doc-comment on why BuildFromFork shares main's
  // NameIdMapper), an Enum PropertyValue also embeds an enum-TYPE id, and that comes from
  // enum_store_, which is NOT shared between main and the diff engine. Copying it verbatim would
  // silently mislabel it (or worse, alias some unrelated enum type that happens to share that
  // numeric id in the diff engine), and dropping it would be silent data loss -- neither is
  // acceptable, see branch_engine.cpp's ContainsEnum. Also covers (defensively, should be
  // unreachable) the gid not being found in `historical_` either.
  struct CowError {
    std::string message;
  };

  // Copy-on-write: returns the diff engine's own VertexAccessor for `gid`, against
  // `current_diff_txn()` (see its own doc-comment for why this is a single slot, not a parameter).
  //   - If the diff engine already has `gid` (a prior COW, or a branch-native create), returns
  //     that -- idempotent.
  //   - Else, reads `historical_`'s copy of `gid` (props + labels) and `CreateVertexEx(gid)`s a
  //     same-gid copy into `current_diff_txn()`, copying each label/property id DIRECTLY -- no
  //     by-name translation needed, since diff_engine_ shares main's own NameIdMapper (one id
  //     space, see diff_engine_'s own doc-comment). Still rejects (CowError) an Enum property
  //     anywhere in the copied properties -- enum-TYPE ids are NOT shared, see CowError's own
  //     doc-comment.
  std::expected<storage::VertexAccessor, CowError> CowVertex(storage::Gid gid);

  // Diff-engine-first point lookup (against `current_diff_txn()`), falling back to `historical_`
  // on a miss. No tombstone case this slice (no delete op exists yet).
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // Gid-ordered streaming UNION of `historical_` (every fork-state vertex, resolved -- so a COW'd
  // gid yields the diff engine's copy, per the tie-break below) with
  // `current_diff_txn()->Vertices(view)` (branch-native creates, i.e. gids historical_ doesn't
  // have). Mirrors
  // BranchReconstruction::UnionVerticesIterable's merge algorithm (chunk 5b) -- both sides are
  // gid-ordered (`storage::Vertex::operator<` compares gid; `AllVerticesIterable`'s own doc-comment
  // leans on the same property) -- so this is a single O(H + D) forward pass (H = historical vertex
  // count, D = diff-engine-resident count), never a per-object lookup of one side into the other.
  // Unlike BranchReconstruction (a cursor-agnostic primitive predating real query-execution
  // integration), this yields the live `storage::VertexAccessor` directly -- no eager value-copy --
  // since it plugs straight into `query::VerticesIterable` (see query/db_accessor.hpp).
  class UnionVerticesIterable {
   public:
    class Iterator {
     public:
      using difference_type = std::ptrdiff_t;
      using value_type = storage::VertexAccessor;

      // Default-constructed = the "end" iterator (`done_` defaults to true). A real begin()-side
      // iterator is only ever built by UnionVerticesIterable::begin(), below.
      Iterator() = default;
      Iterator(storage::VerticesIterable::Iterator hist_it, storage::VerticesIterable::Iterator hist_end,
               storage::VerticesIterable::Iterator diff_it, storage::VerticesIterable::Iterator diff_end);

      storage::VertexAccessor operator*() const { return *current_; }

      Iterator &operator++();

      // Only ever used to test "reached end" in a range-for (`it != end()`) -- comparing two
      // mid-iteration positions against each other is not a supported use, mirroring
      // BranchReconstruction::UnionVerticesIterable's own EndSentinel-based contract (this uses a
      // real same-typed end() instead, so the "3 fixed alternatives in a std::variant" shape
      // query::VerticesIterable::Iterator needs actually works -- see that file's own comment).
      bool operator==(const Iterator &other) const { return done_ == other.done_; }

     private:
      void SeekNext();

      std::optional<storage::VerticesIterable::Iterator> hist_it_;
      std::optional<storage::VerticesIterable::Iterator> hist_end_;
      std::optional<storage::VerticesIterable::Iterator> diff_it_;
      std::optional<storage::VerticesIterable::Iterator> diff_end_;
      std::optional<storage::VertexAccessor> current_;
      bool done_{true};
    };

    Iterator begin();

    Iterator end() { return Iterator{}; }

   private:
    friend class BranchContext;

    UnionVerticesIterable(storage::VerticesIterable hist_vertices, storage::VerticesIterable diff_vertices);

    storage::VerticesIterable hist_vertices_;
    storage::VerticesIterable diff_vertices_;
  };

  UnionVerticesIterable Vertices(storage::View view);

 private:
  BranchContext(std::unique_ptr<storage::InMemoryStorage> diff_engine,
                std::unique_ptr<storage::Storage::Accessor> historical_base)
      : diff_engine_(std::move(diff_engine)), historical_base_(std::move(historical_base)) {}

  std::unique_ptr<storage::InMemoryStorage> diff_engine_;
  std::unique_ptr<storage::Storage::Accessor> historical_base_;
  // Not owned -- see set_current_diff_txn()/current_diff_txn()'s own doc-comment above. Points
  // into CurrentDB::db_transactional_accessor_ for the duration of one query; nullptr otherwise.
  storage::Storage::Accessor *current_diff_txn_{nullptr};
};

}  // namespace memgraph::versioning
