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
#include <optional>
#include <string>
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
#include "versioning/branch_change_filter.hpp"
#include "versioning/branch_change_kind.hpp"
#include "versioning/branch_log.hpp"
#include "versioning/branch_reconstruction.hpp"

namespace memgraph::versioning {

// `BranchContext` implements the lazy diff-engine checkout model. At CHECKOUT time it builds in
// O(1): an empty private `storage::InMemoryStorage` ("the diff engine") plus a
// `HistoricalAccess(fork_ts)` reader ("the historical base", main time-traveled to the fork point).
// Reads resolve diff-engine-first, falling back to historical_ on a miss (`ResolveVertex`/
// `Vertices`). A write first copies-on-write the touched object into the diff engine
// (`CowVertex`/`CowEdge`), then mutates the copy — after which every subsequent read for that gid
// sees the diff engine's copy exclusively.
//
// ONE SHARED NameIdMapper (main's own): the query engine decodes label/property ids through a
// SINGLE mapper per query. A historical (not-yet-COW'd) vertex still carries main's own ids; if
// diff_engine_ had its own unrelated mapper those ids would be meaningless once decoded through it.
// Sharing means CowVertex copies ids DIRECTLY — enum-TYPE ids are the one exception
// (enum_store_ is NOT shared, see CowError).
//
// GID COLLISION HAZARD: diff_engine_ starts its gid counter at 0. A branch-native CREATE before
// any COW would collide with historical_'s own gids. `BuildFromFork` reserves a disjoint gid range
// for branch-native creates — see `kBranchNativeGidWatermark` in branch_engine.cpp.
//
// historical_ is a read-only, self-pinned accessor (HistoricalAccess, inmemory/storage.hpp) held
// open for the whole checkout lifetime — that self-pin keeps the fork-state graph alive.
//
// Tombstones: a diff-engine miss cannot distinguish "explicitly deleted" from "never touched".
// `tombstoned_vertices_`/`tombstoned_edges_` close this: a recorded gid is hidden unconditionally
// from both sides. See `pending_tombstoned_vertices_` for the pending/committed split.
class BranchContext {
 public:
  // BuildFromFork fails if fork_ts is no longer pinned (timestamp was GC'd).
  // Enum properties on historical vertices are checked lazily in CowVertex, not here.
  struct BuildError {
    std::string message;
  };

  // `commit_args`: for the internal gid-watermark-reservation transaction (see
  //   kBranchNativeGidWatermark, branch_engine.cpp). Build via `storage::CommitArgs::make_main(...)`.
  // `branch_wal_root_directory`: stable per-branch WAL root (keyed by branch number, not name —
  //   names can be reused after drop+recreate). BuildFromFork mints a per-session-unique UUID
  //   subdirectory beneath it for collision safety.
  // `changelog`: already-captured WAL deltas from prior checkout sessions; empty for first checkout.
  // `replay_commit_args`: separate CommitArgs for the changelog replay (CommitArgs is move-only;
  //   watermark reservation and replay are two separate commits inside this one call).
  static std::expected<std::unique_ptr<BranchContext>, BuildError> BuildFromFork(
      storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs commit_args,
      storage::CommitArgs replay_commit_args, std::filesystem::path branch_wal_root_directory,
      const std::vector<storage::durability::WalDeltaData> &changelog);

  BranchContext(const BranchContext &) = delete;
  BranchContext &operator=(const BranchContext &) = delete;
  BranchContext(BranchContext &&) = delete;
  BranchContext &operator=(BranchContext &&) = delete;

  // diff_engine_ tears itself down normally; historical_ releases its HistoricalAccess self-pin.
  // `= default` is correct: each per-commit BranchLog is opened and finalized immediately after
  // capture — by the time this destructor runs, no open log remains.
  ~BranchContext() = default;

  storage::InMemoryStorage &diff_engine() { return *diff_engine_; }

  storage::Storage::Accessor &historical() { return *historical_base_; }

  // Single per-BranchContext slot for the query's write accessor on diff_engine(). Not duplicated
  // onto every VertexAccessor/DbAccessor (would exceed mg_procedure_impl.hpp's 80-byte
  // kMaxMgpVertexSize budget and widen the hot accessor for non-branch callers too). Safe because
  // a checked-out branch is exclusive single-writer (VersionStore::TryAcquireCheckout,
  // version_store.hpp) — at most one query runs against a given BranchContext at a time.
  // Set by CurrentDB::SetupDatabaseTransaction; cleared to nullptr by CleanupDBTransaction before
  // the accessor is destroyed. CowVertex/ResolveVertex/Vertices DMG_ASSERT it is non-null.
  void set_current_diff_txn(storage::Storage::Accessor *txn) { current_diff_txn_ = txn; }

  storage::Storage::Accessor *current_diff_txn() const { return current_diff_txn_; }

  // Result of CaptureCommitIfBranch, shared between Interpreter::Commit and PeriodicCommit.
  struct CommitCaptureOutcome {
    // false: nothing to capture (not this txn's diff engine, or a pure-read boundary).
    bool attempted = false;
    // true: capture would exceed max_changelog_length — caller MUST abort instead of committing.
    bool cap_exceeded = false;
  };

  // Branch-commit capture hook, shared between Interpreter::Commit and DbAccessor::PeriodicCommit
  // (periodic-commit boundaries previously flushed the diff-engine without writing the BranchLog).
  // MUST be called BEFORE the caller's own commit call (PrepareForCommitPhase/PeriodicCommit) on
  // `txn` — those fast-discard the transaction's deltas as part of committing.
  // Gated on current_diff_txn_: `USING VERSION 'main'` queries have a non-null BranchContext but
  // current_diff_txn_ is null — those must not trigger capture.
  CommitCaptureOutcome CaptureCommitIfBranch(const storage::Transaction &txn, uint64_t max_changelog_length) {
    if (current_diff_txn_ == nullptr || txn.deltas.empty()) {
      return {.attempted = false, .cap_exceeded = false};
    }

    const auto ts = NextBranchCommitTs();
    auto commit_log = CreateCommitLog(ts);
    uint64_t records = 0;
    CaptureBranchCommit(*commit_log, txn, &diff_engine(), ts, &records);

    if (ChangelogLength() + records > max_changelog_length) {
      // The BranchLog has already received deltas (count_ > 0), so WalFile::~WalFile() will NOT
      // unlink it on destruction — Abandon() explicitly deletes the file before the unique_ptr drops.
      commit_log->Abandon();
      return {.attempted = true, .cap_exceeded = true};
    }

    commit_log->Finalize();
    AddCapturedRecords(records);

    // PROMOTE pending tombstones on successful capture — strictly after the durable BranchLog write
    // and the cap-exceeded check (a cap-exceeded return skips this, leaving pending sets intact for
    // the caller's abort path). merge()+clear() moves each gid exactly once in O(pending) and
    // leaves the pending set unconditionally empty regardless of overlap.
    tombstoned_vertices_.merge(pending_tombstoned_vertices_);
    pending_tombstoned_vertices_.clear();
    tombstoned_edges_.merge(pending_tombstoned_edges_);
    pending_tombstoned_edges_.clear();

    memgraph::metrics::Metrics().global.versioning_branch_commits_captured->Increment();

    return {.attempted = true, .cap_exceeded = false};
  }

  // CowVertex/CowEdge fail only when the object contains an Enum property. An Enum PropertyValue
  // embeds an enum-TYPE id from enum_store_, which is NOT shared between main and diff_engine_ —
  // copying verbatim would silently mislabel it. Also covers gid not found in historical_.
  struct CowError {
    std::string message;
  };

  // Copy-on-write: returns the diff engine's VertexAccessor for `gid` via current_diff_txn().
  //   - Idempotent: if the diff engine already has `gid` (prior COW or branch-native create),
  //     returns that copy immediately.
  //   - Else: reads historical_'s copy of `gid` (props + labels), CreateVertexEx(gid)s a same-gid
  //     copy into current_diff_txn(), copying each label/property id DIRECTLY (shared NameIdMapper).
  //     Rejects (CowError) an Enum property — enum-TYPE ids are not shared (see CowError).
  std::expected<storage::VertexAccessor, CowError> CowVertex(storage::Gid gid);

  // Copy-on-write for edges. Takes the fork edge's EdgeAccessor directly (not just a gid) because
  // historical_->FindEdge(gid) is unreliable for edges (see ResolveEdge's doc-comment).
  //   - Idempotent: if edge_gid is already in the diff engine (prior COW or branch-native create),
  //     returns that copy — fork_edge fields unread except Gid().
  //   - Else: COWs BOTH endpoints (CowVertex, idempotent), recreates the edge in diff_engine_ at
  //     the SAME gid (so ResolveEdges' de-dupe still works), copies fork_edge's properties.
  //     Rejects (CowError) an Enum property.
  std::expected<storage::EdgeAccessor, CowError> CowEdge(const storage::EdgeAccessor &fork_edge);

  // Diff-engine-first point lookup, falling back to historical_ on a miss.
  // Checks tombstoned_vertices_ first — a tombstoned gid never resolves from either side.
  std::optional<storage::VertexAccessor> ResolveVertex(storage::Gid gid, storage::View view);

  // Diff-engine-ONLY point lookup by edge gid (no historical_ fallback). Used for:
  //   (1) CowEdge's idempotency check — "already COW'd or branch-natively created?"
  //   (2) EdgeAccessor self-correcting property reads (GetProperty/Properties/GetPropertySize).
  std::optional<storage::EdgeAccessor> FindDiffEdge(storage::Gid edge_gid, storage::View view);

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
      // `tombstoned_vertices`/`pending_tombstoned_vertices`: non-owning pointers into the owning
      // BranchContext's tombstone sets; SeekNext consults both to skip tombstoned gids.
      // Two separate pointers (not a back-pointer to BranchContext) because this nested Iterator
      // has no BranchContext reference — functionally equivalent to IsVertexTombstoned.
      Iterator(storage::VerticesIterable::Iterator hist_it, storage::VerticesIterable::Iterator hist_end,
               storage::VerticesIterable::Iterator diff_it, storage::VerticesIterable::Iterator diff_end,
               const std::unordered_set<storage::Gid> *tombstoned_vertices,
               const std::unordered_set<storage::Gid> *pending_tombstoned_vertices);

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
      const std::unordered_set<storage::Gid> *tombstoned_vertices_{nullptr};
      // Non-owning — pending-tombstone side; see ctor doc-comment.
      const std::unordered_set<storage::Gid> *pending_tombstoned_vertices_{nullptr};
      bool done_{true};
    };

    Iterator begin();

    Iterator end() { return Iterator{}; }

   private:
    friend class BranchContext;

    UnionVerticesIterable(storage::VerticesIterable hist_vertices, storage::VerticesIterable diff_vertices,
                          const std::unordered_set<storage::Gid> *tombstoned_vertices,
                          const std::unordered_set<storage::Gid> *pending_tombstoned_vertices);

    storage::VerticesIterable hist_vertices_;
    storage::VerticesIterable diff_vertices_;
    // Non-owning — points into BranchContext's tombstoned_vertices_; outlives this range.
    const std::unordered_set<storage::Gid> *tombstoned_vertices_;
    // Non-owning, symmetric — points into BranchContext's pending_tombstoned_vertices_.
    const std::unordered_set<storage::Gid> *pending_tombstoned_vertices_;
  };

  UnionVerticesIterable Vertices(storage::View view);

  // Per-vertex UNION of historical_'s fork-state incident edges with current_diff_txn()'s own,
  // de-duped by EdgeAccessor::Gid() with the diff engine's copy winning ties. NOT a lazy streaming
  // merge: vertex incident-edge sets are degree-bounded, so an eager owned vector is simpler here.
  // historical_ is always read at View::OLD (frozen snapshot, no notion of caller-relative NEW).
  // Tombstone-gated: edges in tombstoned_edges_ are excluded even if historical_ still has them.
  std::vector<storage::EdgeAccessor> ResolveEdges(storage::Gid vertex_gid, storage::EdgeDirection direction,
                                                  storage::View view,
                                                  const std::vector<storage::EdgeTypeId> &edge_types);

  // Records `gid` as deleted on the current, not-yet-committed transaction — into
  // pending_tombstoned_vertices_, NOT the permanent set. Called eagerly at Pull-time, before
  // commit/abort is known; promoted by CaptureCommitIfBranch on commit, discarded by
  // DiscardPendingTombstones on abort. Also marks the main object branched (idempotent: CowVertex
  // already does this, but MarkMainObjectBranched is harmless if called twice).
  void TombstoneVertex(storage::Gid gid) {
    pending_tombstoned_vertices_.insert(gid);
    MarkMainObjectBranched(gid);
  }

  // Symmetric for edges. Does NOT mark MAIN's Edge object: historical_->FindEdge is unreliable by
  // bare gid; CowEdge (which always runs first) handles that marking instead.
  // Inserts into pending_tombstoned_edges_; promoted/discarded like the vertex side.
  void TombstoneEdge(storage::Gid gid) { pending_tombstoned_edges_.insert(gid); }

  // Tombstone predicates consulting BOTH committed and pending sets — a gid this transaction just
  // deleted must stay hidden to itself (re-delete must no-op) even before the commit promotes it.
  // Single choke point all read/hide paths route through: CowVertex, CowEdge, ResolveVertex,
  // ResolveEdge, ResolveEdges, and UnionVerticesIterable::SeekNext all use these.
  bool IsVertexTombstoned(storage::Gid gid) const {
    return tombstoned_vertices_.contains(gid) || pending_tombstoned_vertices_.contains(gid);
  }

  bool IsEdgeTombstoned(storage::Gid gid) const {
    return tombstoned_edges_.contains(gid) || pending_tombstoned_edges_.contains(gid);
  }

  // Called on abort (CleanupDBTransaction, interpreter.cpp): discards pending tombstones so an
  // aborted DELETE does not permanently hide live objects. Safe to call unconditionally — no-op on
  // empty sets. Must be called AFTER CaptureCommitIfBranch on a committed transaction (pending
  // already promoted; clear is then a no-op).
  void DiscardPendingTombstones() noexcept {
    pending_tombstoned_vertices_.clear();
    pending_tombstoned_edges_.clear();
  }

  // Branch-side per-kind change filters (see branch_change_filter.hpp for the model).
  // Write side: RecordVertexChange fires from VertexAccessor::CowIfNeeded; RecordEdgeChange fires
  // from CowEdge and DbAccessor::InsertEdge; re-seeded from the changelog on re-checkout.
  // Read side: MayHave* let readers skip the resolve when a kind is definitely unchanged.
  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept {
    change_filters_.RecordVertexChange(gid, kind);
  }

  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { change_filters_.RecordEdgeChange(endpoint_gid); }

  // Fine-grained per-property recording/query. Gates GetProperty/GetPropertySize fast paths;
  // Properties() (whole-map) uses the coarse MayHavePropertyChange instead.
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
  // Takes BranchLog construction ingredients (not a pre-built log); CreateCommitLog builds a fresh
  // one per commit from them. `initial_changelog_length`: seeds changelog_length_ from prior
  // sessions so the retention cap is enforced against the branch's whole life.
  BranchContext(std::unique_ptr<storage::InMemoryStorage> diff_engine,
                std::unique_ptr<storage::Storage::Accessor> historical_base,
                std::unordered_set<storage::Gid> tombstoned_vertices, std::unordered_set<storage::Gid> tombstoned_edges,
                std::filesystem::path branch_log_session_directory, storage::SalientConfig::Items branch_log_items,
                storage::NameIdMapper *branch_log_mapper, uint64_t initial_changelog_length)
      : diff_engine_(std::move(diff_engine)),
        historical_base_(std::move(historical_base)),
        tombstoned_vertices_(std::move(tombstoned_vertices)),
        tombstoned_edges_(std::move(tombstoned_edges)),
        branch_log_session_directory_(std::move(branch_log_session_directory)),
        branch_log_items_(branch_log_items),
        branch_log_mapper_(branch_log_mapper),
        changelog_length_(initial_changelog_length) {}

  // Sets the monotonic `branched()` bit on MAIN's own Vertex for `gid`, resolved through
  // historical_->FindVertex(gid, View::OLD) with the vertex's lock held. Best-effort hint — not
  // correctness-load-bearing (nothing reads the bit in the read path yet). In-memory only;
  // re-seeded from changelog on re-checkout. Call sites: CowVertex, TombstoneVertex, BuildFromFork
  // replay loop.
  void MarkMainObjectBranched(storage::Gid gid);

  // Edge-side analogue of MarkMainObjectBranched. Resolves via from_vertex_gid's OutEdges(View::OLD)
  // walk (not historical_->FindEdge — unreliable by bare gid; see ResolveEdge's doc-comment).
  // Gated on properties_on_edges: light/reference edges have no Edge object to mark. Best-effort.
  // Call site: BuildFromFork replay loop only.
  void MarkMainEdgeBranched(storage::Gid from_vertex_gid, storage::Gid edge_gid);

  // Monotonic per-session counter for branch-commit WAL timestamps. The branch's BranchLog is a
  // private WAL stream whose timestamps only need to be strictly increasing (never compared against
  // main's clock). Also threaded into CreateCommitLog as the commit's sequence number for ordering.
  // Not thread-safe: single-writer exclusivity guarantees one commit at a time.
  uint64_t NextBranchCommitTs() { return ++branch_commit_ts_; }

  // Cumulative WAL delta records captured across this branch's whole life (prior sessions +
  // current). Bounds `FLAGS_versioning_max_changelog_length` — an unbounded change-log means an
  // unbounded GC-pin retention window.
  uint64_t ChangelogLength() const { return changelog_length_; }

  // Called after a commit's records are successfully appended; advances the running total.
  void AddCapturedRecords(uint64_t n) { changelog_length_ += n; }

  // Creates a fresh BranchLog for one commit (not one per session): WAL's ReadWalInfo stops
  // counting records at the second transaction's differing timestamp, so a single session-spanning
  // log silently truncated multi-commit captures on read. Caller opens, captures into, and
  // Finalize()s the returned log immediately — there is no long-lived log.
  // `seq_num`: embedded in WalFile metadata for ordering; wall-clock filenames can collide within
  // the same microsecond — seq_num (pass NextBranchCommitTs()) is the collision-immune tie-break.
  std::unique_ptr<BranchLog> CreateCommitLog(uint64_t seq_num) {
    return std::make_unique<BranchLog>(branch_log_session_directory_, branch_log_items_, branch_log_mapper_, seq_num);
  }

  std::unique_ptr<storage::InMemoryStorage> diff_engine_;
  std::unique_ptr<storage::Storage::Accessor> historical_base_;
  // Not owned -- see set_current_diff_txn()/current_diff_txn()'s own doc-comment above. Points
  // into CurrentDB::db_transactional_accessor_ for the duration of one query; nullptr otherwise.
  storage::Storage::Accessor *current_diff_txn_{nullptr};

  // gids this branch has explicitly DELETEd — must never resolve again from either side. A bare
  // diff-engine miss cannot distinguish "deleted" from "never touched" for a not-yet-COW'd fork
  // vertex; this set closes that resurrection hazard. Checked first in ResolveVertex and
  // UnionVerticesIterable::SeekNext. Populated by TombstoneVertex and the WalVertexDelete replay.
  std::unordered_set<storage::Gid> tombstoned_vertices_;

  // PENDING tombstones for the current, not-yet-committed transaction. Separate from
  // tombstoned_vertices_ because TombstoneVertex is called eagerly at Pull-time — a DELETE that
  // then aborts must NOT leave a permanent tombstone behind. IsVertexTombstoned checks both sets so
  // a just-deleted gid stays hidden to itself mid-transaction. Resolved once per transaction:
  //   - CaptureCommitIfBranch promotes to tombstoned_vertices_ on commit.
  //   - DiscardPendingTombstones clears without promotion on abort.
  // Always empty between transactions (single-writer exclusivity).
  std::unordered_set<storage::Gid> pending_tombstoned_vertices_;

  // Symmetric for edges — populated by TombstoneEdge and the WalEdgeDelete replay handler.
  std::unordered_set<storage::Gid> tombstoned_edges_;

  // Symmetric for edges — promoted/discarded exactly like pending_tombstoned_vertices_.
  std::unordered_set<storage::Gid> pending_tombstoned_edges_;

  // Per-kind change filters (see branch_change_filter.hpp). In-memory; re-seeded from changelog on
  // every checkout. Monotonic + single-writer — no extra ownership concerns beyond BranchContext's.
  BranchChangeFilters change_filters_;

  // Per-session-unique UUID subdirectory under the branch's WAL root (separate per session:
  // two sessions restart their seq-num counting from scratch, so wall-clock-derived filenames
  // could collide within the same microsecond if they shared a directory). Stored alongside the
  // two other BranchLog construction ingredients CreateCommitLog() needs on every commit.
  std::filesystem::path branch_log_session_directory_;
  storage::SalientConfig::Items branch_log_items_;
  storage::NameIdMapper *branch_log_mapper_;

  // Per-session monotonic counter for captured branch-commit WAL timestamps -- see
  // NextBranchCommitTs()'s own doc-comment. Starts at 0; first captured commit gets ts 1.
  uint64_t branch_commit_ts_{0};

  // Cumulative WAL delta records across this branch's whole life (prior sessions + current session).
  // Bounds FLAGS_versioning_max_changelog_length — the cap must apply to the whole life, not just
  // this session.
  uint64_t changelog_length_{0};
};

}  // namespace memgraph::versioning
