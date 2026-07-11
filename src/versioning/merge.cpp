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

// Graph Versioning CHUNK 6: MERGE (see merge.hpp's class comment for the correctness contract).
//
// CONFLICT-DETECTION MECHANISM -- chosen and why:
//
// Two designs were considered.
//
//   (a) Open the merge transaction with start_timestamp = fork_ts (mirroring
//       InMemoryStorage::HistoricalAccess) and let the ENGINE's own PrepareForWrite (mvcc.hpp)
//       serialization check do the conflict detection for free: any object main touched with a
//       commit timestamp >= fork_ts would trip `has_serialization_error` the moment we tried to
//       write it. This is elegant in principle, but forcing start_timestamp back to an
//       already-used, SHARED value (fork_ts is owned by the branch's own GC fork pin) requires a
//       transaction that is simultaneously (i) writable and (ii) exempt from the commit/abort
//       paths' `commit_log_->MarkFinished(transaction_.start_timestamp)` calls -- the existing
//       `Transaction::is_historical_` flag provides exactly that MarkFinished exemption, but ALSO
//       hard-blocks every write (`MG_ASSERT(!is_historical_, ...)` in
//       Transaction::EnsureCommitInfoExists) -- so reusing it is not an option, and inventing a
//       second, writable-but-shared-start-timestamp transaction mode would mean patching core MVCC
//       commit/abort semantics (transaction.hpp + 3 call sites in inmemory/storage.cpp) for a
//       single caller. Too large a blast radius for a chunk whose #1 job is "never touch main
//       incorrectly".
//
//   (b) Open a completely ORDINARY committing transaction on main (start_timestamp = now, exactly
//       like any other write transaction) and do EXPLICIT, hand-rolled conflict detection: for
//       every fork-existing object the change-log modifies, compare its value as of the fork point
//       against its CURRENT value -- any difference (including "no longer exists", i.e. main
//       deleted it) is a D3 conflict. This needs zero new transaction modes: the merge
//       transaction's own Abort()/commit paths are the STANDARD ones (start_timestamp is a fresh,
//       transaction-owned value, so MarkFinished-ing it is always correct), which is exactly why a
//       REJECTED merge is trivially atomic and never touches the branch's fork pin.
//
// (b) was chosen. It costs an explicit value-diff per touched object (bounded by the change-log's
// own delta-scale, D5) instead of a "free" engine-level check, but it keeps every correctness
// property local to this file plus one small, purely-additive, read-only storage-engine helper
// (see EdgeGidExists's doc-comment in inmemory/storage.hpp) rather than touching core MVCC
// transaction semantics.
//
// TWO PASSES, not one -- and why `historical` never overlaps `merge`:
//
// PASS 1 (read-only, against `main.HistoricalAccess(fork_ts)` only) classifies every change-log
// record (branch-local-create vs. modifies-a-fork-existing-object) and, for every fork-existing
// object the log modifies, snapshots its fork-state value (labels+properties for a vertex,
// properties for an edge) into a plain in-memory map. `historical` is then closed (its
// std::unique_ptr is reset) BEFORE pass 2 ever opens the real merge transaction.
//
// PASS 2 opens the real merge transaction via main.UniqueAccess() (see below for why UniqueAccess
// specifically) and replays the change-log for real: CREATE-collision handling (R11/R19) reads
// `merge`'s live state directly; the D3 conflict check compares pass 1's stored fork-snapshot
// against `merge`'s CURRENT (live, pre-this-merge) value.
//
// Why not just keep `historical` open across the whole merge (a single pass, reading it lazily)?
// InMemoryStorage::HistoricalAccess's own doc-comment states it takes a SHARED (READ-type)
// main_lock_ guard for its ENTIRE lifetime. utils::ResourceLock (resource_lock.hpp) has NO
// per-thread ownership tracking at all -- it is a bare state+counter machine -- so a SAME-THREAD
// UniqueAccess() call made WHILE `historical`'s SHARED guard is still held would block forever
// waiting for a release that can only come from... itself: a guaranteed self-deadlock. Splitting
// into two passes and fully closing `historical` (its own nested scope, see MergeBranch below)
// before pass 2 opens `merge` sidesteps this entirely -- by the time pass 2 calls
// main.UniqueAccess(), NOTHING on this thread still holds main_lock_ in any mode, so
// UniqueAccess() acquires cleanly.
//
// PASS 2 USES main.UniqueAccess(), NOT Access(StorageAccessType::WRITE) -- CORRECTED after review:
// an earlier version of this file used Access(WRITE), reasoning that WRITE-type access is
// SHARED-compatible with concurrent ordinary write transactions and therefore avoids re-deriving
// the deadlock above. That reasoning was itself a bug (HIGH-1): CreateVertexEx/CreateEdgeEx bump
// `vertex_id_`/`edge_id_` via a compound (atomic-fetch-max, THEN separately insert-into-skiplist)
// sequence -- safe ONLY under their documented "no other concurrent writer touching these counters
// via CreateVertexEx/CreateEdgeEx right now" assumption (their only other caller, replica WAL
// replay, holds this because a replica never has a second concurrent writer). Under Access(WRITE),
// an ordinary CONCURRENT user transaction calling the auto-gid CreateVertex/CreateEdge could race
// that same sequence and lose the skip-list insert, tripping the UNCONDITIONAL
// MG_ASSERT(inserted, ...) in CreateVertexInternal/CreateEdgeInternal (inmemory/storage.cpp) --
// i.e. crash the process (R11). EdgeGidExists's pre-check does NOT close this: it is a plain
// check-then-act TOCTOU against a separate, later call, not a synchronized operation. Since pass 1
// has ALREADY closed `historical` (releasing the only SHARED main_lock_ hold this thread had)
// before pass 2 begins, there is no self-deadlock reason left to avoid UniqueAccess() -- it
// excludes every other accessor (reader or writer) for the merge's short, rare, control-plane
// duration, which closes the race with zero further engine changes.
//
// R11 (no edge-gid-collision crash): WalEdgeCreate never calls CreateEdgeEx before EdgeGidExists
// has confirmed the gid is free -- and, under UniqueAccess, that check-then-create is now
// race-free (no other accessor of any kind can be touching main concurrently); on collision,
// CreateEdge (auto-gid) is used instead and the branch's original gid is remapped
// (vertex_gid_remap/edge_gid_remap in MergeResult).
// R19 (no silent vertex-gid-collision loss): WalVertexCreate always inspects CreateVertexEx's
// std::nullopt-on-collision return and remaps rather than dropping the vertex.
//
// COMMIT-RESULT HANDLING (HIGH-2): PrepareForCommitPhase's failure variant is NOT uniformly "main
// wasn't touched". storage::ReplicationError specifically carries its own `transaction_committed`
// flag: true on the ordinary (non-STRICT_SYNC) replication path, where main finalizes the commit
// LOCALLY and only some REPLICA's ack failed (inmemory/storage.cpp's commit path: FinalizeCommitPhase
// has already run by the time it returns `std::unexpected{ReplicationError{.failures=...,
// .transaction_committed=true}}`); false only on the STRICT_SYNC/2PC path, where replicas voted no
// and main genuinely aborted (`AbortAndResetCommitTs()` runs before that variant is constructed).
// Treating both uniformly as "not committed" would, on the true-but-some-replica-failed case,
// report kCommitFailed for an ALREADY-COMMITTED merge -- the caller (per this chunk's own contract
// in merge.hpp, "the caller may fix up the branch and retry the merge" on failure) would then
// retry the SAME change-log against a main that already has it applied, re-triggering the R11/R19
// remap paths for objects that are no longer collisions-in-waiting but already-merged duplicates,
// and re-running D3 checks that would now spuriously conflict against the merge's own,
// just-committed state. Fixed below by std::visit-ing the error, mirroring the established
// precedent in query/plan/operator.cpp's HandlePeriodicCommitError and query/interpreter.cpp's own
// post-PrepareForCommitPhase handling: a ReplicationError with transaction_committed == true is
// treated as SUCCESS (MergeResult populated normally, with replication_warning set); every other
// case (transaction_committed == false, or any of ConstraintViolation/SerializationError/
// PersistenceError/ReplicaShouldNotWriteError) is kCommitFailed, correctly matching main's true
// un-committed state in every one of those cases.
//
// NOT unit-tested: memgraph::tests::MakeMainCommitArgs() (used by this chunk's own test suite)
// registers no replicas, so PrepareForCommitPhase can never actually produce a ReplicationError in
// this file's tests -- there is no way to reach the transaction_committed==true branch without a
// real replica (or a replication-layer test double this chunk does not have). The invariant above
// is verified by direct reading of inmemory/storage.cpp's commit path and by mirroring the
// query-engine's own already-shipped handling of the identical case, not by an executed test here;
// flagged rather than faked.

#include "versioning/merge.hpp"

#include <map>
#include <set>
#include <unordered_set>

#include <fmt/format.h>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::versioning {

namespace {

namespace sd = storage::durability;

std::set<storage::LabelId> LabelSet(const storage::VertexAccessor &v) {
  auto labels = v.Labels(storage::View::OLD);
  if (!labels.has_value()) {
    throw utils::BasicException("Failed to read labels for vertex {} while checking for a merge conflict.",
                                v.Gid().AsUint());
  }
  return std::set<storage::LabelId>(labels->begin(), labels->end());
}

std::map<storage::PropertyId, storage::PropertyValue> PropertyMap(const storage::VertexAccessor &v) {
  auto props = v.Properties(storage::View::OLD);
  if (!props.has_value()) {
    throw utils::BasicException("Failed to read properties for vertex {} while checking for a merge conflict.",
                                v.Gid().AsUint());
  }
  return std::move(*props);
}

std::map<storage::PropertyId, storage::PropertyValue> PropertyMap(const storage::EdgeAccessor &e) {
  auto props = e.Properties(storage::View::OLD);
  if (!props.has_value()) {
    throw utils::BasicException("Failed to read properties for edge {} while checking for a merge conflict.",
                                e.Gid().AsUint());
  }
  return std::move(*props);
}

// Pass-1-computed fork-state snapshots (see this file's top-of-file comment for why these are
// captured ahead of time rather than read lazily against a still-open `historical` accessor).
struct VertexForkSnapshot {
  std::set<storage::LabelId> labels;
  std::map<storage::PropertyId, storage::PropertyValue> properties;
};

struct EdgeForkSnapshot {
  std::map<storage::PropertyId, storage::PropertyValue> properties;
};

// D3, pass 2: has main changed fork-existing vertex `gid` (modified OR deleted) since the fork
// point? Compares pass 1's stored fork-state snapshot against `merge`'s CURRENT (live, "now")
// value.
//
// Deliberately View::OLD (not NEW) for the `merge`-side read: `merge`'s command_id never advances
// over the whole apply loop (AdvanceCommand is never called), so View::OLD undoes every delta THIS
// SAME merge transaction has already applied earlier in the pass, always presenting main's true
// pre-merge committed state -- exactly what the fork-vs-now comparison needs. (Contrast with the
// apply loop's own "find the object to operate on" lookups below, which need View::NEW precisely
// because they DO want to see this pass's own earlier writes -- e.g. a vertex the branch created
// two records ago.)
std::expected<void, MergeError> CheckVertexUnchangedSinceFork(const VertexForkSnapshot &fork_snapshot,
                                                              storage::ReplicationAccessor &merge, storage::Gid gid) {
  auto now_v = merge.FindVertex(gid, storage::View::OLD);
  if (!now_v.has_value()) {
    return std::unexpected(
        MergeError{.kind = MergeErrorKind::kModifyConflict,
                   .message = fmt::format("Merge conflict: main deleted vertex {} after the branch's fork point, "
                                          "but the branch also modified it.",
                                          gid.AsUint()),
                   .conflicting_gids = {gid}});
  }

  if (fork_snapshot.labels != LabelSet(*now_v) || fork_snapshot.properties != PropertyMap(*now_v)) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kModifyConflict,
        .message =
            fmt::format("Merge conflict: vertex {} was modified on main after the branch's fork point.", gid.AsUint()),
        .conflicting_gids = {gid}});
  }
  return {};
}

// Symmetric D3 check for edges (properties only -- endpoints/type never change post-creation).
std::expected<void, MergeError> CheckEdgeUnchangedSinceFork(const EdgeForkSnapshot &fork_snapshot,
                                                            storage::ReplicationAccessor &merge, storage::Gid gid) {
  auto now_e = merge.FindEdge(gid, storage::View::OLD);
  if (!now_e.has_value()) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kModifyConflict,
        .message =
            fmt::format("Merge conflict: main deleted edge {} after the branch's fork point, but the branch also "
                        "modified it.",
                        gid.AsUint()),
        .conflicting_gids = {gid}});
  }

  if (fork_snapshot.properties != PropertyMap(*now_e)) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kModifyConflict,
        .message =
            fmt::format("Merge conflict: edge {} was modified on main after the branch's fork point.", gid.AsUint()),
        .conflicting_gids = {gid}});
  }
  return {};
}

}  // namespace

std::expected<MergeResult, MergeError> MergeBranch(storage::InMemoryStorage &main, uint64_t fork_ts,
                                                   const std::vector<sd::WalDeltaData> &changelog,
                                                   storage::NameIdMapper *name_id_mapper,
                                                   storage::CommitArgs commit_args) {
  // Gids the branch itself created somewhere in the change-log -- these have no fork-state
  // counterpart (they didn't exist at fork_ts), so D3's fork-vs-now check does not apply to them;
  // they are purely the branch's own, newly-introduced objects. Populated in PASS 1 below and only
  // ever read (never mutated) in pass 2.
  std::unordered_set<storage::Gid> branch_local_vertices;
  std::unordered_set<storage::Gid> branch_local_edges;
  std::unordered_map<storage::Gid, VertexForkSnapshot> vertex_fork_snapshots;
  std::unordered_map<storage::Gid, EdgeForkSnapshot> edge_fork_snapshots;

  auto make_corrupt = [](std::string message, storage::Gid gid) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kCorruptChangelog, .message = std::move(message), .conflicting_gids = {gid}});
  };
  auto make_apply_failed = [](std::string message, storage::Gid gid) {
    return std::unexpected(
        MergeError{.kind = MergeErrorKind::kApplyFailed, .message = std::move(message), .conflicting_gids = {gid}});
  };

  // --------------------------------------------------------------------------------------------
  // PASS 1: read-only against `historical` (self-pinned at fork_ts). Classifies branch-local
  // creates and snapshots the fork-state of every fork-existing object the log modifies. No write
  // transaction is open yet -- nothing here can touch main.
  // --------------------------------------------------------------------------------------------
  {
    auto historical_or_err = main.HistoricalAccess(fork_ts);
    if (!historical_or_err.has_value()) {
      return std::unexpected(
          MergeError{.kind = MergeErrorKind::kForkPinLost,
                     .message = fmt::format("Cannot merge: fork timestamp {} is not (or no longer) pinned.", fork_ts),
                     .conflicting_gids = {}});
    }
    std::unique_ptr<storage::Storage::Accessor> historical = std::move(*historical_or_err);

    auto ensure_vertex_snapshot = [&](storage::Gid gid) -> std::expected<void, MergeError> {
      if (branch_local_vertices.contains(gid) || vertex_fork_snapshots.contains(gid)) return {};
      auto fork_v = historical->FindVertex(gid, storage::View::OLD);
      if (!fork_v.has_value()) {
        return make_corrupt(
            fmt::format("Branch change-log modifies vertex {} which is not present in the fork-state base -- "
                        "corrupt change-log or wrong fork_ts.",
                        gid.AsUint()),
            gid);
      }
      vertex_fork_snapshots.emplace(gid, VertexForkSnapshot{LabelSet(*fork_v), PropertyMap(*fork_v)});
      return {};
    };
    auto ensure_edge_snapshot = [&](storage::Gid gid) -> std::expected<void, MergeError> {
      if (branch_local_edges.contains(gid) || edge_fork_snapshots.contains(gid)) return {};
      auto fork_e = historical->FindEdge(gid, storage::View::OLD);
      if (!fork_e.has_value()) {
        return make_corrupt(
            fmt::format("Branch change-log modifies edge {} which is not present in the fork-state base -- "
                        "corrupt change-log or wrong fork_ts.",
                        gid.AsUint()),
            gid);
      }
      edge_fork_snapshots.emplace(gid, EdgeForkSnapshot{PropertyMap(*fork_e)});
      return {};
    };

    // MEDIUM-1: edge ops also snapshot their ENDPOINT vertices (best-effort for
    // WalEdgeSetProperty's optional from/to hints), not just the edge itself. This lets pass 2
    // tell apart "the change-log is genuinely corrupt" (an endpoint that never existed anywhere,
    // at fork or branch-created) from "main deleted this fork-existing endpoint after the fork
    // point" (a real D3 conflict) when a later FindVertex(endpoint) comes up empty -- see
    // `missing_vertex_error` in pass 2 below.
    auto classify =
        utils::Overloaded{[&](sd::WalVertexCreate const &data) -> std::expected<void, MergeError> {
                            branch_local_vertices.insert(data.gid);
                            return {};
                          },
                          [&](sd::WalVertexDelete const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalVertexAddLabel const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalVertexRemoveLabel const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalVertexSetProperty const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalEdgeCreate const &data) -> std::expected<void, MergeError> {
                            branch_local_edges.insert(data.gid);
                            if (auto check = ensure_vertex_snapshot(data.from_vertex); !check) return check;
                            return ensure_vertex_snapshot(data.to_vertex);
                          },
                          [&](sd::WalEdgeDelete const &data) -> std::expected<void, MergeError> {
                            if (auto check = ensure_edge_snapshot(data.gid); !check) return check;
                            if (auto check = ensure_vertex_snapshot(data.from_vertex); !check) return check;
                            return ensure_vertex_snapshot(data.to_vertex);
                          },
                          [&](sd::WalEdgeSetProperty const &data) -> std::expected<void, MergeError> {
                            if (auto check = ensure_edge_snapshot(data.gid); !check) return check;
                            if (data.from_gid.has_value()) {
                              if (auto check = ensure_vertex_snapshot(*data.from_gid); !check) return check;
                            }
                            if (data.to_gid.has_value() && *data.to_gid != storage::kInvalidGid) {
                              if (auto check = ensure_vertex_snapshot(*data.to_gid); !check) return check;
                            }
                            return {};
                          },
                          [&](sd::WalTransactionStart const &) -> std::expected<void, MergeError> { return {}; },
                          [&](sd::WalTransactionEnd const &) -> std::expected<void, MergeError> { return {}; },
                          [&](auto const &) -> std::expected<void, MergeError> { return {}; }};

    for (const auto &delta : changelog) {
      auto result = std::visit(classify, delta.data_);
      if (!result.has_value()) {
        // Nothing has ever been written to main (pass 1 only ever reads `historical`) -- just
        // propagate the error, `historical` unwinds itself on scope exit below.
        return std::unexpected(std::move(result.error()));
      }
    }
    // `historical` (and its SHARED main_lock_ guard + self-pin) is released HERE, before pass 2
    // opens the real merge transaction -- see this file's top-of-file comment for why that
    // ordering is load-bearing.
  }

  // --------------------------------------------------------------------------------------------
  // PASS 2: the real, committing transaction. main.UniqueAccess() -- exclusive of every other
  // accessor -- see the top-of-file comment (HIGH-1) for why this, and not Access(WRITE), is the
  // correct choice, and for why it no longer risks the self-deadlock that originally motivated
  // avoiding it (pass 1's `historical` is fully closed by this point).
  // --------------------------------------------------------------------------------------------
  std::unique_ptr<storage::ReplicationAccessor> merge(
      static_cast<storage::ReplicationAccessor *>(main.UniqueAccess().release()));

  // Branch-local gid -> the gid it actually ended up with on main (only populated on an R11
  // collision remap). Original-gid keyed, so every later change-log record that references the
  // SAME original gid (a further mutation, or an edge endpoint) resolves consistently.
  std::unordered_map<storage::Gid, storage::Gid> vertex_remap;
  std::unordered_map<storage::Gid, storage::Gid> edge_remap;

  auto resolve_vertex = [&](storage::Gid gid) {
    auto it = vertex_remap.find(gid);
    return it == vertex_remap.end() ? gid : it->second;
  };
  auto resolve_edge = [&](storage::Gid gid) {
    auto it = edge_remap.find(gid);
    return it == edge_remap.end() ? gid : it->second;
  };

  // MEDIUM-1: classify an endpoint vertex `gid` (ORIGINAL, pre-remap -- matches how
  // vertex_fork_snapshots/branch_local_vertices are keyed) that pass 2 could not find on main via
  // an edge operation. If pass 1 captured a fork-state snapshot for it, it existed at fork_ts and
  // is referenced here only via an edge op (never its own labels/properties) -- its disappearance
  // can only mean main deleted it after the fork point: a genuine D3 conflict, not a corrupt
  // change-log (the bug this fixes: such a case previously surfaced as kCorruptChangelog naming
  // the EDGE, never as kModifyConflict naming the actual missing VERTEX). If `gid` is neither
  // branch-local nor snapshotted, pass 1's own ensure_vertex_snapshot would already have failed the
  // whole merge before pass 2 ever started -- this fallback is purely defensive.
  auto missing_vertex_error = [&](storage::Gid gid, std::string_view context) -> MergeError {
    if (vertex_fork_snapshots.contains(gid)) {
      return MergeError{
          .kind = MergeErrorKind::kModifyConflict,
          .message = fmt::format("Merge conflict: main deleted vertex {} after the branch's fork point, but the "
                                 "branch's change-log still references it {}.",
                                 gid.AsUint(),
                                 context),
          .conflicting_gids = {gid}};
    }
    return MergeError{
        .kind = MergeErrorKind::kCorruptChangelog,
        .message = fmt::format("Branch change-log references vertex {} {} that cannot be found on main and was "
                               "never present in the fork-state base.",
                               gid.AsUint(),
                               context),
        .conflicting_gids = {gid}};
  };

  MergeResult stats;

  auto apply = utils::Overloaded{
      [&](sd::WalVertexCreate const &data) -> std::expected<void, MergeError> {
        auto v = merge->CreateVertexEx(data.gid);
        if (!v.has_value()) {
          // R11/R19: gid already occupied in main's live state (main's own post-fork create, or a
          // not-yet-reclaimed tombstone) -- remap to a freshly-allocated, guaranteed-unique gid
          // rather than silently losing this vertex.
          auto remapped = merge->CreateVertex();
          vertex_remap[data.gid] = remapped.Gid();
        }
        ++stats.vertices_created;
        return {};
      },
      [&](sd::WalVertexDelete const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW (not OLD): this vertex may have been created earlier in THIS SAME merge pass
        // (same command_id throughout -- AdvanceCommand is never called), and View::OLD would undo
        // that same-command create's own DELETE_OBJECT sentinel delta, hiding it. Mirrors
        // dbms/inmemory/replication_handlers.cpp's own delta-applier, which uses View::NEW
        // uniformly for this exact reason.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log deletes vertex {} which cannot be found on main.", resolved.AsUint()),
              data.gid);
        }
        auto del = merge->DeleteVertex(&*v);
        if (!del.has_value()) {
          if (del.error() == storage::Error::VERTEX_HAS_EDGES) {
            // MEDIUM-2 (targeted mitigation, not the full adjacency reconciliation deferred to
            // chunk 8): the branch's own commit could only have recorded this delete if the
            // vertex had NO edges at the time (Memgraph refuses to delete a vertex with edges
            // still attached) -- so if it has edges NOW, main must have attached at least one
            // since the fork point. That is a real D3 conflict on V's adjacency, not a generic
            // apply failure; reclassify it as such rather than leaving it as kApplyFailed. This
            // does not attempt to name the specific edge(s) main added (that needs adjacency-set
            // reconciliation, chunk 8's job), only to get the ERROR KIND right.
            return std::unexpected(MergeError{
                .kind = MergeErrorKind::kModifyConflict,
                .message = fmt::format("Merge conflict: main attached an edge to vertex {} after the branch's "
                                       "fork point, so the branch's delete can no longer be applied.",
                                       resolved.AsUint()),
                .conflicting_gids = {data.gid}});
          }
          return make_apply_failed(
              fmt::format("Failed to delete vertex {} while applying the merge.", resolved.AsUint()), data.gid);
        }
        if (!del->has_value()) {
          return make_apply_failed(
              fmt::format("Failed to delete vertex {} while applying the merge.", resolved.AsUint()), data.gid);
        }
        ++stats.objects_deleted;
        return {};
      },
      [&](sd::WalVertexAddLabel const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW -- see the WalVertexDelete case above for why.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(fmt::format("Branch change-log adds a label to vertex {} which cannot be found on main.",
                                          resolved.AsUint()),
                              data.gid);
        }
        auto ret = v->AddLabel(merge->NameToLabel(data.label));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to add label to vertex {} while applying the merge.", resolved.AsUint()), data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalVertexRemoveLabel const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW -- see the WalVertexDelete case above for why.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log removes a label from vertex {} which cannot be found on main.",
                          resolved.AsUint()),
              data.gid);
        }
        auto ret = v->RemoveLabel(merge->NameToLabel(data.label));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to remove label from vertex {} while applying the merge.", resolved.AsUint()),
              data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalVertexSetProperty const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW -- see the WalVertexDelete case above for why.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log sets a property on vertex {} which cannot be found on main.",
                          resolved.AsUint()),
              data.gid);
        }
        auto ret =
            v->SetProperty(merge->NameToProperty(data.property), storage::ToPropertyValue(data.value, name_id_mapper));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to set a property on vertex {} while applying the merge.", resolved.AsUint()),
              data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalEdgeCreate const &data) -> std::expected<void, MergeError> {
        auto resolved_from = resolve_vertex(data.from_vertex);
        auto resolved_to = resolve_vertex(data.to_vertex);
        // View::NEW -- an endpoint may have been created earlier in THIS SAME merge pass (see the
        // WalVertexDelete case above for why View::OLD would hide it).
        auto from_v = merge->FindVertex(resolved_from, storage::View::NEW);
        auto to_v = merge->FindVertex(resolved_to, storage::View::NEW);
        if (!from_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.from_vertex, fmt::format("as the source endpoint of edge {}", data.gid.AsUint())));
        }
        if (!to_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.to_vertex, fmt::format("as the target endpoint of edge {}", data.gid.AsUint())));
        }
        auto edge_type = merge->NameToEdgeType(data.edge_type);
        if (merge->EdgeGidExists(data.gid)) {
          // R11: gid already occupied on main -- remap via the engine's own guaranteed-unique
          // auto-gid allocator rather than ever calling CreateEdgeEx on a colliding gid (which
          // would MG_ASSERT-crash for heavy edges, see EdgeGidExists's doc-comment).
          auto edge_result = merge->CreateEdge(&*from_v, &*to_v, edge_type);
          if (!edge_result.has_value()) {
            return make_apply_failed(
                fmt::format("Failed to create a remapped edge for branch edge {}.", data.gid.AsUint()), data.gid);
          }
          edge_remap[data.gid] = edge_result->Gid();
        } else {
          auto edge_result = merge->CreateEdgeEx(&*from_v, &*to_v, edge_type, data.gid);
          if (!edge_result.has_value()) {
            return make_apply_failed(
                fmt::format("Failed to create edge {} while applying the merge.", data.gid.AsUint()), data.gid);
          }
        }
        ++stats.edges_created;
        return {};
      },
      [&](sd::WalEdgeDelete const &data) -> std::expected<void, MergeError> {
        const bool is_branch_local = branch_local_edges.contains(data.gid);
        if (!is_branch_local) {
          if (auto check = CheckEdgeUnchangedSinceFork(edge_fork_snapshots.at(data.gid), *merge, data.gid); !check) {
            return std::unexpected(check.error());
          }
        }
        auto resolved = is_branch_local ? resolve_edge(data.gid) : data.gid;
        auto resolved_from = resolve_vertex(data.from_vertex);
        auto resolved_to = resolve_vertex(data.to_vertex);
        // View::NEW throughout this branch -- see the WalVertexDelete case above for why.
        auto from_v = merge->FindVertex(resolved_from, storage::View::NEW);
        auto to_v = merge->FindVertex(resolved_to, storage::View::NEW);
        if (!from_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.from_vertex, fmt::format("as the source endpoint of edge {}", resolved.AsUint())));
        }
        if (!to_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.to_vertex, fmt::format("as the target endpoint of edge {}", resolved.AsUint())));
        }
        auto edge_type = merge->NameToEdgeType(data.edge_type);
        auto e = merge->FindEdge(resolved, storage::View::NEW, edge_type, &*from_v, &*to_v);
        if (!e.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log deletes edge {} which cannot be found on main.", resolved.AsUint()),
              data.gid);
        }
        auto ret = merge->DeleteEdge(&*e);
        if (!ret.has_value()) {
          return make_apply_failed(fmt::format("Failed to delete edge {} while applying the merge.", resolved.AsUint()),
                                   data.gid);
        }
        ++stats.objects_deleted;
        return {};
      },
      [&](sd::WalEdgeSetProperty const &data) -> std::expected<void, MergeError> {
        const bool is_branch_local = branch_local_edges.contains(data.gid);
        if (!is_branch_local) {
          if (auto check = CheckEdgeUnchangedSinceFork(edge_fork_snapshots.at(data.gid), *merge, data.gid); !check) {
            return std::unexpected(check.error());
          }
        }
        auto resolved = is_branch_local ? resolve_edge(data.gid) : data.gid;

        // Tiered resolution mirroring BranchOverlay::EnsureOverlayEdge's own strategy (newest to
        // oldest WAL format), resolving each endpoint hint through vertex_remap first: a
        // pre-existing edge's endpoints are never remapped, but a branch-local edge's may be.
        // View::NEW throughout -- see the WalVertexDelete case above for why (an endpoint or the
        // edge itself may have been created earlier in this same merge pass).
        std::optional<storage::EdgeAccessor> e;
        if (data.from_gid.has_value() && data.to_gid.has_value() && data.edge_type.has_value() &&
            *data.to_gid != storage::kInvalidGid && !data.edge_type->empty()) {
          // MEDIUM-1 (gap closed, defensive/parity fix -- structurally unreachable via a
          // legitimately-produced change-log, NOT unit-tested): check each endpoint individually so
          // a missing endpoint that existed at fork_ts is reported as kModifyConflict naming the
          // VERTEX, exactly like the WalEdgeCreate/WalEdgeDelete cases above, instead of falling
          // through to the generic "edge not found" kCorruptChangelog below. In practice this branch
          // can't actually fire for a well-formed log: from_gid/to_gid always mirror an edge's true,
          // never-changing endpoints at record time (wal.cpp's EncodeDelta populates them straight
          // from the live Vertex* at the SetProperty call site -- see
          // dbms/inmemory/replication_handlers.cpp's identical "hint must exist" assumption), and the
          // storage engine unconditionally refuses to delete a vertex that still has an edge
          // attached (VERTEX_HAS_EDGES, enforced both on the live-transaction path and independently
          // on WAL-replay/recovery) -- so if this edge (branch-local or fork-existing) is still
          // findable at all, both its endpoints must be too. A fork-existing edge's disappearance is
          // covered earlier anyway by CheckEdgeUnchangedSinceFork's own "edge not found" check
          // (naming the edge, an acceptable D3 kModifyConflict even if not vertex-precise). Kept for
          // defense-in-depth / symmetry with the WalEdgeCreate/WalEdgeDelete handling above, not
          // because a reachable bug was found here.
          auto from_v = merge->FindVertex(resolve_vertex(*data.from_gid), storage::View::NEW);
          if (!from_v.has_value()) {
            return std::unexpected(missing_vertex_error(
                *data.from_gid, fmt::format("as the source endpoint of edge {}", resolved.AsUint())));
          }
          auto to_v = merge->FindVertex(resolve_vertex(*data.to_gid), storage::View::NEW);
          if (!to_v.has_value()) {
            return std::unexpected(missing_vertex_error(
                *data.to_gid, fmt::format("as the target endpoint of edge {}", resolved.AsUint())));
          }
          e = merge->FindEdge(resolved, storage::View::NEW, merge->NameToEdgeType(*data.edge_type), &*from_v, &*to_v);
        } else if (data.from_gid.has_value()) {
          auto from_v = merge->FindVertex(resolve_vertex(*data.from_gid), storage::View::NEW);
          if (!from_v.has_value()) {
            return std::unexpected(
                missing_vertex_error(*data.from_gid, fmt::format("as an endpoint of edge {}", resolved.AsUint())));
          }
          e = merge->FindEdge(resolved, resolve_vertex(*data.from_gid), storage::View::NEW);
        } else {
          e = merge->FindEdge(resolved, storage::View::NEW);
        }

        if (!e.has_value()) {
          return make_corrupt(fmt::format("Branch change-log sets a property on edge {} which cannot be found on main.",
                                          resolved.AsUint()),
                              data.gid);
        }
        auto ret =
            e->SetProperty(merge->NameToProperty(data.property), storage::ToPropertyValue(data.value, name_id_mapper));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to set a property on edge {} while applying the merge.", resolved.AsUint()),
              data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalTransactionStart const &) -> std::expected<void, MergeError> { return {}; },
      [&](sd::WalTransactionEnd const &) -> std::expected<void, MergeError> { return {}; },
      // Non-graph-data records (indices, constraints, enums, text/vector indices, TTL, ...) are out
      // of MergeBranch's scope, mirroring BranchOverlay::Materialize.
      [&](auto const &) -> std::expected<void, MergeError> { return {}; }};

  for (const auto &delta : changelog) {
    auto result = std::visit(apply, delta.data_);
    if (!result.has_value()) {
      // Atomic reject (D3): abort OUR OWN, freshly-opened transaction (start_timestamp is a
      // normal, transaction-owned value -- never fork_ts, and the branch's fork pin was already
      // released back in pass 1 by `historical`'s own destructor, independent of the branch's own
      // pin) -- main is left byte-unchanged.
      merge->Abort();
      return std::unexpected(std::move(result.error()));
    }
  }

  auto commit_result = merge->PrepareForCommitPhase(std::move(commit_args));
  if (!commit_result.has_value()) {
    // HIGH-2: NOT every commit_result failure means main was left untouched -- see the top-of-file
    // comment ("COMMIT-RESULT HANDLING") for the full storage::ReplicationError::
    // transaction_committed rationale and precedent (query/plan/operator.cpp's
    // HandlePeriodicCommitError, query/interpreter.cpp's own post-PrepareForCommitPhase handling).
    auto committed_with_warning =
        std::visit(utils::Overloaded{[](storage::ReplicationError const &err) -> std::optional<std::string> {
                                       if (err.transaction_committed) return storage::FormatReplicationError(err);
                                       return std::nullopt;
                                     },
                                     [](auto const &) -> std::optional<std::string> { return std::nullopt; }},
                   commit_result.error());

    if (!committed_with_warning.has_value()) {
      // Genuinely NOT committed: a real ConstraintViolation/SerializationError/PersistenceError/
      // ReplicaShouldNotWriteError, or a ReplicationError with transaction_committed == false (the
      // STRICT_SYNC/2PC path, where PrepareForCommitPhase's own AbortAndResetCommitTs() already
      // ran) -- main is left byte-unchanged in every one of these cases.
      return std::unexpected(MergeError{.kind = MergeErrorKind::kCommitFailed,
                                        .message = "Merge failed to commit onto main.",
                                        .conflicting_gids = {}});
    }
    // Main DID commit the merge; only a replica's ack failed to come back -- this is a SUCCESS,
    // just one worth surfacing to the caller (not unit-tested, see the top-of-file comment).
    stats.replication_warning = std::move(committed_with_warning);
  }

  const auto &committed_transaction = merge->GetTransaction();
  stats.commit_timestamp = committed_transaction.commit_info ? committed_transaction.commit_info->timestamp.load() : 0;
  stats.vertex_gid_remap = std::move(vertex_remap);
  stats.edge_gid_remap = std::move(edge_remap);
  return stats;
}

}  // namespace memgraph::versioning
