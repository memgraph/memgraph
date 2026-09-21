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
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::versioning {

// Pass-1-computed fork-state snapshots (see merge.cpp's top-of-file comment for why these are
// captured ahead of time rather than read lazily against a still-open `historical` accessor).
// Public (not merge.cpp-local) since M1 (Batch 4, "MERGE = normal commit") exposes them via
// MergeResult below: the interpreter's MERGE handler needs a fork-time "old value" for every
// property an after-commit TriggerContext row reports (D3 guarantees fork-state == pre-merge main,
// so these snapshots ARE the correct "old" value even though the WAL delta itself only carries the
// NEW value).
struct VertexForkSnapshot {
  std::set<storage::LabelId> labels;
  std::map<storage::PropertyId, storage::PropertyValue> properties;
};

struct EdgeForkSnapshot {
  std::map<storage::PropertyId, storage::PropertyValue> properties;
  // The FROM endpoint's gid at fork time -- see merge.cpp's FindHistoricalEdgeByEndpoint doc-comment
  // for why edges need this (unlike vertices, a bare gid lookup is not reliable for light edges).
  storage::Gid from_vertex_gid;
};

// Graph Versioning CHUNK 6: MERGE -- replay a branch's forward change-log onto CURRENT main as a
// single, real, committing transaction (spec A.1/D3). This is the ONLY path that ever writes main
// on behalf of a branch, so correctness here is non-negotiable:
//
//   - Atomicity (D3): a merge either fully applies and commits, or is rejected wholesale, leaving
//     main byte-unchanged AND the branch's own fork pin intact (R37) so the user can fix up the
//     branch and retry. There is no partial-application state.
//   - Conflict detection (R9/D3): if main and the branch both touched the same fork-existing
//     object since the fork point, the merge is rejected (never silently "last write wins").
//   - R11 (no crash): replayed CREATE operations are checked against main's LIVE state before
//     ever calling the underlying explicit-gid create primitives, and a colliding gid is remapped
//     to a freshly-allocated, guaranteed-unique one rather than colliding.
//   - R19 (no silent data loss): a vertex gid collision is never allowed to fall through as a
//     silent no-op (see CreateVertexEx's own std::nullopt-on-collision contract).
//
// See merge.cpp's top-of-file comment for the conflict-detection MECHANISM chosen (explicit
// fork-vs-now value comparison against a NORMAL committing transaction) and why the alternative
// (forcing start_timestamp = fork_ts to get PrepareForWrite's serialization check "for free") was
// rejected for this chunk. It also explains why the implementation is internally two passes (a
// read-only classification/snapshot pass against HistoricalAccess(fork_ts), fully closed, THEN the
// real committing transaction opened via main.UniqueAccess()) rather than one: pass 1 must be fully
// closed before pass 2 opens, both because HistoricalAccess's SHARED main_lock_ hold would
// otherwise coexist with pass 2's UniqueAccess() (which needs main_lock_ fully unlocked) and
// because UniqueAccess is what makes CreateVertexEx/CreateEdgeEx's explicit-gid create path safe
// (R11) -- see the top-of-file comment for the full reasoning.
enum class MergeErrorKind : uint8_t {
  // main.HistoricalAccess(fork_ts) failed -- the branch's own fork pin is not (or no longer) held.
  // Should not happen for a live, undropped branch; defensive.
  kForkPinLost,
  // The change-log references an object in a way that is inconsistent with itself (e.g. modifies
  // a gid that is neither branch-created earlier in this same log nor present in the fork-state
  // base) -- a corrupt change-log or a caller passing the wrong fork_ts.
  kCorruptChangelog,
  // D3: main and the branch both changed the same fork-existing object since the fork point.
  kModifyConflict,
  // A storage write call failed for a reason other than a D3 modify-conflict (e.g. a genuine
  // concurrent serialization error, a disabled-properties-on-edges failure, ...).
  kApplyFailed,
  // PrepareForCommitPhase itself failed (constraint violation, replication error, ...) after every
  // delta applied cleanly.
  kCommitFailed,
};

struct MergeError {
  MergeErrorKind kind;
  std::string message;                         // human-readable, names the colliding/offending gid(s)
  std::vector<storage::Gid> conflicting_gids;  // the gid(s) implicated, for programmatic use
};

struct MergeResult {
  // Branch-local (change-log) gid -> the actual gid it ended up with on main. Only populated for
  // objects that collided with main's live state and were therefore remapped to a fresh gid (R11);
  // the common, non-colliding case is simply absent from these maps (the change-log's own gid was
  // reused as-is).
  std::unordered_map<storage::Gid, storage::Gid> vertex_gid_remap;
  std::unordered_map<storage::Gid, storage::Gid> edge_gid_remap;
  uint64_t vertices_created{};
  uint64_t edges_created{};
  uint64_t objects_modified{};
  uint64_t objects_deleted{};
  // Set iff PrepareForCommitPhase reported a storage::ReplicationError with transaction_committed
  // == true (main committed, but one or more replicas failed to also apply it) -- the merge is
  // still a SUCCESS (main has the branch's changes), this is surfaced for the caller to log/relay,
  // mirroring how query/interpreter.cpp treats the identical case for ordinary write queries. See
  // merge.cpp's commit-handling comment for why this exact path has no unit-test coverage.
  std::optional<std::string> replication_warning;

  // --- Batch 4 ("MERGE = normal commit") additions -- after-commit triggers (#4) ---------------

  // M1: pass-1's fork-state snapshots (gid -> value AT fork_ts), moved out of merge.cpp's own
  // pass-1 locals on success. The interpreter's MERGE handler reads these for the "old" side of
  // every SET/REMOVE property (and label) row it assembles for the after-commit TriggerContext --
  // the WAL delta itself only ever carries the NEW value, and D3's own conflict-check invariant
  // ("fork-existing objects the merge touches are byte-identical between fork_ts and pre-merge
  // main, or the merge is rejected") is exactly what makes the fork snapshot a correct stand-in for
  // "main's value immediately before this merge" -- there is no other way to recover it post-commit.
  std::unordered_map<storage::Gid, VertexForkSnapshot> vertex_fork_snapshots;
  std::unordered_map<storage::Gid, EdgeForkSnapshot> edge_fork_snapshots;

  // M1/#4 (classification, needed to build a CORRECT TriggerContext): pass-1's own branch-local-vs-
  // fork-existing classification (see merge.cpp's WalVertexCreate/WalEdgeCreate `classify` comment,
  // the Q6 fix). A `WalVertexCreate`/`WalEdgeCreate` record is emitted BOTH for a genuine new
  // branch-native object AND for a COW echo of a fork-existing one that pass 2's `apply` step
  // no-ops -- without this set the interpreter's MERGE handler could not tell "really created" (a
  // CREATED_* trigger row) apart from "already existed, only its later Set/AddLabel records are
  // real changes" (an UPDATED_* row), and would double-report or mis-tag COW echoes as creations.
  std::unordered_set<storage::Gid> branch_local_vertices;
  std::unordered_set<storage::Gid> branch_local_edges;

  // R4 (rework, supersedes the original M3 design): MergeBranch does NOT retain its own committed
  // accessor past this function's return, and does NOT capture deleted-vertex/edge rows for
  // after-commit triggers. The original M3 design kept `merge` (a UNIQUE accessor -- it holds
  // main's exclusive main_lock_ via storage_guard_, released only at destruction) alive across the
  // interpreter's async after-commit trigger dispatch so deleted-object rows (which
  // TriggerContext::AdaptForAccessor can never re-bind post-commit -- the gid no longer exists on
  // main) would stay valid. CONFIRMED to deadlock: retaining that accessor blocks ALL main access
  // -- reads included -- for the entire duration of trigger dispatch, which is a non-starter both
  // for tests (a READ accessor opened on main right after MergeBranch hangs) and in production
  // (it would stall every other session touching this database). MERGE's after-commit triggers
  // therefore report CREATED/UPDATED objects only (see the interpreter's MERGE handler doc-comment
  // for the full v1-limitation writeup); `merge` falls out of scope and finalizes normally at the
  // end of MergeBranch, exactly like every ordinary write transaction.
};

// Replays `changelog` (a branch's own forward WAL-format change-log, e.g. versioning::BranchLog::
// ReadAll's return value) onto `main`, as a single committing transaction, conflict-checked against
// `fork_ts` (D3). `name_id_mapper` resolves the change-log's label/property/edge-type NAMES back to
// ids -- in practice the same mapper `main` itself uses. `commit_args` is forwarded to
// PrepareForCommitPhase unchanged (the caller -- chunk 7 -- supplies the real DatabaseProtector;
// unit tests use memgraph::tests::MakeMainCommitArgs()).
//
// On success: main now reflects the branch's change-log applied on top of whatever main looked
// like at the time of the call. On failure: main is byte-unchanged (the merge transaction is aborted,
// never partially committed) and the branch's fork pin at `fork_ts` is untouched -- both the
// branch and main remain exactly as they were before the call, so the caller may fix up the branch
// and retry the merge.
//
// Does NOT drop the branch or release its fork pin on success -- that is the caller's (chunk 7's)
// job once it has also updated the branch registry.
std::expected<MergeResult, MergeError> MergeBranch(storage::InMemoryStorage &main, uint64_t fork_ts,
                                                   const std::vector<storage::durability::WalDeltaData> &changelog,
                                                   storage::NameIdMapper *name_id_mapper,
                                                   storage::CommitArgs commit_args);

}  // namespace memgraph::versioning
