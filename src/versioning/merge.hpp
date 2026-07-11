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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"

namespace memgraph::versioning {

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
  uint64_t commit_timestamp{};
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
};

// Replays `changelog` (a branch's own forward WAL-format change-log, e.g. versioning::BranchLog::
// ReadAll's return value) onto `main`, as a single committing transaction, conflict-checked against
// `fork_ts` (D3). `name_id_mapper` resolves the change-log's label/property/edge-type NAMES back to
// ids -- in practice the same mapper `main` itself uses (mirrors BranchOverlay/BranchReconstruction's
// own constructor contract). `commit_args` is forwarded to PrepareForCommitPhase unchanged (the
// caller -- chunk 7 -- supplies the real DatabaseProtector; unit tests use
// memgraph::tests::MakeMainCommitArgs()).
//
// On success: main now reflects the branch's change-log applied on top of whatever main looked
// like at the time of the call (commit_timestamp in the result is main's own fresh commit
// timestamp for this merge). On failure: main is byte-unchanged (the merge transaction is aborted,
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
