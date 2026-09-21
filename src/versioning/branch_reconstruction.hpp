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

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "versioning/branch_log.hpp"

namespace memgraph::versioning {

// CAPTURE hook: walks every delta a (now fully-applied) branch transaction produced, resolves each
// one's true owner (vertex or edge) by walking `delta.prev` past intermediate DELTA links -- the
// identical owner-resolution main's own commit-time WAL append performs (inmemory/storage.cpp's
// `append_deltas`) -- and forwards the corresponding forward record into `branch_log` via
// `BranchLog::AppendDelta`, then closes the transaction with `AppendTransactionEnd(commit_timestamp)`.
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
//
// `out_record_count` (chunk 10, D5/R13 -- retention-cap enforcement): defaulted, opt-in out-param.
// When non-null, receives the TOTAL number of records `BranchLog::ReadAll`/`CollectBranchChangelog`
// will later see for this commit: the forward WAL delta records this call appended to `branch_log`
// via `BranchLog::AppendDelta` (NOT the raw `transaction.deltas` count -- ADD_IN_EDGE/REMOVE_IN_EDGE
// deltas and non-SET_PROPERTY edge-owned deltas are skipped, see the loop below), PLUS ONE for the
// `WalTransactionEnd` delimiter that `AppendTransactionEnd` unconditionally writes below --
// `storage::durability::ReadWalInfo`'s `num_deltas` (which drives both `BranchLog::ReadAll`'s loop
// count and thus `CollectBranchChangelog`'s returned vector size) counts that delimiter as a record
// too, so this out-param MUST match that unit exactly. The caller (`Interpreter::Commit`) uses this
// to track `BranchContext`'s cumulative `ChangelogLength()` (itself seeded from
// `changelog.size()` on re-checkout, see `BuildFromFork`) and enforce
// `FLAGS_versioning_max_changelog_length` BEFORE the diff-engine transaction reaches
// `PrepareForCommitPhase` -- see that call site's own comment for why rejecting here is abort-safe.
// A unit mismatch here would make the live-session counter under- or over-count by one per commit
// relative to the persisted truth it is periodically re-seeded from.
storage::durability::WalTxnEndPos CaptureBranchCommit(BranchLog &branch_log, const storage::Transaction &transaction,
                                                      storage::Storage *target_storage, uint64_t commit_timestamp,
                                                      uint64_t *out_record_count = nullptr);

}  // namespace memgraph::versioning
