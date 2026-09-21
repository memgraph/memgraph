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

#include "versioning/branch_reconstruction.hpp"

#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::versioning {

storage::durability::WalTxnEndPos CaptureBranchCommit(BranchLog &branch_log, const storage::Transaction &transaction,
                                                      storage::Storage *target_storage, uint64_t commit_timestamp,
                                                      uint64_t *out_record_count) {
  using storage::Delta;
  using storage::PreviousPtr;

  uint64_t record_count = 0;

  for (const auto &delta : transaction.deltas) {
    // ADD_IN_EDGE/REMOVE_IN_EDGE are the vertex-side mirror of an edge's ADD_OUT_EDGE/
    // REMOVE_OUT_EDGE half and carry no independent WAL record of their own -- mirrors main's own
    // commit-time owner-resolution (inmemory/storage.cpp's `append_deltas`).
    if (delta.action == Delta::Action::ADD_IN_EDGE || delta.action == Delta::Action::REMOVE_IN_EDGE) {
      continue;
    }

    // Resolve the delta's true owner by walking `prev` past any intermediate DELTA links -- the
    // same walk main's real commit-time WAL append performs.
    auto owner = delta.prev.Get();
    while (owner.type == PreviousPtr::Type::DELTA) {
      owner = owner.delta->prev.Get();
    }

    if (owner.type == PreviousPtr::Type::VERTEX) {
      branch_log.AppendDelta(delta, owner.vertex, commit_timestamp, target_storage);
      ++record_count;
    } else if (owner.type == PreviousPtr::Type::EDGE) {
      // Only SET_PROPERTY is WAL-encoded for edges -- edge create/delete is captured entirely
      // through the endpoint vertices' ADD_OUT_EDGE/RECREATE_OBJECT/DELETE_OBJECT deltas instead.
      if (delta.action != Delta::Action::SET_PROPERTY) continue;
      auto *edge = owner.edge;
      // Same cache main's own commit path reads (transaction.hpp's EdgeSetPropertyInfo) -- not a
      // test-only invention.
      auto info = transaction.GetEdgeSetPropertyInfo(edge->gid);
      branch_log.AppendDelta(delta, edge, commit_timestamp, target_storage, info.in_vertex_gid, info.edge_type_id);
      ++record_count;
    }
    // DELTA/NULL_PTR owners: allocation failed or otherwise unresolved -- nothing to capture,
    // mirrors main's own handling of the same cases.
  }

  auto const txn_end_pos = branch_log.AppendTransactionEnd(commit_timestamp);

  if (out_record_count != nullptr) {
    // +1 for the WalTransactionEnd delimiter AppendTransactionEnd just wrote -- ReadAll/
    // CollectBranchChangelog count it too, so this MUST match `changelog.size()`'s unit (which
    // seeds BranchContext::changelog_length_ on re-checkout); otherwise the retention cap drifts by
    // one-per-commit vs. the persisted truth.
    *out_record_count = record_count + 1;
  }

  return txn_end_pos;
}

}  // namespace memgraph::versioning
