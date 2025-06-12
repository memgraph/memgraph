// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/replication/replication_transaction.hpp"

namespace memgraph::storage {

auto TransactionReplication::FinalizePrepareCommitPhase(uint64_t durability_commit_timestamp,
                                                        DatabaseAccessProtector db_acc) -> bool {
  bool strict_sync_replicas_succ{true};
  MG_ASSERT(locked_clients->empty() || db_acc.has_value(),
            "Any clients assumes we are MAIN, we should have gatekeeper_access_wrapper so we can correctly "
            "handle ASYNC tasks");
  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(durability_commit_timestamp); },
                                   replica_stream);

    const auto finalized = client->FinalizeTransactionReplication(db_acc, replica_stream, durability_commit_timestamp);

    // TODO: (andi) Think about this when handling mixed cluster of sync,strict_sync and async replicas
    // The current idea is to run 2PC only on strict sync replicas and then if they are all ok, send prepare-commit
    // with the immediate commit to sync and async replicas
    if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC ||
        client->Mode() == replication_coordination_glue::ReplicationMode::SYNC) {
      strict_sync_replicas_succ = finalized && strict_sync_replicas_succ;
    }
  }
  return strict_sync_replicas_succ;
}

auto TransactionReplication::SendFinalizeCommitRpc(bool const decision, utils::UUID const &storage_uuid,
                                                   DatabaseAccessProtector db_acc,
                                                   uint64_t const durability_commit_timestamp) const noexcept -> bool {
  bool strict_sync_replicas_succ{true};
  for (auto &client : *locked_clients) {
    if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
      strict_sync_replicas_succ &=
          client->SendFinalizeCommitRpc(decision, storage_uuid, db_acc, durability_commit_timestamp);
    }
  }
  return strict_sync_replicas_succ;
}

auto TransactionReplication::ReplicationStartSuccessful() const -> bool {
  return locked_clients->empty() || !streams.empty();
}

auto TransactionReplication::ShouldRunTwoPC() const -> bool {
  return std::ranges::any_of(*locked_clients, [](auto const &client) {
    return client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC;
  });
}

}  // namespace memgraph::storage
