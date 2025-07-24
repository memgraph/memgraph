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

// For all replicas, we append transaction end
// When handling STRICT_SYNC replica, we send deltas as part of the 1st phase of the 2PC protocol and wait for the
// response.
// When handling some other type of replica, it is checked whether there is another STRICT_SYNC replica. There are 2
// possible cluster combinations: STRICT_SYNC and ASYNC or SYNC and ASYNC. If there are no STRICT_SYNC replicas in the
// cluster, we send all deltas and commit immediately on replicas.
auto TransactionReplication::ShipDeltas(uint64_t durability_commit_timestamp, DatabaseAccessProtector db_acc) -> bool {
  bool success{true};
  MG_ASSERT(locked_clients->empty() || db_acc.has_value(),
            "Any clients assumes we are MAIN, we should have gatekeeper_access_wrapper so we can correctly "
            "handle ASYNC tasks");

  bool const should_run_2pc = ShouldRunTwoPC();
  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(durability_commit_timestamp); },
                                   replica_stream);
    // NOLINTNEXTLINE
    auto const finalized = std::invoke([&]() -> bool {
      // If I am STRICT SYNC replica, ship deltas as part of the 1st phase and preserve replica stream.
      // NOLINTNEXTLINE
      if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
        // NOLINTNEXTLINE
        return client->FinalizePrepareCommitPhase(db_acc, replica_stream, durability_commit_timestamp);
      }
      // If there are no STRICT_SYNC replicas, shipping deltas means finalizing the transaction
      // RPC stream gets destroyed => RPC lock released.
      if (!should_run_2pc) {
        // NOLINTNEXTLINE
        bool const res =
            client->FinalizeTransactionReplication(db_acc, std::move(replica_stream), durability_commit_timestamp);
        // Even if fails, we don't care, it's ASYNC
        if (client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
          return true;
        }
        return res;
      }
      // If ASYNC replica which is part of 2PC, just skip this
      // SYNC replica cannot be part of 2PC
      return true;
    });

    success &= finalized;
  }
  return success;
}

// RPC locks will get released at the end of this function for all STRICT_SYNC and ASYNC replicas
// We shouldn't execute this code for SYNC replicas, this is only executed if these replicas are part of STRICT_SYNC
// cluster
auto TransactionReplication::FinalizeTransaction(bool const decision, utils::UUID const &storage_uuid,
                                                 DatabaseAccessProtector db_acc,
                                                 uint64_t const durability_commit_timestamp) -> bool {
  bool strict_sync_replicas_succ{true};

  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
      const bool commit_res = client->SendFinalizeCommitRpc(decision, storage_uuid, db_acc, durability_commit_timestamp,
                                                            std::move(replica_stream));
      strict_sync_replicas_succ &= commit_res;
    } else if (client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
      if (decision) {
        client->FinalizeTransactionReplication(db_acc, std::move(replica_stream), durability_commit_timestamp);
      } else {
        // Reconnect needed because we optimistically prepared PrepareCommitReq message already
        client->AbortRpcClient();
      }
    }
  }
  return strict_sync_replicas_succ;
}

auto TransactionReplication::ShouldRunTwoPC() const -> bool {
  return std::ranges::any_of(*locked_clients, [](auto const &client) {
    return client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC;
  });
}

}  // namespace memgraph::storage
