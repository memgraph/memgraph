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

#include "storage/v2/replication/replication_transaction.hpp"

#include "storage/v2/commit_args.hpp"

namespace memgraph::storage {

// For all replicas, we append transaction end
// When handling STRICT_SYNC replica, we send deltas as part of the 1st phase of the 2PC protocol and wait for the
// response.
// When handling some other type of replica, it is checked whether there is another STRICT_SYNC replica. There are 2
// possible cluster combinations: STRICT_SYNC and ASYNC or SYNC and ASYNC. If there are no STRICT_SYNC replicas in the
// cluster, we send all deltas and commit immediately on replicas.
auto TransactionReplication::ShipDeltas(uint64_t durability_commit_timestamp, CommitArgs const &commit_args)
    -> std::expected<void, ShipDeltasError> {
  if (locked_clients->empty()) return {};

  MG_ASSERT(commit_args.replication_allowed(),
            "Any clients assumes we are MAIN, we should have gatekeeper_access_wrapper so we can correctly "
            "handle ASYNC tasks");

  auto const &db_acc = commit_args.database_protector();
  bool const should_run_2pc = ShouldRunTwoPC();
  std::expected<void, ShipDeltasError> status{};
  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(durability_commit_timestamp); },
                                   replica_stream);
    // NOLINTNEXTLINE
    auto finalized = std::invoke([&]() -> std::expected<void, io::network::ClientCommunicationError> {
      // If I am STRICT SYNC replica, ship deltas as part of the 1st phase and preserve replica stream.
      // NOLINTNEXTLINE
      if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
        // NOLINTNEXTLINE
        return client->FinalizePrepareCommitPhase(replica_stream, durability_commit_timestamp);
      }
      // If there are no STRICT_SYNC replicas, shipping deltas means finalizing the transaction
      // RPC stream gets destroyed => RPC lock released.
      if (!should_run_2pc) {
        // NOLINTNEXTLINE
        auto const res =
            client->FinalizeTransactionReplication(db_acc, std::move(replica_stream), durability_commit_timestamp);
        // Even if fails, we don't care, it's ASYNC
        if (client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
          return {};
        }
        return res;
      }
      // If ASYNC replica which is part of 2PC, just skip this
      // SYNC replica cannot be part of 2PC
      return {};
    });

    if (!finalized.has_value()) {
      if (finalized.error() == io::network::ClientCommunicationError::TIMEOUT_ERROR) {
        // Always collect timeout replica names; timeout takes priority over other errors
        if (!status.has_value()) {
          status.error().timed_out_replicas.emplace_back(client->Name());
        } else {
          status = std::unexpected{
              ShipDeltasError{.error = finalized.error(), .timed_out_replicas = {std::string{client->Name()}}}};
        }
      } else if (status.has_value()) {
        // First non-timeout error, only set if no error yet
        status = std::unexpected{ShipDeltasError{.error = finalized.error(), .timed_out_replicas = {}}};
      }
    }
  }
  return status;
}

// RPC locks will get released at the end of this function for all STRICT_SYNC and ASYNC replicas
// We shouldn't execute this code for SYNC replicas, this is only executed if these replicas are part of STRICT_SYNC
// cluster
auto TransactionReplication::FinalizeTransaction(bool const decision, utils::UUID const &storage_uuid,
                                                 DatabaseProtector const &protector,
                                                 uint64_t const durability_commit_timestamp) -> bool {
  bool strict_sync_replicas_succ{true};

  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
      auto const commit_res =
          client->SendFinalizeCommitRpc(decision, storage_uuid, durability_commit_timestamp, std::move(replica_stream));
      strict_sync_replicas_succ &= commit_res;
    } else if (client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
      if (decision) {
        client->FinalizeTransactionReplication(protector, std::move(replica_stream), durability_commit_timestamp);
      } else if (replica_stream) {
        // Reconnect needed because we optimistically prepared PrepareCommitReq message already.
        // We should only do this if we own the RPC lock.
        client->AbortRpcClient();
      }
    }
  }
  return strict_sync_replicas_succ;
}

namespace {
auto ReplicationModeToString(replication_coordination_glue::ReplicationMode mode) -> std::string {
  switch (mode) {
    case replication_coordination_glue::ReplicationMode::SYNC:
      return "SYNC";
    case replication_coordination_glue::ReplicationMode::ASYNC:
      return "ASYNC";
    case replication_coordination_glue::ReplicationMode::STRICT_SYNC:
      return "STRICT_SYNC";
  }
  return "UNKNOWN";
}
}  // namespace

auto TransactionReplication::CollectStartTxnErrors() const -> std::optional<StartTxnReplicationErrors> {
  StartTxnReplicationErrors result;
  for (auto &&[client, maybe_err] : ranges::views::zip(*locked_clients, errors_)) {
    if (maybe_err.has_value()) {
      result.failures.push_back(
          {.replica_name = client->Name(), .mode = ReplicationModeToString(client->Mode()), .error = *maybe_err});
    }
  }
  if (result.failures.empty()) return std::nullopt;
  return result;
}

TransactionReplication::TransactionReplication(uint64_t const durability_commit_timestamp, Storage *storage,
                                               CommitArgs const &commit_args, ReplicationStorageClientList &clients)
    : locked_clients{clients.ReadLock()} {
  if (!locked_clients->empty()) {
    streams.reserve(locked_clients->size());
    errors_.reserve(locked_clients->size());
    auto const &db_acc = commit_args.database_protector();
    for (const auto &client : *locked_clients) {
      // If any client requires two phase commit, then we are running that phase
      run_two_phase_commit |= client->TwoPhaseCommit();
      auto res = client->StartTransactionReplication(storage, db_acc, durability_commit_timestamp);
      // TODO: (andi) Can you simplify this
      if (res.has_value()) {
        streams.emplace_back(std::move(res.value()));
        errors_.emplace_back(std::nullopt);
      } else {
        streams.emplace_back(std::nullopt);
        errors_.emplace_back(res.error());
      }
    }
  }
}

}  // namespace memgraph::storage
