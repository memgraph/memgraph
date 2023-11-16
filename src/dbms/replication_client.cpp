// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "dbms/replication_client.hpp"

namespace memgraph::dbms {

void StartReplicaClient(DbmsHandler &dbms_handler, replication::ReplicationClient &client) {
  // No client error, start instance level client
  auto const &endpoint = client.rpc_client_.Endpoint();
  spdlog::trace("Replication client started at: {}:{}", endpoint.address, endpoint.port);
  client.StartFrequentCheck([&dbms_handler](std::string_view name) {
    // Working connection, check if any database has been left behind
    dbms_handler.ForEach([name](dbms::Database *db) {
      auto *replica_client =
          db->storage()->repl_storage_state_.GetClient(name);  // Specific database <-> replica client
      if (replica_client == nullptr) return;  // Skip as this database does not replicate to this replica
      if (replica_client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
        // Database <-> replica might be behind, check and recover
        replica_client->TryCheckReplicaStateAsync(db->storage());
      }
    });
  });
}

}  // namespace memgraph::dbms
