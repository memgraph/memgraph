// Copyright 2024 Memgraph Ltd.
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
#include "replication/replication_client.hpp"

namespace memgraph::dbms {

void StartReplicaClient(DbmsHandler &dbms_handler, replication::ReplicationClient &client) {
  // No client error, start instance level client
  auto const &endpoint = client.rpc_client_.Endpoint();
  spdlog::trace("Replication client started at: {}:{}", endpoint.address, endpoint.port);
  client.StartFrequentCheck([&client, &dbms_handler](std::string_view name, bool reconnect) {
    // Working connection
    // Check if system needs restoration
    if (reconnect) client.behind_ = true;
#ifdef MG_ENTERPRISE
    dbms_handler.SystemRestore(client);
#endif
    // Check if any database has been left behind
    dbms_handler.ForEach([name, reconnect](dbms::DatabaseAccess db_acc) {
      // Specific database <-> replica client
      db_acc->storage()->repl_storage_state_.WithClient(name, [&](storage::ReplicationStorageClient *client) {
        if (reconnect || client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
          // Database <-> replica might be behind, check and recover
          client->TryCheckReplicaStateAsync(db_acc->storage(), db_acc);
        }
      });
    });
  });
}  // namespace memgraph::dbms

}  // namespace memgraph::dbms
