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
#pragma once

#include "dbms/dbms_handler.hpp"
#include "dbms/replication_handler.hpp"
#include "replication/include/replication/state.hpp"
#include "utils/result.hpp"

namespace memgraph::dbms {

inline bool DoReplicaToMainPromotion(dbms::DbmsHandler &dbms_handler) {
  auto &repl_state = dbms_handler.ReplicationState();
  // STEP 1) bring down all REPLICA servers
  dbms_handler.ForEach([](DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    // Remember old epoch + storage timestamp association
    storage->PrepareForNewEpoch();
  });

  // STEP 2) Change to MAIN
  // TODO: restore replication servers if false?
  if (!repl_state.SetReplicationRoleMain()) {
    // TODO: Handle recovery on failure???
    return false;
  }

  // STEP 3) We are now MAIN, update storage local epoch
  const auto &epoch =
      std::get<replication::RoleMainData>(std::as_const(dbms_handler.ReplicationState()).ReplicationData()).epoch_;
  dbms_handler.ForEach([&](DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    storage->repl_storage_state_.epoch_ = epoch;
  });

  return true;
};

inline bool SetReplicationRoleReplica(dbms::DbmsHandler &dbms_handler,
                                      const memgraph::replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (dbms_handler.ReplicationState().IsReplica()) {
    return false;
  }

  // TODO StorageState needs to be synched. Could have a dangling reference if someone adds a database as we are
  //      deleting the replica.
  // Remove database specific clients

  // dbms_handler.ForEach([&](Database *db) {
  //   auto *storage = db->storage();
  //   storage->repl_storage_state_.replication_clients_.WithLock([](auto &clients) { clients.clear(); });
  // });

  // Remove instance level clients
  // std::get<replication::RoleMainData>(dbms_handler.ReplicationState().ReplicationData()).registered_replicas_.clear();

  // // Creates the server
  // dbms_handler.ReplicationState().SetReplicationRoleReplica(config);

  // // Start
  // const auto success =
  //     std::visit(utils::Overloaded{[](replication::RoleMainData const &) {
  //                                    // ASSERT
  //                                    return false;
  //                                  },
  //                                  [&dbms_handler](replication::RoleReplicaData const &data) {
  //                                    // Register handlers
  //                                    InMemoryReplicationHandlers::Register(&dbms_handler, *data.server);
  //                                    if (!data.server->Start()) {
  //                                      spdlog::error("Unable to start the replication server.");
  //                                      return false;
  //                                    }
  //                                    return true;
  //                                  }},
  //                dbms_handler.ReplicationState().ReplicationData());
  // TODO Handle error (restore to main?)
  return true;
}

inline bool RegisterAllDatabasesClients(dbms::DbmsHandler &dbms_handler,
                                        replication::ReplicationClient &instance_client) {
  if (!allow_mt_repl && dbms_handler.All().size() > 1) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }

  bool all_clients_good = true;

  // Add database specific clients (NOTE Currently all databases are connected to each replica)
  dbms_handler.ForEach([&](DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    if (!allow_mt_repl && storage->name() != kDefaultDB) {
      return;
    }
    // TODO: ATM only IN_MEMORY_TRANSACTIONAL, fix other modes
    if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;

    all_clients_good &= storage->repl_storage_state_.replication_clients_.WithLock(
        [storage, &instance_client, db_acc = std::move(db_acc)](auto &storage_clients) mutable {  // NOLINT
          auto client = std::make_unique<storage::ReplicationStorageClient>(instance_client);
          // All good, start replica client
          client->Start(storage, std::move(db_acc));
          // After start the storage <-> replica state should be READY or RECOVERING (if correctly started)
          // MAYBE_BEHIND isn't a statement of the current state, this is the default value
          // Failed to start due an error like branching of MAIN and REPLICA
          if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
            return false;  // TODO: sometimes we need to still add to storage_clients
          }
          storage_clients.push_back(std::move(client));
          return true;
        });
  });

  return all_clients_good;
}

inline std::optional<RegisterReplicaError> HandleRegisterReplicaStatus(
    utils::BasicResult<replication::RegisterReplicaError, replication::ReplicationClient *> &instance_client) {
  if (instance_client.HasError()) switch (instance_client.GetError()) {
      case replication::RegisterReplicaError::NOT_MAIN:
        MG_ASSERT(false, "Only main instance can register a replica!");
        return {};
      case replication::RegisterReplicaError::NAME_EXISTS:
        return dbms::RegisterReplicaError::NAME_EXISTS;
      case replication::RegisterReplicaError::ENDPOINT_EXISTS:
        return dbms::RegisterReplicaError::ENDPOINT_EXISTS;
      case replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
        return dbms::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      case replication::RegisterReplicaError::SUCCESS:
        break;
    }
  return {};
}

}  // namespace memgraph::dbms
