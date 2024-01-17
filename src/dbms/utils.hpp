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
  // STEP 1) Prepare for new epoch
  dbms_handler.ForEach([](Database *database) {
    auto *storage = database->storage();
    // Remember old epoch + storage timestamp association
    storage->PrepareForNewEpoch();
  });

  // STEP 2) Change to MAIN = Kill replication server
  if (!repl_state.SetReplicationRoleMain()) {
    return false;
  }

  // STEP 3) We are now MAIN, update storage local epoch
  const auto &epoch = std::get<replication::RoleMainData>(std::as_const(repl_state).ReplicationData()).epoch_;
  dbms_handler.ForEach([&epoch](Database *database) {
    auto *storage = database->storage();
    storage->repl_storage_state_.epoch_ = epoch;
  });
  return true;
};

inline bool RegisterAllDatabasesClients(dbms::DbmsHandler &dbms_handler,
                                        replication::ReplicationClient &instance_client) {
  if (!allow_mt_repl && dbms_handler.All().size() > 1) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }

  bool all_clients_good = true;

  // Add database specific clients (NOTE Currently all databases are connected to each replica)
  dbms_handler.ForEach([&](Database *db) {
    auto *storage = db->storage();
    if (!allow_mt_repl && storage->id() != kDefaultDB) {
      return;
    }
    // TODO: ATM only IN_MEMORY_TRANSACTIONAL, fix other modes
    if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;

    all_clients_good &=
        storage->repl_storage_state_.replication_clients_.WithLock([storage, &instance_client](auto &storage_clients) {
          auto client = std::make_unique<storage::ReplicationStorageClient>(instance_client);
          client->Start(storage);
          // After start the storage <-> replica state should be READY or RECOVERING (if correctly started)
          // MAYBE_BEHIND isn't a statement of the current state, this is the default value
          // Failed to start due to branching of MAIN and REPLICA
          if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
            return false;
          }
          storage_clients.push_back(std::move(client));
          return true;
        });
  });

  return all_clients_good;
}

inline std::optional<RegisterReplicaError> HandleErrorOnReplicaClient(
    utils::BasicResult<replication::RegisterReplicaError, replication::ReplicationClient *> &instance_client) {
  if (instance_client.HasError()) switch (instance_client.GetError()) {
      case replication::RegisterReplicaError::NOT_MAIN:
        MG_ASSERT(false, "Only main instance can register a replica!");
        return {};
      case replication::RegisterReplicaError::NAME_EXISTS:
        return dbms::RegisterReplicaError::NAME_EXISTS;
      case replication::RegisterReplicaError::END_POINT_EXISTS:
        return dbms::RegisterReplicaError::END_POINT_EXISTS;
      case replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
        return dbms::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      case replication::RegisterReplicaError::SUCCESS:
        break;
    }
  return {};
}

}  // namespace memgraph::dbms
