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

#include "dbms/inmemory/coordinator_handlers.hpp"

#include "dbms/dbms_handler.hpp"
#include "replication/coordinator_exceptions.hpp"
#include "replication/coordinator_rpc.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::dbms {

void CoordinatorHandlers::Register(DbmsHandler &dbms_handler) {
  using Callable = std::function<void(slk::Reader * req_reader, slk::Builder * res_builder)>;
  auto &server = dbms_handler.CoordinatorState().GetCoordinatorServer();

  server.Register<Callable, replication::FailoverRpc>(
      [&dbms_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received FailoverRpc");
        CoordinatorHandlers::FailoverHandler(dbms_handler, req_reader, res_builder);
      });
}

void CoordinatorHandlers::FailoverHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                          slk::Builder *res_builder) {
  auto &repl_state = dbms_handler.ReplicationState();
  MG_ASSERT(repl_state.IsReplica(), "Failover must be performed on replica!");
  replication::FailoverReq req;
  slk::Load(&req, req_reader);
  spdlog::info("Received {} replicas", req.replication_clients_info.size());
  for (const auto &config : req.replication_clients_info) {
    spdlog::info("Received replica: {}", config.instance_name);
    spdlog::info("Received endpoint: {}", config.replication_ip_address);
    spdlog::info("Received port: {}", config.replication_port);
    spdlog::info("Received mode: {}\n", config.replication_mode);
  }

  bool success = true;
  try {
    // STEP 1) Prepare for new epoch
    dbms_handler.ForEach([](Database *database) {
      auto *storage = database->storage();
      // Remember old epoch + storage timestamp association
      storage->PrepareForNewEpoch();
    });

    // STEP 2) Change to MAIN
    // TODO: restore replication servers if false?
    if (!repl_state.SetReplicationRoleMain()) {
      // TODO: Handle recovery on failure???
      success = false;
    }

    // STEP 3) We are now MAIN, update storage local epoch
    const auto &epoch = std::get<replication::RoleMainData>(std::as_const(repl_state).ReplicationData()).epoch_;
    dbms_handler.ForEach([&epoch](Database *database) {
      auto *storage = database->storage();
      storage->repl_storage_state_.epoch_ = epoch;
    });

    // STEP 4) Convert ReplicationClientInfo to ReplicationClientConfig
    std::vector<replication::ReplicationClientConfig> clients_config;
    clients_config.reserve(req.replication_clients_info.size());
    std::ranges::transform(req.replication_clients_info, std::back_inserter(clients_config),
                           [](const auto &repl_info_config) {
                             return replication::ReplicationClientConfig{
                                 .name = repl_info_config.instance_name,
                                 .mode = repl_info_config.replication_mode,
                                 .ip_address = repl_info_config.replication_ip_address,
                                 .port = repl_info_config.replication_port,
                             };
                           });

    // STEP 5) Register received replicas
    std::ranges::for_each(clients_config, [&dbms_handler, &repl_state](const auto &config) {
      auto instance_client = repl_state.RegisterReplica(config);
      if (instance_client.HasError()) switch (instance_client.GetError()) {
          case memgraph::replication::RegisterReplicaError::NOT_MAIN:
            throw replication::CoordinatorFailoverException("Failover must be performed to main!");
          case memgraph::replication::RegisterReplicaError::NAME_EXISTS:
            throw replication::CoordinatorFailoverException("Replica with the same name already exists!");
          case memgraph::replication::RegisterReplicaError::END_POINT_EXISTS:
            throw replication::CoordinatorFailoverException("Replica with the same endpoint already exists!");
          case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
            throw replication::CoordinatorFailoverException("Registered replica could not be persisted!");
          case memgraph::replication::RegisterReplicaError::SUCCESS:
            break;
        }

      if (!allow_mt_repl && dbms_handler.All().size() > 1) {
        spdlog::warn("Multi-tenant replication is currently not supported!");
      }

      bool all_clients_good = true;

      // Add database specific clients (NOTE Currently all databases are connected to each replica)
      dbms_handler.ForEach([&all_clients_good, &instance_client](Database *database) {
        auto *storage = database->storage();
        if (!allow_mt_repl && storage->id() != kDefaultDB) {
          return;
        }
        // TODO: ATM only IN_MEMORY_TRANSACTIONAL, fix other modes
        if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;

        all_clients_good &= storage->repl_storage_state_.replication_clients_.WithLock(
            [storage, &instance_client](auto &storage_clients) {
              auto client = std::make_unique<storage::ReplicationStorageClient>(*instance_client.GetValue());
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

      // NOTE Currently if any databases fails, we revert back
      if (!all_clients_good) {
        spdlog::error("Failed to register all databases to the REPLICA \"{}\"", config.name);
        // TODO: add a code for unregistering registered replicas
        // UnregisterReplica(config.name);
        throw replication::CoordinatorFailoverException("Failed to register all databases to the REPLICA!");
      }

      // No client error, start instance level client
      StartReplicaClient(dbms_handler, *instance_client.GetValue());
    });

  } catch (replication::CoordinatorFailoverException &e) {
    spdlog::error("Failover failed: {}", e.what());
    success = false;
  }

  replication::FailoverRes res{success};
  slk::Save(res, res_builder);
  spdlog::info("Failover handler finished execution!");
}

}  // namespace memgraph::dbms
#endif
