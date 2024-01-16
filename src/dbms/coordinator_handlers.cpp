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

#ifdef MG_ENTERPRISE

#include "dbms/coordinator_handlers.hpp"
#include "dbms/utils.hpp"

#include "coordination/coordinator_exceptions.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "dbms/dbms_handler.hpp"

namespace memgraph::dbms {

void CoordinatorHandlers::Register(DbmsHandler &dbms_handler) {
  auto &server = dbms_handler.CoordinatorState().GetCoordinatorServer();

  server.Register<coordination::FailoverRpc>(
      [&dbms_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received FailoverRpc from coordinator server");
        CoordinatorHandlers::FailoverHandler(dbms_handler, req_reader, res_builder);
      });
}

void CoordinatorHandlers::FailoverHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                          slk::Builder *res_builder) {
  auto &repl_state = dbms_handler.ReplicationState();

  if (repl_state.IsReplica()) {
    spdlog::error("Failover must be performed on replica!");
    slk::Save(coordination::FailoverRes{false}, res_builder);
    return;
  }

  coordination::FailoverReq req;
  slk::Load(&req, req_reader);

  bool success = true;
  try {
    success = memgraph::dbms::DoReplicaToMainPromotion(dbms_handler);

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
            throw coordination::CoordinatorFailoverException("Failover must be performed to main!");
          case memgraph::replication::RegisterReplicaError::NAME_EXISTS:
            throw coordination::CoordinatorFailoverException("Replica with the same name already exists!");
          case memgraph::replication::RegisterReplicaError::END_POINT_EXISTS:
            throw coordination::CoordinatorFailoverException("Replica with the same endpoint already exists!");
          case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
            throw coordination::CoordinatorFailoverException("Registered replica could not be persisted!");
          case memgraph::replication::RegisterReplicaError::SUCCESS:
            break;
        }

      auto &instance_client_ptr = instance_client.GetValue();
      const bool all_clients_good = memgraph::dbms::RegisterAllDatabasesClients(dbms_handler, *instance_client_ptr);

      if (!all_clients_good) {
        spdlog::error("Failed to register all databases to the REPLICA \"{}\"", config.name);
        throw coordination::CoordinatorFailoverException("Failed to register all databases to the REPLICA!");
      }

      StartReplicaClient(dbms_handler, *instance_client.GetValue());
    });

  } catch (coordination::CoordinatorFailoverException &e) {
    spdlog::error("Failover failed: {}", e.what());
    success = false;
  }

  coordination::FailoverRes res{success};
  slk::Save(res, res_builder);
  spdlog::info("Failover handler finished execution!");
}

}  // namespace memgraph::dbms
#endif
