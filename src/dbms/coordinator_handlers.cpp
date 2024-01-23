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
#include "dbms/replication_client.hpp"

#include "range/v3/view.hpp"

namespace memgraph::dbms {

void CoordinatorHandlers::Register(DbmsHandler &dbms_handler) {
  auto &server = dbms_handler.CoordinatorState().GetCoordinatorServer();

  server.Register<coordination::PromoteReplicaToMainRpc>(
      [&dbms_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received PromoteReplicaToMainRpc");
        CoordinatorHandlers::PromoteReplicaToMainHandler(dbms_handler, req_reader, res_builder);
      });

  server.Register<coordination::SetMainToReplicaRpc>(
      [&dbms_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received PromoteReplicaToMainRpc from coordinator server");
        CoordinatorHandlers::SetMainToReplicaHandler(dbms_handler, req_reader, res_builder);
      });
}

void CoordinatorHandlers::SetMainToReplicaHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                                  slk::Builder *res_builder) {
  auto &repl_state = dbms_handler.ReplicationState();

  if (!repl_state.IsMain()) {
    spdlog::error("Setting to replica must be performed on main.");
    slk::Save(coordination::SetMainToReplicaRes{false}, res_builder);
    return;
  }

  coordination::SetMainToReplicaReq req;
  slk::Load(&req, req_reader);

  replication::ReplicationServerConfig clients_config{.ip_address = req.replication_client_info.replication_ip_address,
                                                      .port = req.replication_client_info.replication_port};

  if (bool success = memgraph::dbms::SetReplicationRoleReplica(dbms_handler, clients_config); !success) {
    spdlog::error("Setting main to replica failed!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
}

void CoordinatorHandlers::PromoteReplicaToMainHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                                      slk::Builder *res_builder) {
  auto &repl_state = dbms_handler.ReplicationState();

  if (!repl_state.IsReplica()) {
    spdlog::error("Failover must be performed on replica!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  if (bool success = memgraph::dbms::DoReplicaToMainPromotion(dbms_handler); !success) {
    spdlog::error("Promoting replica to main failed!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  coordination::PromoteReplicaToMainReq req;
  slk::Load(&req, req_reader);

  auto const converter = [](const auto &repl_info_config) {
    return replication::ReplicationClientConfig{
        .name = repl_info_config.instance_name,
        .mode = repl_info_config.replication_mode,
        .ip_address = repl_info_config.replication_ip_address,
        .port = repl_info_config.replication_port,
    };
  };

  // TODO: ASSERT replicas count ATM 0, we are just become Main

  for (auto const &config : req.replication_clients_info | ranges::views::transform(converter)) {
    auto instance_client = repl_state.RegisterReplica(config);
    if (instance_client.HasError()) {
      switch (instance_client.GetError()) {
        case memgraph::replication::RegisterReplicaError::NOT_MAIN:  // todo lock
          spdlog::error("Failover must be performed to main!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case memgraph::replication::RegisterReplicaError::NAME_EXISTS:  // todo check before we send request
          spdlog::error("Replica with the same name already exists!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case memgraph::replication::RegisterReplicaError::ENDPOINT_EXISTS:  // todo check before we request
          spdlog::error("Replica with the same endpoint already exists!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:  // not a problem disk issue, all bets
                                                                                   // off
          spdlog::error("Registered replica could not be persisted!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case memgraph::replication::RegisterReplicaError::SUCCESS:
          break;
      }
    }

    auto &instance_client_ref = *instance_client.GetValue();
    const bool all_clients_good = memgraph::dbms::RegisterAllDatabasesClients(dbms_handler, instance_client_ref);

    if (!all_clients_good) {
      // go back to replica
      spdlog::error("Failed to register all databases to the REPLICA \"{}\"", config.name);
      slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
      return;
    }

    StartReplicaClient(dbms_handler, instance_client_ref);
  };

  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
}

}  // namespace memgraph::dbms
#endif
