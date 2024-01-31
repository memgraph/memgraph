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

#include "coordination/coordinator_exceptions.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/replication_client.hpp"
#include "dbms/utils.hpp"

#include "range/v3/view.hpp"

namespace memgraph::dbms {

void CoordinatorHandlers::Register(DbmsHandler &dbms_handler) {
  auto &server = dbms_handler.CoordinatorState().GetCoordinatorServer();

  server.Register<coordination::PromoteReplicaToMainRpc>(
      [&dbms_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received PromoteReplicaToMainRpc");
        CoordinatorHandlers::PromoteReplicaToMainHandler(dbms_handler, req_reader, res_builder);
      });

  server.Register<coordination::DemoteMainToReplicaRpc>(
      [&dbms_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received DemoteMainToReplicaRpc from coordinator server");
        CoordinatorHandlers::DemoteMainToReplicaHandler(dbms_handler, req_reader, res_builder);
      });
}

void CoordinatorHandlers::DemoteMainToReplicaHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                                     slk::Builder *res_builder) {
  auto &repl_state = dbms_handler.ReplicationState();
  spdlog::info("Executing SetMainToReplicaHandler");

  if (repl_state.IsReplica()) {
    spdlog::error("Setting to replica must be performed on main.");
    slk::Save(coordination::DemoteMainToReplicaRes{false}, res_builder);
    return;
  }

  coordination::DemoteMainToReplicaReq req;
  slk::Load(&req, req_reader);

  const replication::ReplicationServerConfig clients_config{
      .ip_address = req.replication_client_info.replication_ip_address,
      .port = req.replication_client_info.replication_port};

  if (bool const success = memgraph::dbms::SetReplicationRoleReplica(dbms_handler, clients_config); !success) {
    spdlog::error("Demoting main to replica failed!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
}

void CoordinatorHandlers::PromoteReplicaToMainHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                                      slk::Builder *res_builder) {
  auto &repl_state = dbms_handler.ReplicationState();

  if (!repl_state.IsReplica()) {
    spdlog::error("Only replica can be promoted to main!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues.
  if (bool const success = memgraph::dbms::DoReplicaToMainPromotion(dbms_handler); !success) {
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

  MG_ASSERT(
      std::get<replication::RoleMainData>(repl_state.ReplicationData()).registered_replicas_.empty(),
      "No replicas should be registered after promoting replica to main and before registering replication clients!");

  // registering replicas
  for (auto const &config : req.replication_clients_info | ranges::views::transform(converter)) {
    auto instance_client = repl_state.RegisterReplica(config);
    if (instance_client.HasError()) {
      using enum memgraph::replication::RegisterReplicaError;
      switch (instance_client.GetError()) {
        // Can't happen, we are already replica
        case NOT_MAIN:
          spdlog::error("Failover must be performed on main!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        // Can't happen, checked on the coordinator side
        case NAME_EXISTS:
          spdlog::error("Replica with the same name already exists!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        // Can't happen, checked on the coordinator side
        case ENDPOINT_EXISTS:
          spdlog::error("Replica with the same endpoint already exists!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        // We don't handle disk issues
        case COULD_NOT_BE_PERSISTED:
          spdlog::error("Registered replica could not be persisted!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case SUCCESS:
          break;
      }
    }
    if (!allow_mt_repl && dbms_handler.All().size() > 1) {
      spdlog::warn("Multi-tenant replication is currently not supported!");
    }

    auto &instance_client_ref = *instance_client.GetValue();

    // Update system before enabling individual storage <-> replica clients
    dbms_handler.SystemRestore(instance_client_ref);

    const bool all_clients_good = memgraph::dbms::RegisterAllDatabasesClients<true>(dbms_handler, instance_client_ref);
    MG_ASSERT(all_clients_good, "Failed to register one or more databases to the REPLICA \"{}\".", config.name);

    StartReplicaClient(dbms_handler, instance_client_ref);
  }

  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
}

}  // namespace memgraph::dbms
#endif
