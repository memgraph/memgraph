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
#include "coordination/coordinator_handlers.hpp"

#include <range/v3/view.hpp>

#include "coordination/coordinator_rpc.hpp"
#include "coordination/include/coordination/coordinator_server.hpp"
#include "replication/state.hpp"

namespace memgraph::dbms {

void CoordinatorHandlers::Register(memgraph::coordination::CoordinatorServer &server,
                                   replication::ReplicationHandler &replication_handler) {
  server.Register<coordination::PromoteReplicaToMainRpc>(
      [&](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received PromoteReplicaToMainRpc");
        CoordinatorHandlers::PromoteReplicaToMainHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::DemoteMainToReplicaRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received DemoteMainToReplicaRpc from coordinator server");
        CoordinatorHandlers::DemoteMainToReplicaHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received SwapMainUUIDRPC on coordinator server");
        CoordinatorHandlers::SwapMainUUIDHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::GetInstanceUUIDRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received GetInstanceUUIDRpc on coordinator server");
        CoordinatorHandlers::GetInstanceUUIDHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::GetInstanceTimestampsRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received GetInstanceTimestampsRpc on coordinator server");
        CoordinatorHandlers::GetInstanceTimestampsHandler(replication_handler, req_reader, res_builder);
      });
}

void CoordinatorHandlers::GetInstanceTimestampsHandler(replication::ReplicationHandler &replication_handler,
                                                       slk::Reader *req_reader, slk::Builder *res_builder) {
  coordination::GetInstanceTimestampsReq req;
  slk::Load(&req, req_reader);

  slk::Save(coordination::GetInstanceTimestampsRes{replication_handler.GetTimestampsForEachDb()}, res_builder);
}

void CoordinatorHandlers::SwapMainUUIDHandler(replication::ReplicationHandler &replication_handler,
                                              slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!replication_handler.IsReplica()) {
    spdlog::error("Setting main uuid must be performed on replica.");
    slk::Save(replication_coordination_glue::SwapMainUUIDRes{false}, res_builder);
    return;
  }

  replication_coordination_glue::SwapMainUUIDReq req;
  slk::Load(&req, req_reader);
  spdlog::info(fmt::format("Set replica data UUID  to main uuid {}", std::string(req.uuid)));
  std::get<memgraph::replication::RoleReplicaData>(replication_handler.GetReplState().ReplicationData()).uuid_ =
      req.uuid;

  slk::Save(replication_coordination_glue::SwapMainUUIDRes{true}, res_builder);
}

void CoordinatorHandlers::DemoteMainToReplicaHandler(replication::ReplicationHandler &replication_handler,
                                                     slk::Reader *req_reader, slk::Builder *res_builder) {
  spdlog::info("Executing DemoteMainToReplicaHandler");

  coordination::DemoteMainToReplicaReq req;
  slk::Load(&req, req_reader);

  const replication::ReplicationServerConfig clients_config{
      .ip_address = req.replication_client_info.replication_ip_address,
      .port = req.replication_client_info.replication_port};

  if (!replication_handler.SetReplicationRoleReplica(clients_config, std::nullopt)) {
    spdlog::error("Demoting main to replica failed!");
    slk::Save(coordination::DemoteMainToReplicaRes{false}, res_builder);
    return;
  }

  slk::Save(coordination::DemoteMainToReplicaRes{true}, res_builder);
}

void CoordinatorHandlers::GetInstanceUUIDHandler(replication::ReplicationHandler &replication_handler,
                                                 slk::Reader *req_reader, slk::Builder *res_builder) {
  spdlog::info("Executing GetInstanceUUIDHandler");

  coordination::GetInstanceUUIDReq req;
  slk::Load(&req, req_reader);

  slk::Save(coordination::GetInstanceUUIDRes{replication_handler.GetReplicaUUID()}, res_builder);
}

void CoordinatorHandlers::PromoteReplicaToMainHandler(replication::ReplicationHandler &replication_handler,
                                                      slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!replication_handler.IsReplica()) {
    spdlog::error("Failover must be performed on replica!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }
  coordination::PromoteReplicaToMainReq req;
  slk::Load(&req, req_reader);

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues.
  if (const bool success = replication_handler.DoReplicaToMainPromotion(req.main_uuid_); !success) {
    spdlog::error("Promoting replica to main failed!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  auto const converter = [](const auto &repl_info_config) {
    return replication::ReplicationClientConfig{
        .name = repl_info_config.instance_name,
        .mode = repl_info_config.replication_mode,
        .ip_address = repl_info_config.replication_ip_address,
        .port = repl_info_config.replication_port,
    };
  };

  // registering replicas
  for (auto const &config : req.replication_clients_info | ranges::views::transform(converter)) {
    auto instance_client = replication_handler.RegisterReplica(config, false);
    if (instance_client.HasError()) {
      using enum memgraph::replication::RegisterReplicaError;
      switch (instance_client.GetError()) {
        // Can't happen, checked on the coordinator side
        case memgraph::query::RegisterReplicaError::NAME_EXISTS:
          spdlog::error("Replica with the same name already exists!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        // Can't happen, checked on the coordinator side
        case memgraph::query::RegisterReplicaError::ENDPOINT_EXISTS:
          spdlog::error("Replica with the same endpoint already exists!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        // We don't handle disk issues
        case memgraph::query::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
          spdlog::error("Registered replica could not be persisted!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case memgraph::query::RegisterReplicaError::ERROR_ACCEPTING_MAIN:
          spdlog::error("Replica didn't accept change of main!");
          slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
          return;
        case memgraph::query::RegisterReplicaError::CONNECTION_FAILED:
          // Connection failure is not a fatal error
          break;
      }
    }
  }
  spdlog::error(fmt::format("FICO : Promote replica to main was success {}", std::string(req.main_uuid_)));
  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
}

}  // namespace memgraph::dbms
#endif
