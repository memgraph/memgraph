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

  server.Register<coordination::UnregisterReplicaRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received UnregisterReplicaRpc on coordinator server");
        CoordinatorHandlers::UnregisterReplicaHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::EnableWritingOnMainRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received EnableWritingOnMainRpc on coordinator server");
        CoordinatorHandlers::EnableWritingOnMainHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::GetInstanceUUIDRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received GetInstanceUUIDRpc on coordinator server");
        CoordinatorHandlers::GetInstanceUUIDHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::GetDatabaseHistoriesRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received GetDatabasesHistoryRpc on coordinator server");
        CoordinatorHandlers::GetDatabaseHistoriesHandler(replication_handler, req_reader, res_builder);
      });
  server.Register<coordination::RegisterReplicaOnMainRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received RegisterReplicaOnMainRpc on coordinator server");
        CoordinatorHandlers::RegisterReplicaOnMainHandler(replication_handler, req_reader, res_builder);
      });
}

void CoordinatorHandlers::GetDatabaseHistoriesHandler(replication::ReplicationHandler &replication_handler,
                                                      slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
  slk::Save(coordination::GetDatabaseHistoriesRes{replication_handler.GetDatabasesHistories()}, res_builder);
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

  // Use localhost as ip for creating ReplicationServer
  const replication::ReplicationServerConfig clients_config{
      .ip_address = "0.0.0.0", .port = req.replication_client_info.replication_server.GetPort()};

  if (!replication_handler.SetReplicationRoleReplica(clients_config, std::nullopt)) {
    spdlog::error("Demoting main to replica failed!");
    slk::Save(coordination::DemoteMainToReplicaRes{false}, res_builder);
    return;
  }

  slk::Save(coordination::DemoteMainToReplicaRes{true}, res_builder);
}

void CoordinatorHandlers::GetInstanceUUIDHandler(replication::ReplicationHandler &replication_handler,
                                                 slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
  spdlog::info("Executing GetInstanceUUIDHandler");

  slk::Save(coordination::GetInstanceUUIDRes{replication_handler.GetReplicaUUID()}, res_builder);
}

void CoordinatorHandlers::PromoteReplicaToMainHandler(replication::ReplicationHandler &replication_handler,
                                                      slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!replication_handler.IsReplica()) {
    spdlog::error("Promote to main must be performed on replica!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }
  coordination::PromoteReplicaToMainReq req;
  slk::Load(&req, req_reader);

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues.
  if (const bool success = replication_handler.DoReplicaToMainPromotion(req.main_uuid); !success) {
    spdlog::error("Promoting replica to main failed!");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  // registering replicas
  for (auto const &config : req.replication_clients_info) {
    if (!DoRegisterReplica<coordination::PromoteReplicaToMainRes>(replication_handler, config, res_builder)) {
      return;
    }
  }
  spdlog::info("Promote replica to main was success {}", std::string(req.main_uuid));
  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
}

void CoordinatorHandlers::RegisterReplicaOnMainHandler(replication::ReplicationHandler &replication_handler,
                                                       slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!replication_handler.IsMain()) {
    spdlog::error("Registering replica on main must be performed on main!");
    slk::Save(coordination::RegisterReplicaOnMainRes{false}, res_builder);
    return;
  }
  coordination::RegisterReplicaOnMainReq req;
  slk::Load(&req, req_reader);

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues.
  auto const &main_uuid = replication_handler.GetReplState().GetMainRole().uuid_;
  if (req.main_uuid != main_uuid) {
    spdlog::error("Registering replica to main failed because MAIN has uuid {} and coordinator has sent uuid {}!",
                  std::string(req.main_uuid), std::string(main_uuid));
    slk::Save(coordination::RegisterReplicaOnMainRes{false}, res_builder);
    return;
  }

  if (!DoRegisterReplica<coordination::RegisterReplicaOnMainRes>(replication_handler, req.replication_client_info,
                                                                 res_builder)) {
    return;
  }

  spdlog::info("Registering replica {} to main was success ", req.replication_client_info.instance_name);
  slk::Save(coordination::RegisterReplicaOnMainRes{true}, res_builder);
}

void CoordinatorHandlers::UnregisterReplicaHandler(replication::ReplicationHandler &replication_handler,
                                                   slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!replication_handler.IsMain()) {
    spdlog::error("Unregistering replica must be performed on main.");
    slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
    return;
  }

  coordination::UnregisterReplicaReq req;
  slk::Load(&req, req_reader);

  auto res = replication_handler.UnregisterReplica(req.instance_name);
  switch (res) {
    using enum memgraph::query::UnregisterReplicaResult;
    case SUCCESS:
      slk::Save(coordination::UnregisterReplicaRes{true}, res_builder);
      break;
    case NOT_MAIN:
      spdlog::error("Unregistering replica must be performed on main.");
      slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
      break;
    case CAN_NOT_UNREGISTER:
      spdlog::error("Could not unregister replica.");
      slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
      break;
    case COULD_NOT_BE_PERSISTED:
      spdlog::error("Could not persist replica unregistration.");
      slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
      break;
  }
}

void CoordinatorHandlers::EnableWritingOnMainHandler(replication::ReplicationHandler &replication_handler,
                                                     slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
  if (!replication_handler.IsMain()) {
    spdlog::error("Enable writing on main must be performed on main!");
    slk::Save(coordination::EnableWritingOnMainRes{false}, res_builder);
    return;
  }

  if (!replication_handler.GetReplState().EnableWritingOnMain()) {
    spdlog::error("Enabling writing on main failed!");
    slk::Save(coordination::EnableWritingOnMainRes{false}, res_builder);
    return;
  }

  slk::Save(coordination::EnableWritingOnMainRes{true}, res_builder);
}

}  // namespace memgraph::dbms
#endif
