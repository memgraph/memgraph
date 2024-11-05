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
#include "coordination/data_instance_management_server_handlers.hpp"

#include <range/v3/view.hpp>

#include "coordination/coordinator_rpc.hpp"
#include "coordination/include/coordination/data_instance_management_server.hpp"
#include "replication/state.hpp"

namespace memgraph::dbms {

void DataInstanceManagementServerHandlers::Register(memgraph::coordination::DataInstanceManagementServer &server,
                                                    replication::ReplicationHandler &replication_handler) {
  server.Register<coordination::StateCheckRpc>([&](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
    spdlog::info("Received StateCheckRpc");
    DataInstanceManagementServerHandlers::StateCheckHandler(replication_handler, req_reader, res_builder);
  });

  server.Register<coordination::PromoteReplicaToMainRpc>(
      [&](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received PromoteReplicaToMainRpc");
        DataInstanceManagementServerHandlers::PromoteReplicaToMainHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::DemoteMainToReplicaRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received DemoteMainToReplicaRpc");
        DataInstanceManagementServerHandlers::DemoteMainToReplicaHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received SwapMainUUIDRPC");
        DataInstanceManagementServerHandlers::SwapMainUUIDHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::UnregisterReplicaRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received UnregisterReplicaRpc");
        DataInstanceManagementServerHandlers::UnregisterReplicaHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::EnableWritingOnMainRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received EnableWritingOnMainRpc");
        DataInstanceManagementServerHandlers::EnableWritingOnMainHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::GetInstanceUUIDRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received GetInstanceUUIDRpc");
        DataInstanceManagementServerHandlers::GetInstanceUUIDHandler(replication_handler, req_reader, res_builder);
      });

  server.Register<coordination::GetDatabaseHistoriesRpc>(
      [&replication_handler](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received GetDatabasesHistoryRpc");
        DataInstanceManagementServerHandlers::GetDatabaseHistoriesHandler(replication_handler, req_reader, res_builder);
      });
  server.Register<coordination::RegisterReplicaOnMainRpc>([&replication_handler](slk::Reader *req_reader,
                                                                                 slk::Builder *res_builder) -> void {
    spdlog::info("Received RegisterReplicaOnMainRpc");
    DataInstanceManagementServerHandlers::RegisterReplicaOnMainHandler(replication_handler, req_reader, res_builder);
  });
}

void DataInstanceManagementServerHandlers::StateCheckHandler(replication::ReplicationHandler &replication_handler,
                                                             slk::Reader *req_reader, slk::Builder *res_builder) {
  coordination::StateCheckReq req;
  slk::Load(&req, req_reader);

  bool const is_replica = replication_handler.IsReplica();
  auto const uuid = std::invoke([&replication_handler, is_replica]() -> std::optional<utils::UUID> {
    if (is_replica) {
      return replication_handler.GetReplicaUUID();
    }
    return replication_handler.GetMainUUID();
  });

  auto const writing_enabled = std::invoke([&replication_handler, is_replica]() -> bool {
    if (is_replica) {
      return false;
    }
    return replication_handler.GetReplState().IsMainWriteable();
  });

  slk::Save(coordination::StateCheckRes{is_replica, uuid, writing_enabled}, res_builder);
  spdlog::info("State check returned: is_replica = {}, uuid = {}, writing_enabled = {}", is_replica,
               uuid.has_value() ? std::string{*uuid} : "", writing_enabled);
}

void DataInstanceManagementServerHandlers::GetDatabaseHistoriesHandler(
    replication::ReplicationHandler &replication_handler, slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
  slk::Save(coordination::GetDatabaseHistoriesRes{replication_handler.GetDatabasesHistories()}, res_builder);
  spdlog::info("Database's history returned successfully.");
}

void DataInstanceManagementServerHandlers::SwapMainUUIDHandler(replication::ReplicationHandler &replication_handler,
                                                               slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!replication_handler.IsReplica()) {
    spdlog::error("Setting uuid must be performed on replica.");
    slk::Save(replication_coordination_glue::SwapMainUUIDRes{false}, res_builder);
    return;
  }

  replication_coordination_glue::SwapMainUUIDReq req;
  slk::Load(&req, req_reader);
  std::get<memgraph::replication::RoleReplicaData>(replication_handler.GetReplState().ReplicationData()).uuid_ =
      req.uuid;

  slk::Save(replication_coordination_glue::SwapMainUUIDRes{true}, res_builder);
  spdlog::info("UUID successfully set to {}.", std::string(req.uuid));
}

void DataInstanceManagementServerHandlers::DemoteMainToReplicaHandler(
    replication::ReplicationHandler &replication_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  coordination::DemoteMainToReplicaReq req;
  slk::Load(&req, req_reader);

  // Use localhost as ip for creating ReplicationServer
  const replication::ReplicationServerConfig clients_config{
      .repl_server = io::network::Endpoint("0.0.0.0", req.replication_client_info.replication_server.GetPort())};

  if (!replication_handler.SetReplicationRoleReplica(clients_config, std::nullopt)) {
    spdlog::error("Demoting main to replica failed.");
    slk::Save(coordination::DemoteMainToReplicaRes{false}, res_builder);
    return;
  }

  slk::Save(coordination::DemoteMainToReplicaRes{true}, res_builder);
  spdlog::info("MAIN successfully demoted to REPLICA.");
}

void DataInstanceManagementServerHandlers::GetInstanceUUIDHandler(replication::ReplicationHandler &replication_handler,
                                                                  slk::Reader * /*req_reader*/,
                                                                  slk::Builder *res_builder) {
  if (!replication_handler.IsReplica()) {
    spdlog::trace(
        "Got unexpected request for fetching current main uuid as replica but the instance is not replica at the "
        "moment. Returning empty uuid.");
    slk::Save(coordination::GetInstanceUUIDRes{{}}, res_builder);
    return;
  }

  auto const replica_uuid = replication_handler.GetReplicaUUID();
  slk::Save(coordination::GetInstanceUUIDRes{replica_uuid}, res_builder);
  spdlog::info("Replica's UUID returned successfully: {}.", replica_uuid ? std::string{*replica_uuid} : "");
}

void DataInstanceManagementServerHandlers::PromoteReplicaToMainHandler(
    replication::ReplicationHandler &replication_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  coordination::PromoteReplicaToMainReq req;
  slk::Load(&req, req_reader);

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues. If I receive request to promote myself to main when I am already main
  // I will do it again, the action is idempotent.
  if (const bool success = replication_handler.DoReplicaToMainPromotion(req.main_uuid); !success) {
    spdlog::error("Promoting replica to main failed.");
    slk::Save(coordination::PromoteReplicaToMainRes{false}, res_builder);
    return;
  }

  // registering replicas
  // If disk fails here we are in problem. However, we don't tackle this at the moment since this is so rare event.
  // If I receive request to promote myself while I am already main, I could have some instances already registered.
  // In that case ignore failures.
  for (auto const &config : req.replication_clients_info) {
    if (!DoRegisterReplica<coordination::PromoteReplicaToMainRes>(replication_handler, config, res_builder)) {
      continue;
    }
  }

  replication_handler.GetReplState().GetMainRole().writing_enabled_ = true;

  slk::Save(coordination::PromoteReplicaToMainRes{true}, res_builder);
  spdlog::info("Promoting replica to main finished successfully. New MAIN's uuid: {}", std::string(req.main_uuid));
}

void DataInstanceManagementServerHandlers::RegisterReplicaOnMainHandler(
    replication::ReplicationHandler &replication_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
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
    spdlog::error("Registering replica to main failed because MAIN's uuid {} != from coordinator's uuid {}!",
                  std::string(req.main_uuid), std::string(main_uuid));
    slk::Save(coordination::RegisterReplicaOnMainRes{false}, res_builder);
    return;
  }

  if (!DoRegisterReplica<coordination::RegisterReplicaOnMainRes>(replication_handler, req.replication_client_info,
                                                                 res_builder)) {
    spdlog::error("Replica {} couldn't be registered.", req.replication_client_info.instance_name);
    return;
  }

  slk::Save(coordination::RegisterReplicaOnMainRes{true}, res_builder);
  spdlog::info("Registering replica {} to main finished successfully.", req.replication_client_info.instance_name);
}

void DataInstanceManagementServerHandlers::UnregisterReplicaHandler(
    replication::ReplicationHandler &replication_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
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
    case CANNOT_UNREGISTER:
      spdlog::error("Could not unregister replica.");
      slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
      break;
    case COULD_NOT_BE_PERSISTED:
      spdlog::error("Could not persist replica unregistration.");
      slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
      break;
    case NO_ACCESS:
      spdlog::error("Couldn't get unique access to ReplicationState when unregistering replica.");
      slk::Save(coordination::UnregisterReplicaRes{false}, res_builder);
      break;
  }
  spdlog::info("Replica {} successfully unregistered.", req.instance_name);
}

void DataInstanceManagementServerHandlers::EnableWritingOnMainHandler(
    replication::ReplicationHandler &replication_handler, slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
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
  spdlog::info("Enabled writing on main.");
}

}  // namespace memgraph::dbms
#endif
