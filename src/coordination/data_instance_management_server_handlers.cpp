// Copyright 2025 Memgraph Ltd.
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

#include "coordination/coordinator_rpc.hpp"
#include "coordination/include/coordination/data_instance_management_server.hpp"
#include "replication/state.hpp"

#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

namespace memgraph::dbms {

void DataInstanceManagementServerHandlers::Register(memgraph::coordination::DataInstanceManagementServer &server,
                                                    replication::ReplicationHandler &replication_handler) {
  server.Register<coordination::StateCheckRpc>(
      [&](uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        StateCheckHandler(replication_handler, request_version, req_reader, res_builder);
      });

  server.Register<coordination::PromoteToMainRpc>(
      [&](uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        PromoteToMainHandler(replication_handler, request_version, req_reader, res_builder);
      });

  server.Register<coordination::DemoteMainToReplicaRpc>([&replication_handler](uint64_t const request_version,
                                                                               slk::Reader *req_reader,
                                                                               slk::Builder *res_builder) -> void {
    DemoteMainToReplicaHandler(replication_handler, request_version, req_reader, res_builder);
  });

  server.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&replication_handler](uint64_t const request_version, slk::Reader *req_reader,
                             slk::Builder *res_builder) -> void {
        SwapMainUUIDHandler(replication_handler, request_version, req_reader, res_builder);
      });

  server.Register<coordination::UnregisterReplicaRpc>([&replication_handler](uint64_t const request_version,
                                                                             slk::Reader *req_reader,
                                                                             slk::Builder *res_builder) -> void {
    UnregisterReplicaHandler(replication_handler, request_version, req_reader, res_builder);
  });

  server.Register<coordination::ReplicationLagRpc>(
      [&replication_handler](uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        GetReplicationLagHandler(replication_handler, request_version, req_reader, res_builder);
      });
  server.Register<coordination::EnableWritingOnMainRpc>([&replication_handler](uint64_t const request_version,
                                                                               slk::Reader *req_reader,
                                                                               slk::Builder *res_builder) -> void {
    EnableWritingOnMainHandler(replication_handler, request_version, req_reader, res_builder);
  });

  server.Register<coordination::GetDatabaseHistoriesRpc>([&replication_handler](uint64_t const request_version,
                                                                                slk::Reader *req_reader,
                                                                                slk::Builder *res_builder) -> void {
    GetDatabaseHistoriesHandler(replication_handler, request_version, req_reader, res_builder);
  });
  server.Register<coordination::RegisterReplicaOnMainRpc>([&replication_handler](uint64_t const request_version,
                                                                                 slk::Reader *req_reader,
                                                                                 slk::Builder *res_builder) -> void {
    RegisterReplicaOnMainHandler(replication_handler, request_version, req_reader, res_builder);
  });
}

void DataInstanceManagementServerHandlers::StateCheckHandler(const replication::ReplicationHandler &replication_handler,
                                                             uint64_t const request_version, slk::Reader *req_reader,
                                                             slk::Builder *res_builder) {
  coordination::StateCheckReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  const auto locked_repl_state = replication_handler.GetReplState();

  bool const is_replica = locked_repl_state->IsReplica();
  auto const uuid = std::invoke([&locked_repl_state, is_replica]() -> std::optional<utils::UUID> {
    if (is_replica) {
      return locked_repl_state->GetReplicaRole().uuid_;
    }
    return locked_repl_state->GetMainRole().uuid_;
  });

  auto const writing_enabled = std::invoke([&locked_repl_state, is_replica]() -> bool {
    if (is_replica) {
      return false;
    }
    return locked_repl_state->IsMainWriteable();
  });

  coordination::StateCheckRes const rpc_res{is_replica, uuid, writing_enabled};
  rpc::SendFinalResponse(rpc_res, res_builder,
                         fmt::format("is_replica = {}, uuid = {}, writing_enabled = {}", is_replica,
                                     uuid.has_value() ? std::string{*uuid} : "", writing_enabled));
}

void DataInstanceManagementServerHandlers::GetDatabaseHistoriesHandler(
    replication::ReplicationHandler const &replication_handler, uint64_t const request_version,
    slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
  coordination::GetDatabaseHistoriesRes const rpc_res{replication_handler.GetDatabasesHistories()};
  rpc::SendFinalResponse(rpc_res, res_builder);
}

void DataInstanceManagementServerHandlers::GetReplicationLagHandler(
    replication::ReplicationHandler const &replication_handler, uint64_t const /*request_version*/, slk::Reader * /*req_reader*/,
    slk::Builder *res_builder) {
  auto locked_repl_state = replication_handler.GetReplState();
  if (locked_repl_state->IsReplica()) {
    spdlog::error("Replication lag can only be retrieved from the main instance");
    coordination::ReplicationLagRes const res{std::nullopt};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  auto repl_lag_info = replication_handler.GetReplicationLag();
  coordination::ReplicationLagRes const res{std::move(repl_lag_info)};
  rpc::SendFinalResponse(res, res_builder);
}

auto DataInstanceManagementServerHandlers::DoRegisterReplica(replication::ReplicationHandler &replication_handler,
                                                             coordination::ReplicationClientInfo const &config)
    -> bool {
  auto const converter = [&config](const auto &repl_info_config) {
    return replication::ReplicationClientConfig{.name = repl_info_config.instance_name,
                                                .mode = repl_info_config.replication_mode,
                                                .repl_server_endpoint = config.replication_server};
  };

  if (auto instance_client = replication_handler.RegisterReplica(converter(config)); instance_client.HasError()) {
    using query::RegisterReplicaError;
    switch (instance_client.GetError()) {
      case RegisterReplicaError::NO_ACCESS: {
        spdlog::error("Error when registering instance {} as replica. Couldn't get unique access to ReplicationState.");
        return false;
      }
      case RegisterReplicaError::NOT_MAIN: {
        spdlog::error("Error when registering instance {} as replica. Instance not main anymore.",
                      config.instance_name);
        return false;
      }
      case RegisterReplicaError::NAME_EXISTS: {
        spdlog::error("Error when registering instance {} as replica. Instance with the same name already registered.",
                      config.instance_name);
        return false;
      }
      case RegisterReplicaError::ENDPOINT_EXISTS: {
        spdlog::error("Error when registering instance {} as replica. Instance with the same endpoint already exists.",
                      config.instance_name);
        return false;
      }
      case RegisterReplicaError::COULD_NOT_BE_PERSISTED: {
        spdlog::error("Error when registering instance {} as replica. Registering instance could not be persisted.",
                      config.instance_name);
        return false;
      }
      case RegisterReplicaError::ERROR_ACCEPTING_MAIN: {
        spdlog::error("Error when registering instance {} as replica. Instance couldn't accept change of main.",
                      config.instance_name);
        return false;
      }
      case RegisterReplicaError::CONNECTION_FAILED: {
        spdlog::error(
            "Error when registering instance {} as replica. Instance couldn't register all databases successfully.",
            config.instance_name);
        return false;
      }
      default: {
        LOG_FATAL("Error in handling RegisterReplicaError. Unknown enum value.");
      }
    }
  }
  spdlog::trace("Instance {} successfully registered as replica.", config.instance_name);
  return true;
}

void DataInstanceManagementServerHandlers::SwapMainUUIDHandler(replication::ReplicationHandler &replication_handler,
                                                               uint64_t const request_version, slk::Reader *req_reader,
                                                               slk::Builder *res_builder) {
  replication_coordination_glue::SwapMainUUIDReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  auto locked_repl_state = replication_handler.GetReplState();

  if (!locked_repl_state->IsReplica()) {
    spdlog::error("Setting uuid must be performed on replica.");
    replication_coordination_glue::SwapMainUUIDRes const rpc_res{false};
    rpc::SendFinalResponse(rpc_res, res_builder);
    return;
  }

  auto &repl_data = std::get<replication::RoleReplicaData>(locked_repl_state->ReplicationData());
  spdlog::info("Set replica data UUID to main uuid {}", std::string(req.uuid));
  locked_repl_state->TryPersistRoleReplica(repl_data.config, req.uuid);
  repl_data.uuid_ = req.uuid;

  replication_coordination_glue::SwapMainUUIDRes const rpc_res{true};
  rpc::SendFinalResponse(rpc_res, res_builder);
  spdlog::info("UUID successfully set to {}.", std::string(req.uuid));
}

void DataInstanceManagementServerHandlers::DemoteMainToReplicaHandler(
    replication::ReplicationHandler &replication_handler, uint64_t const request_version, slk::Reader *req_reader,
    slk::Builder *res_builder) {
  coordination::DemoteMainToReplicaReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  // Use localhost as ip for creating ReplicationServer
  const replication::ReplicationServerConfig clients_config{
      .repl_server = io::network::Endpoint("0.0.0.0", req.replication_client_info.replication_server.GetPort())};

  if (!replication_handler.SetReplicationRoleReplica(clients_config)) {
    spdlog::error("Demoting main to replica failed.");
    coordination::DemoteMainToReplicaRes const rpc_res{false};
    rpc::SendFinalResponse(rpc_res, res_builder);
    return;
  }

  coordination::DemoteMainToReplicaRes const rpc_res{true};
  rpc::SendFinalResponse(rpc_res, res_builder);
  spdlog::info("MAIN successfully demoted to REPLICA.");
}

void DataInstanceManagementServerHandlers::PromoteToMainHandler(replication::ReplicationHandler &replication_handler,
                                                                uint64_t const request_version, slk::Reader *req_reader,
                                                                slk::Builder *res_builder) {
  coordination::PromoteToMainReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  // Main promotion, replica registration and main write enabling should all be performed
  // under the same lock. Currently, we take and release lock for all operations.
  // Since this can happen only if coordinator is present AND RPC is single threaded, this is fine
  // for now.

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues. If I receive request to promote myself to main when I am already main
  // I will do it again, the action is idempotent.
  if (const bool success = replication_handler.DoToMainPromotion(req.main_uuid); !success) {
    spdlog::error("Promoting replica to main failed.");
    coordination::PromoteToMainRes const res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  // If registering one of replicas fails, we signal to the coordinator that we failed. The writing is disabled on main
  // so no problem. The coordinator should retry its request until registering all replicas succeeds.
  for (auto const &config : req.replication_clients_info) {
    if (!DoRegisterReplica(replication_handler, config)) {
      coordination::PromoteToMainRes const res{false};
      rpc::SendFinalResponse(res, res_builder);
      return;
    }
  }

  replication_handler.GetReplState()->GetMainRole().writing_enabled_ = true;

  coordination::PromoteToMainRes const res{true};
  rpc::SendFinalResponse(res, res_builder);
  spdlog::info("Promoting replica to main finished successfully. New MAIN's uuid: {}", std::string(req.main_uuid));
}

void DataInstanceManagementServerHandlers::RegisterReplicaOnMainHandler(
    replication::ReplicationHandler &replication_handler, uint64_t const request_version, slk::Reader *req_reader,
    slk::Builder *res_builder) {
  // TODO: Fix potential datarace. Main check, uuid check and replica registration should all be performed under the
  // same lock. Since this can happen only if coordinator is present AND RPC is single threaded, this is fine for now.
  if (!replication_handler.IsMain()) {
    spdlog::error("Registering replica on main must be performed on main!");
    coordination::RegisterReplicaOnMainRes const res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  coordination::RegisterReplicaOnMainReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  // This can fail because of disk. If it does, the cluster state could get inconsistent.
  // We don't handle disk issues.
  auto const &main_uuid = replication_handler.GetReplState()->GetMainRole().uuid_;
  if (req.main_uuid != main_uuid) {
    spdlog::error("Registering replica to main failed because MAIN's uuid {} != from coordinator's uuid {}!",
                  std::string(req.main_uuid), std::string(main_uuid));
    coordination::RegisterReplicaOnMainRes const res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  if (!DoRegisterReplica(replication_handler, req.replication_client_info)) {
    spdlog::error("Replica {} couldn't be registered.", req.replication_client_info.instance_name);
    coordination::RegisterReplicaOnMainRes const res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  coordination::RegisterReplicaOnMainRes const res{true};
  rpc::SendFinalResponse(res, res_builder);
  spdlog::info("Registering replica {} to main finished successfully.", req.replication_client_info.instance_name);
}

void DataInstanceManagementServerHandlers::UnregisterReplicaHandler(
    replication::ReplicationHandler &replication_handler, uint64_t const request_version, slk::Reader *req_reader,
    slk::Builder *res_builder) {
  coordination::UnregisterReplicaReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  switch (replication_handler.UnregisterReplica(req.instance_name)) {
    using enum query::UnregisterReplicaResult;
    case SUCCESS: {
      coordination::UnregisterReplicaRes const rpc_res{true};
      rpc::SendFinalResponse(rpc_res, res_builder);
      break;
    }
    case NOT_MAIN: {
      spdlog::error("Unregistering replica must be performed on main.");
      coordination::UnregisterReplicaRes const rpc_res{false};
      rpc::SendFinalResponse(rpc_res, res_builder);
      break;
    }
    case CANNOT_UNREGISTER: {
      spdlog::error("Could not unregister replica.");
      coordination::UnregisterReplicaRes const rpc_res{false};
      rpc::SendFinalResponse(rpc_res, res_builder);
      break;
    }
    case COULD_NOT_BE_PERSISTED: {
      spdlog::error("Could not persist replica unregistration.");
      coordination::UnregisterReplicaRes const rpc_res{false};
      rpc::SendFinalResponse(rpc_res, res_builder);
      break;
    }
    case NO_ACCESS: {
      spdlog::error("Couldn't get unique access to ReplicationState when unregistering replica.");
      coordination::UnregisterReplicaRes const rpc_res{false};
      rpc::SendFinalResponse(rpc_res, res_builder);
      break;
    }
  }
  spdlog::info("Replica {} successfully unregistered.", req.instance_name);
}

void DataInstanceManagementServerHandlers::EnableWritingOnMainHandler(
    replication::ReplicationHandler &replication_handler, uint64_t const /*request_version*/,
    slk::Reader * /*req_reader*/, slk::Builder *res_builder) {
  auto locked_repl_state = replication_handler.GetReplState();

  if (!locked_repl_state->IsMain()) {
    spdlog::error("Enable writing on main must be performed on main!");
    coordination::EnableWritingOnMainRes const rpc_res{false};
    rpc::SendFinalResponse(rpc_res, res_builder);
    return;
  }

  if (!locked_repl_state->EnableWritingOnMain()) {
    spdlog::error("Enabling writing on main failed!");
    coordination::EnableWritingOnMainRes const rpc_res{false};
    rpc::SendFinalResponse(rpc_res, res_builder);
    return;
  }

  coordination::EnableWritingOnMainRes const rpc_res{true};
  rpc::SendFinalResponse(rpc_res, res_builder);
  spdlog::info("Enabled writing on main.");
}

}  // namespace memgraph::dbms
#endif
