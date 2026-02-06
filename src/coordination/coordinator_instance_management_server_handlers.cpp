// Copyright 2026 Memgraph Ltd.
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

#include "coordination/coordinator_instance_management_server_handlers.hpp"
#include "coordination/coordinator_rpc.hpp"

#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

namespace memgraph::rpc {
class FileReplicationHandler;
}  // namespace memgraph::rpc

namespace memgraph::coordination {

void CoordinatorInstanceManagementServerHandlers::Register(CoordinatorInstanceManagementServer &server,
                                                           CoordinatorInstance &coordinator_instance) {
  server.Register<AddCoordinatorRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::AddCoordinatorHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });

  server.Register<RemoveCoordinatorRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::RemoveCoordinatorHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });

  server.Register<RegisterInstanceRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::RegisterInstanceHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });

  server.Register<UnregisterInstanceRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::UnregisterInstanceHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });

  server.Register<SetInstanceToMainRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::SetInstanceToMainHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });

  server.Register<DemoteInstanceRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::DemoteInstanceHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });

  server.Register<ForceResetRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                     uint64_t const request_version,
                                     slk::Reader *req_reader,
                                     slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::ForceResetHandler(
        coordinator_instance, request_version, req_reader, res_builder);
  });

  server.Register<ShowInstancesRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                        uint64_t const request_version,
                                        slk::Reader *req_reader,
                                        slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::ShowInstancesHandler(
        coordinator_instance, request_version, req_reader, res_builder);
  });

  server.Register<GetRoutingTableRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::GetRoutingTableHandler(
            coordinator_instance, request_version, req_reader, res_builder);
      });
}

void CoordinatorInstanceManagementServerHandlers::AddCoordinatorHandler(CoordinatorInstance const &coordinator_instance,
                                                                        uint64_t request_version,
                                                                        slk::Reader *req_reader,
                                                                        slk::Builder *res_builder) {
  AddCoordinatorReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.AddCoordinatorInstance(req.arg_);
  AddCoordinatorRes const rpc_res{res == AddCoordinatorInstanceStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::RemoveCoordinatorHandler(
    CoordinatorInstance const &coordinator_instance, uint64_t request_version, slk::Reader *req_reader,
    slk::Builder *res_builder) {
  RemoveCoordinatorReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.RemoveCoordinatorInstance(req.arg_);
  RemoveCoordinatorRes const rpc_res{res == RemoveCoordinatorInstanceStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::RegisterInstanceHandler(CoordinatorInstance &coordinator_instance,
                                                                          uint64_t request_version,
                                                                          slk::Reader *req_reader,
                                                                          slk::Builder *res_builder) {
  RegisterInstanceReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.RegisterReplicationInstance(req.arg_);
  RegisterInstanceRes const rpc_res{res == RegisterInstanceCoordinatorStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::UnregisterInstanceHandler(CoordinatorInstance &coordinator_instance,
                                                                            uint64_t request_version,
                                                                            slk::Reader *req_reader,
                                                                            slk::Builder *res_builder) {
  UnregisterInstanceReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.UnregisterReplicationInstance(req.arg_);
  UnregisterInstanceRes const rpc_res{res == UnregisterInstanceCoordinatorStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::SetInstanceToMainHandler(CoordinatorInstance &coordinator_instance,
                                                                           uint64_t request_version,
                                                                           slk::Reader *req_reader,
                                                                           slk::Builder *res_builder) {
  SetInstanceToMainReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.SetReplicationInstanceToMain(req.arg_);
  SetInstanceToMainRes const rpc_res{res == SetInstanceToMainCoordinatorStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::DemoteInstanceHandler(CoordinatorInstance &coordinator_instance,
                                                                        uint64_t request_version,
                                                                        slk::Reader *req_reader,
                                                                        slk::Builder *res_builder) {
  DemoteInstanceReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.DemoteInstanceToReplica(req.arg_);
  DemoteInstanceRes const rpc_res{res == DemoteInstanceCoordinatorStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::ForceResetHandler(CoordinatorInstance &coordinator_instance,
                                                                    uint64_t request_version, slk::Reader *req_reader,
                                                                    slk::Builder *res_builder) {
  ForceResetReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const res = coordinator_instance.TryVerifyOrCorrectClusterState();
  ForceResetRes const rpc_res{res == ReconcileClusterStateStatus::SUCCESS};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::ShowInstancesHandler(CoordinatorInstance const &coordinator_instance,
                                                                       uint64_t const request_version,
                                                                       slk::Reader *req_reader,
                                                                       slk::Builder *res_builder) {
  ShowInstancesReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  ShowInstancesRes const rpc_res{coordinator_instance.ShowInstancesAsLeader()};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

void CoordinatorInstanceManagementServerHandlers::GetRoutingTableHandler(
    CoordinatorInstance const &coordinator_instance, uint64_t request_version, slk::Reader *req_reader,
    slk::Builder *res_builder) {
  GetRoutingTableReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  GetRoutingTableRes const rpc_res{coordinator_instance.GetRoutingTable(req.db_name_)};
  rpc::SendFinalResponse(rpc_res, request_version, res_builder);
}

}  // namespace memgraph::coordination
#endif
