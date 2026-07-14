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
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<AddCoordinatorRpc, AddCoordinatorInstanceStatus>(
            [&coordinator_instance](CoordinatorInstanceConfig const &config) -> AddCoordinatorInstanceStatus {
              return coordinator_instance.AddCoordinatorInstance(config);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<RemoveCoordinatorRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<RemoveCoordinatorRpc,
                                                                       RemoveCoordinatorInstanceStatus>(
            [&coordinator_instance](int const coord_id) -> RemoveCoordinatorInstanceStatus {
              return coordinator_instance.RemoveCoordinatorInstance(coord_id);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<RegisterInstanceRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<RegisterInstanceRpc,
                                                                       RegisterInstanceCoordinatorStatus>(
            [&coordinator_instance](DataInstanceConfig const &config) -> RegisterInstanceCoordinatorStatus {
              return coordinator_instance.RegisterReplicationInstance(config);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<UnregisterInstanceRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<UnregisterInstanceRpc,
                                                                       UnregisterInstanceCoordinatorStatus>(
            [&coordinator_instance](std::string_view instance_name) -> UnregisterInstanceCoordinatorStatus {
              return coordinator_instance.UnregisterReplicationInstance(instance_name);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<SetInstanceToMainRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<SetInstanceToMainRpc,
                                                                       SetInstanceToMainCoordinatorStatus>(
            [&coordinator_instance](std::string_view new_main_name) -> SetInstanceToMainCoordinatorStatus {
              return coordinator_instance.SetReplicationInstanceToMain(new_main_name);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<DemoteInstanceRpc>([&](std::optional<rpc::FileReplicationHandler> const
                                             & /*file_replication_handler*/,
                                         uint64_t const request_version,
                                         slk::Reader *req_reader,
                                         slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<DemoteInstanceRpc, DemoteInstanceCoordinatorStatus>(
        [&coordinator_instance](std::string_view instance_name) -> DemoteInstanceCoordinatorStatus {
          return coordinator_instance.DemoteInstanceToReplica(instance_name);
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<UpdateConfigRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                       uint64_t const request_version,
                                       slk::Reader *req_reader,
                                       slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<UpdateConfigRpc, UpdateConfigStatus>(
        [&coordinator_instance](UpdateInstanceConfig const &config) -> UpdateConfigStatus {
          return coordinator_instance.UpdateConfig(config);
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<ForceResetRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                     uint64_t const request_version,
                                     slk::Reader *req_reader,
                                     slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<ForceResetRpc, ReconcileClusterStateStatus>(
        [&coordinator_instance]() -> ReconcileClusterStateStatus {
          return coordinator_instance.TryVerifyOrCorrectClusterState();
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<ShowInstancesRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                        uint64_t const request_version,
                                        slk::Reader *req_reader,
                                        slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<ShowInstancesRpc>(
        [&coordinator_instance]() -> std::optional<std::vector<InstanceStatus>> {
          return coordinator_instance.ShowInstancesAsLeader();
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<GetRoutingTableRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<GetRoutingTableRpc>(
            [&coordinator_instance](std::string_view const db_name) -> RoutingTable {
              return coordinator_instance.GetRoutingTable(db_name);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<CoordReplicationLagRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<CoordReplicationLagRpc>(
            [&coordinator_instance]() -> std::map<std::string, std::map<std::string, ReplicaDBLagData>> {
              return coordinator_instance.ShowReplicationLag();
            },
            request_version,
            req_reader,
            res_builder);
      });

  // Role/privilege writes forwarded from a follower. The response carries the leader's exact status enum (as an
  // optional<uint8_t>) so the follower reproduces the same client-visible outcome rather than a bare success/failure.
  server.Register<CreateRoleRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                     uint64_t const request_version,
                                     slk::Reader *req_reader,
                                     slk::Builder *res_builder) -> void {
    CreateRoleReq req;
    rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const status = static_cast<uint8_t>(coordinator_instance.CreateRole(req.arg_));
    rpc::SendFinalResponse(CreateRoleRes{std::optional<uint8_t>{status}}, request_version, res_builder);
  });

  server.Register<DropRoleRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                   uint64_t const request_version,
                                   slk::Reader *req_reader,
                                   slk::Builder *res_builder) -> void {
    DropRoleReq req;
    rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const status = static_cast<uint8_t>(coordinator_instance.DropRole(req.arg_));
    rpc::SendFinalResponse(DropRoleRes{std::optional<uint8_t>{status}}, request_version, res_builder);
  });

  server.Register<GrantPrivilegeRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        GrantPrivilegeReq req;
        rpc::LoadWithUpgrade(req, request_version, req_reader);
        auto const status = static_cast<uint8_t>(coordinator_instance.GrantPrivilege(req.arg_.first, req.arg_.second));
        rpc::SendFinalResponse(GrantPrivilegeRes{std::optional<uint8_t>{status}}, request_version, res_builder);
      });

  server.Register<RevokePrivilegeRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        RevokePrivilegeReq req;
        rpc::LoadWithUpgrade(req, request_version, req_reader);
        auto const status = static_cast<uint8_t>(coordinator_instance.RevokePrivilege(req.arg_.first, req.arg_.second));
        rpc::SendFinalResponse(RevokePrivilegeRes{std::optional<uint8_t>{status}}, request_version, res_builder);
      });

  // Role/privilege reads forwarded from a follower. An unengaged optional means this coordinator is not the ready
  // leader, which the follower surfaces as a forwarding error.
  server.Register<GetRolesRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                   uint64_t const request_version,
                                   slk::Reader *req_reader,
                                   slk::Builder *res_builder) -> void {
    GetRolesReq req;
    rpc::LoadWithUpgrade(req, request_version, req_reader);
    rpc::SendFinalResponse(GetRolesRes{coordinator_instance.GetRolesAsLeader()}, request_version, res_builder);
  });

  server.Register<GetRolePrivilegesRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        GetRolePrivilegesReq req;
        rpc::LoadWithUpgrade(req, request_version, req_reader);
        rpc::SendFinalResponse(GetRolePrivilegesRes{coordinator_instance.GetRolePrivilegesAsLeader(req.arg_)},
                               request_version,
                               res_builder);
      });
}

}  // namespace memgraph::coordination
#endif
