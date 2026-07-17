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

  // Role/privilege writes forwarded from a follower. The response carries a bool success flag; the follower maps a
  // false to a forwarding error rather than the leader's exact status.
  server.Register<CreateRoleRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                     uint64_t const request_version,
                                     slk::Reader *req_reader,
                                     slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<CreateRoleRpc, CreateRoleStatus>(
        [&coordinator_instance](std::string_view role_name) -> CreateRoleStatus {
          return coordinator_instance.CreateRole(role_name);
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<DropRoleRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                   uint64_t const request_version,
                                   slk::Reader *req_reader,
                                   slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<DropRoleRpc, DropRoleStatus>(
        [&coordinator_instance](std::string_view role_name) -> DropRoleStatus {
          return coordinator_instance.DropRole(role_name);
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<GrantPrivilegeRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<GrantPrivilegeRpc, GrantPrivilegeStatus>(
            [&coordinator_instance](std::pair<std::string, uint64_t> const &arg) -> GrantPrivilegeStatus {
              return coordinator_instance.GrantPrivilege(arg.first, arg.second);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<RevokePrivilegeRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<RevokePrivilegeRpc, RevokePrivilegeStatus>(
            [&coordinator_instance](std::pair<std::string, uint64_t> const &arg) -> RevokePrivilegeStatus {
              return coordinator_instance.RevokePrivilege(arg.first, arg.second);
            },
            request_version,
            req_reader,
            res_builder);
      });

  server.Register<SetCoordinatorSettingRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<SetCoordinatorSettingRpc,
                                                                       SetCoordinatorSettingStatus>(
            [&coordinator_instance](std::pair<std::string, std::string> const &arg) -> SetCoordinatorSettingStatus {
              return coordinator_instance.SetCoordinatorSetting(arg.first, arg.second);
            },
            request_version,
            req_reader,
            res_builder);
      });

  // Role/privilege reads forwarded from a follower. An unengaged optional means this coordinator is not the ready
  // leader, which the follower surfaces as a forwarding error.
  server.Register<GetRolesRpc>([&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                                   uint64_t const request_version,
                                   slk::Reader *req_reader,
                                   slk::Builder *res_builder) -> void {
    CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<GetRolesRpc>(
        [&coordinator_instance]() -> std::optional<std::vector<CoordinatorRole>> {
          return coordinator_instance.GetRolesAsLeader();
        },
        request_version,
        req_reader,
        res_builder);
  });

  server.Register<GetRolePrivilegesRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::FwdRequestHandler<GetRolePrivilegesRpc>(
            [&coordinator_instance](std::string_view role_name) -> std::optional<std::pair<bool, uint64_t>> {
              return coordinator_instance.GetRolePrivilegesAsLeader(role_name);
            },
            request_version,
            req_reader,
            res_builder);
      });
}

}  // namespace memgraph::coordination
#endif
