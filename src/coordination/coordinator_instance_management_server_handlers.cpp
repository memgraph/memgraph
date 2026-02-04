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
                                                           CoordinatorInstance const &coordinator_instance) {
  server.Register<AddCoordinatorRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          slk::Reader *req_reader,
          slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::AddCoordinatorHandler(
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
  auto const res = coordinator_instance.AddCoordinatorInstance(req.config_);
  if (res == AddCoordinatorInstanceStatus::SUCCESS) {
    AddCoordinatorRes const rpc_res{true};
    rpc::SendFinalResponse(rpc_res, request_version, res_builder);
    return;
  }

  switch (res) {
    using enum memgraph::coordination::AddCoordinatorInstanceStatus;
    case ID_ALREADY_EXISTS: {
      spdlog::error("Couldn't add coordinator since instance with such id already exists!");
      break;
    }
    case MGMT_ENDPOINT_ALREADY_EXISTS: {
      spdlog::error("Couldn't add coordinator since instance with such management endpoint already exists!");
      break;
    }
    case COORDINATOR_ENDPOINT_ALREADY_EXISTS: {
      spdlog::error("Couldn't add coordinator since instance with such coordinator server already exists!");
      break;
    }
    case RAFT_LOG_ERROR: {
      spdlog::error("Couldn't add coordinator because Raft log couldn't be accepted. Please try again!");
      break;
    }
    default: {
      std::unreachable();
    }
  }

  AddCoordinatorRes const rpc_res{false};
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
