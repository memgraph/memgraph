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

#include "coordination/coordinator_instance_management_server_handlers.hpp"
#include "coordination/coordinator_rpc.hpp"

#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

namespace memgraph::rpc {
class FileReplicationHandler;
}  // namespace memgraph::rpc

namespace memgraph::coordination {

void CoordinatorInstanceManagementServerHandlers::Register(CoordinatorInstanceManagementServer &server,
                                                           CoordinatorInstance const &coordinator_instance) {
  server.Register<coordination::ShowInstancesRpc>(
      [&](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        CoordinatorInstanceManagementServerHandlers::ShowInstancesHandler(coordinator_instance, request_version,
                                                                          req_reader, res_builder);
      });

  server.Register<coordination::GetRoutingTableRpc>(
    [&](uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) -> void {
      CoordinatorInstanceManagementServerHandlers::GetRoutingTableHandler(coordinator_instance, request_version,
                                                                        req_reader, res_builder);
    });
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
