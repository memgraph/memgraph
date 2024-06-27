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

#include "coordination/coord_instance_management_server_handlers.hpp"
#include "coordination/coordinator_rpc.hpp"

namespace memgraph::coordination {

void CoordinatorInstanceManagementServerHandlers::Register(CoordInstanceManagementServer &server,
                                                           CoordinatorInstance &coordinator_instance) {
  server.Register<coordination::ShowInstancesRpc>([&](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
    spdlog::info("Received ShowInstancesHandlerRpc");
    CoordinatorInstanceManagementServerHandlers::ShowInstancesHandler(coordinator_instance, req_reader, res_builder);
  });
}

void CoordinatorInstanceManagementServerHandlers::ShowInstancesHandler(CoordinatorInstance &coordinator_instance,
                                                                       slk::Reader *req_reader,
                                                                       slk::Builder *res_builder) {
  coordination::ShowInstancesReq req;
  slk::Load(&req, req_reader);

  auto instances = coordinator_instance.ShowInstances();

  coordination::ShowInstancesRes res;
  slk::Save(res, res_builder);
}

}  // namespace memgraph::coordination
