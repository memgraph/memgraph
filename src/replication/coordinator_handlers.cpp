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

#include "replication/coordinator_handlers.hpp"
#include "replication/coordinator_rpc.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::replication {

void CoordinatorHandlers::Register(const ReplicationState &repl_state, CoordinatorServer &server) {
  using Callable = std::function<void(slk::Reader * req_reader, slk::Builder * res_builder)>;

  server.Register<Callable, replication::FailoverRpc>(
      [&repl_state](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
        spdlog::info("Received FailoverRpc");
        CoordinatorHandlers::FailoverHandler(repl_state, req_reader, res_builder);
      });
}

void CoordinatorHandlers::FailoverHandler(const ReplicationState &repl_state, slk::Reader *req_reader,
                                          slk::Builder *res_builder) {
  MG_ASSERT(repl_state.IsReplica(), "Failover must me performed on replica!");
  FailoverReq req;
  slk::Load(&req, req_reader);
  for (const auto &config : req.replication_clients_info) {
    spdlog::info("Received replica: {}", config.instance_name);
    spdlog::info("Received endpoint: {}", config.replication_ip_address);
    spdlog::info("Received port: {}", config.replication_port);
    spdlog::info("Received mode: {}\n", config.replication_mode);
  }

  FailoverRes res{true};
  slk::Save(res, res_builder);
  spdlog::info("Failover handler finished execution!");
}

}  // namespace memgraph::replication
#endif
