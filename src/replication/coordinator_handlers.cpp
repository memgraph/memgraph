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

void CoordinatorHandlers::Register(CoordinatorServer &server) {
  using Callable = std::function<void(slk::Reader * req_reader, slk::Builder * res_builder)>;

  server.Register<Callable, replication::FailoverRpc>([](slk::Reader *req_reader, slk::Builder *res_builder) -> void {
    spdlog::debug("Received FailoverRpc");
    CoordinatorHandlers::FailoverHandler(req_reader, res_builder);
  });
}

void CoordinatorHandlers::FailoverHandler(slk::Reader * /*req_reader*/, slk::Builder * /*res_builder*/) {
  spdlog::debug("Failover handler finished execution!");
}

}  // namespace memgraph::replication
#endif
