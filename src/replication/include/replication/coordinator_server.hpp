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

#pragma once

#ifdef MG_ENTERPRISE

#include "replication/config.hpp"
#include "rpc/server.hpp"

namespace memgraph::replication {

class CoordinatorServer {
 public:
  explicit CoordinatorServer(const memgraph::replication::ReplicationServerConfig &config);
  CoordinatorServer(const CoordinatorServer &) = delete;
  CoordinatorServer(CoordinatorServer &&) = delete;
  CoordinatorServer &operator=(const CoordinatorServer &) = delete;
  CoordinatorServer &operator=(CoordinatorServer &&) = delete;

  virtual ~CoordinatorServer();

  bool Start();

  template <typename F, typename TRequestResponse>
  void Register(F &&callback) {
    rpc_server_.Register<TRequestResponse>(std::forward<F>(callback));
  }

 private:
  communication::ServerContext rpc_server_context_;
  rpc::Server rpc_server_;
};

}  // namespace memgraph::replication
#endif
