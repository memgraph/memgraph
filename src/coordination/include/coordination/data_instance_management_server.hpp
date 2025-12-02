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

#pragma once

#ifdef MG_ENTERPRISE

#include "rpc/server.hpp"

import memgraph.coordination.coordinator_communication_config;

namespace memgraph::coordination {

class DataInstanceManagementServer {
 public:
  explicit DataInstanceManagementServer(const ManagementServerConfig &config);
  DataInstanceManagementServer(const DataInstanceManagementServer &) = delete;
  DataInstanceManagementServer(DataInstanceManagementServer &&) = delete;
  DataInstanceManagementServer &operator=(const DataInstanceManagementServer &) = delete;
  DataInstanceManagementServer &operator=(DataInstanceManagementServer &&) = delete;

  ~DataInstanceManagementServer();

  bool Start();

  template <typename TRequestResponse, typename F>
  void Register(F &&callback) {
    rpc_server_.Register<TRequestResponse>(std::forward<F>(callback));
  }

 private:
  communication::ServerContext rpc_server_context_;
  rpc::Server rpc_server_;
};

}  // namespace memgraph::coordination
#endif
