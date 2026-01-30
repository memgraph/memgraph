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

#include "coordination/coordinator_communication_config.hpp"
#include "rpc/client.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::coordination {

class CoordinatorInstanceClient {
 public:
  explicit CoordinatorInstanceClient(ManagementServerConfig const &config);

  auto RpcClient() const -> rpc::Client &;

 private:
  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;
};
}  // namespace memgraph::coordination
#endif
