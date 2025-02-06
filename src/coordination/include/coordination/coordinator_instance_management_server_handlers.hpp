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

#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_instance_management_server.hpp"

namespace memgraph::coordination {
#ifdef MG_ENTERPRISE
class CoordinatorInstanceManagementServerHandlers {
 public:
  static void Register(memgraph::coordination::CoordinatorInstanceManagementServer &server,
                       CoordinatorInstance &coordinator_instance);

 private:
  static void ShowInstancesHandler(CoordinatorInstance const &coordinator_instance, slk::Reader *req_reader,
                                   slk::Builder *res_builder);
};

template <typename TResponse>
void SendFinalResponse(TResponse const &res, slk::Builder *builder) {
  slk::Save(TResponse::kType.id, builder);
  slk::Save(rpc::current_version, builder);
  slk::Save(res, builder);
  builder->Finalize();
  spdlog::trace("[RpcServer] sent {}", TResponse::kType.name);
}

#endif
}  // namespace memgraph::coordination
