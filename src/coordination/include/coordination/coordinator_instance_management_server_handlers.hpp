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

#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_instance_management_server.hpp"

namespace memgraph::coordination {
class CoordinatorInstanceManagementServerHandlers {
 public:
  static void Register(memgraph::coordination::CoordinatorInstanceManagementServer &server,
                       CoordinatorInstance &coordinator_instance);

 private:
  static void ShowInstancesHandler(CoordinatorInstance const &coordinator_instance, slk::Reader *req_reader,
                                   slk::Builder *res_builder);
};

}  // namespace memgraph::coordination
#endif
