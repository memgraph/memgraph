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

#include "coordination/coordinator_client.hpp"
#include "coordination/coordinator_client_info.hpp"
#include "coordination/coordinator_server.hpp"
#include "utils/rw_lock.hpp"

#include <list>
#include <memory>
#include <optional>

namespace memgraph::coordination {

struct CoordinatorData {
  std::list<CoordinatorClient> registered_replicas_;
  std::list<CoordinatorClientInfo> registered_replicas_info_;
  std::unique_ptr<CoordinatorClient> registered_main_;
  std::optional<CoordinatorClientInfo> registered_main_info_;

  mutable utils::RWLock coord_data_lock_{utils::RWLock::Priority::READ};
};

struct CoordinatorMainReplicaData {
  std::unique_ptr<CoordinatorServer> coordinator_server_;
};

}  // namespace memgraph::coordination
#endif
