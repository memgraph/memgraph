// Copyright 2026 Memgraph Ltd.
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

#include "parameters/parameters.hpp"
#include "replication/state.hpp"
#include "system/state.hpp"

namespace memgraph::parameters {

// Parameter replication RPC handlers are implemented in replication_handlers.cpp.
// ApplyRecovery, GetSnapshotForRecovery are declared in parameters.hpp.

bool SystemRecoveryHandler(Parameters &parameters, const std::vector<ParameterInfo> &params);

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              Parameters &parameters);

}  // namespace memgraph::parameters
