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

#ifdef MG_ENTERPRISE

#include <algorithm>

#include "coordination/coordination_observer.hpp"
#include "coordination/coordinator_instance.hpp"

#include "json/json.hpp"

namespace memgraph::coordination {

CoordinationClusterChangeObserver::CoordinationClusterChangeObserver(CoordinatorInstance *instance)
    : instance_{instance} {}

void CoordinationClusterChangeObserver::Update(std::vector<CoordinatorToCoordinatorConfig> const &configs) {
  instance_->AddOrUpdateClientConnectors(configs);
}

}  // namespace memgraph::coordination

#endif
