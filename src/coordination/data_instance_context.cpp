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

#ifdef MG_ENTERPRISE

#include "coordination/data_instance_context.hpp"
#include <nlohmann/json.hpp>

import memgraph.coordination.constants;

namespace memgraph::coordination {

void to_json(nlohmann::json &j, DataInstanceContext const &instance_state) {
  j = nlohmann::json{{kConfig.data(), instance_state.config},
                     {kStatus.data(), instance_state.status},
                     {kUuid, instance_state.instance_uuid}};
}

void from_json(nlohmann::json const &j, DataInstanceContext &instance_state) {
  j.at(kConfig.data()).get_to(instance_state.config);
  j.at(kStatus.data()).get_to(instance_state.status);
  j.at(kUuid.data()).get_to(instance_state.instance_uuid);
}

}  // namespace memgraph::coordination

#endif
