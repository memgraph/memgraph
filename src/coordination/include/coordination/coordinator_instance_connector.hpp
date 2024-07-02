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

#include <memory>
#include "coordination/coordinator_instance_client.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "coordination/instance_status.hpp"

namespace memgraph::coordination {
class CoordinatorInstanceConnector {
 public:
  explicit CoordinatorInstanceConnector(CoordinatorInstanceManagementServerConfig const &config, int leader_id)
      : client_{std::make_unique<CoordinatorInstanceClient>(config)}, leader_id_(leader_id) {}

  auto SendShowInstances() -> std::optional<std::vector<InstanceStatus>>;

  [[nodiscard]] auto LeaderId() const -> int;

 private:
  std::unique_ptr<CoordinatorInstanceClient> client_;
  const int leader_id_;
};
}  // namespace memgraph::coordination
