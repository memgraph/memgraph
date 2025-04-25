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

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance_context.hpp"
#include "coordination/data_instance_context.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/resource_lock.hpp"
#include "utils/uuid.hpp"

#include <libnuraft/nuraft.hxx>
#include <nlohmann/json_fwd.hpp>
#include <range/v3/view.hpp>

#include <string>

namespace memgraph::coordination {

using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;
using replication_coordination_glue::ReplicationRole;

// Represents the state of the cluster from the coordinator's perspective.
// Source of truth since it is modified only as the result of RAFT's commiting.
// Needs to be thread safe because the NuRaft's thread is committing and changing the state
// while possibly the user thread interacts with the API and asks for information.
class CoordinatorClusterState {
 public:
  CoordinatorClusterState() = default;

  CoordinatorClusterState(CoordinatorClusterState const &);
  CoordinatorClusterState &operator=(CoordinatorClusterState const &);

  CoordinatorClusterState(CoordinatorClusterState &&other) noexcept;
  CoordinatorClusterState &operator=(CoordinatorClusterState &&other) noexcept;
  ~CoordinatorClusterState() = default;

  auto MainExists() const -> bool;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto IsCurrentMain(std::string_view instance_name) const -> bool;

  auto DoAction(std::vector<DataInstanceContext> data_instances,
                std::vector<CoordinatorInstanceContext> coordinator_instances, utils::UUID main_uuid) -> void;

  auto Serialize(ptr<buffer> &data) const -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext>;

  auto GetDataInstancesContext() const -> std::vector<DataInstanceContext>;

  auto GetCurrentMainUUID() const -> utils::UUID;

  auto TryGetCurrentMainName() const -> std::optional<std::string>;

  // Setter function used on parsing data from json
  void SetCurrentMainUUID(utils::UUID);

  // Setter function used on parsing data from json
  void SetDataInstances(std::vector<DataInstanceContext>);

  // Setter function used on parsing data from json
  void SetCoordinatorInstances(std::vector<CoordinatorInstanceContext>);

  friend auto operator==(CoordinatorClusterState const &lhs, CoordinatorClusterState const &rhs) -> bool {
    return lhs.data_instances_ == rhs.data_instances_ && lhs.current_main_uuid_ == rhs.current_main_uuid_;
  }

 private:
  std::vector<DataInstanceContext> data_instances_;
  std::vector<CoordinatorInstanceContext> coordinator_instances_;
  utils::UUID current_main_uuid_;
  mutable utils::ResourceLock app_lock_;
};

void to_json(nlohmann::json &j, CoordinatorClusterState const &state);
void from_json(nlohmann::json const &j, CoordinatorClusterState &instance_state);

}  // namespace memgraph::coordination
#endif
