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

#include "coordination/coordinator_communication_config.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/resource_lock.hpp"
#include "utils/uuid.hpp"

#include <libnuraft/nuraft.hxx>
#include <range/v3/view.hpp>
#include "json/json.hpp"

#include <map>
#include <numeric>
#include <string>
#include <variant>

namespace memgraph::coordination {

using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;
using replication_coordination_glue::ReplicationRole;

struct DataInstanceState {
  CoordinatorToReplicaConfig config;
  ReplicationRole status;

  // for replica this is main uuid of current main
  // for "main" main this same as current_main_id_
  // when replica is down and comes back up we reset uuid of main replica is listening to
  // so we need to send swap uuid again
  // For MAIN we don't enable writing until cluster is in healthy state
  utils::UUID instance_uuid;

  friend auto operator==(DataInstanceState const &lhs, DataInstanceState const &rhs) -> bool = default;
};

void to_json(nlohmann::json &j, DataInstanceState const &instance_state);
void from_json(nlohmann::json const &j, DataInstanceState &instance_state);

// Represents the state of the cluster from the coordinator's perspective.
// Source of truth since it is modified only as the result of RAFT's commiting.
class CoordinatorClusterState {
 public:
  CoordinatorClusterState() = default;
  explicit CoordinatorClusterState(std::vector<DataInstanceState> instances, utils::UUID current_main_uuid);

  CoordinatorClusterState(CoordinatorClusterState const &);
  CoordinatorClusterState &operator=(CoordinatorClusterState const &);

  CoordinatorClusterState(CoordinatorClusterState &&other) noexcept;
  CoordinatorClusterState &operator=(CoordinatorClusterState &&other) noexcept;
  ~CoordinatorClusterState() = default;

  auto MainExists() const -> bool;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto IsCurrentMain(std::string_view instance_name) const -> bool;

  auto DoAction(std::pair<std::vector<DataInstanceState>, utils::UUID> log_entry) -> void;

  auto Serialize(ptr<buffer> &data) -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetDataInstances() const -> std::vector<DataInstanceState>;

  auto GetCurrentMainUUID() const -> utils::UUID;

  auto TryGetCurrentMainName() const -> std::optional<std::string>;

  // Setter function used on parsing data from json
  void SetCurrentMainUUID(utils::UUID);

  // Setter function used on parsing data from json
  void SetDataInstances(std::vector<DataInstanceState>);

  friend auto operator==(CoordinatorClusterState const &lhs, CoordinatorClusterState const &rhs) -> bool {
    return lhs.data_instances_ == rhs.data_instances_ && lhs.current_main_uuid_ == rhs.current_main_uuid_;
  }

 private:
  std::vector<DataInstanceState> data_instances_{};
  utils::UUID current_main_uuid_{};
  mutable utils::ResourceLock log_lock_{};
};

void to_json(nlohmann::json &j, CoordinatorClusterState const &state);
void from_json(nlohmann::json const &j, CoordinatorClusterState &instance_state);

}  // namespace memgraph::coordination
#endif
