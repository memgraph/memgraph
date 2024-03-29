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
#include "nuraft/raft_log_action.hpp"
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

struct ReplicationInstanceState {
  CoordinatorToReplicaConfig config;
  ReplicationRole status;

  // for replica this is main uuid of current main
  // for "main" main this same as current_main_id_
  // when replica is down and comes back up we reset uuid of main replica is listening to
  // so we need to send swap uuid again
  // For MAIN we don't enable writing until cluster is in healthy state
  utils::UUID instance_uuid;

  bool needs_demote{false};

  friend auto operator==(ReplicationInstanceState const &lhs, ReplicationInstanceState const &rhs) -> bool {
    return lhs.config == rhs.config && lhs.status == rhs.status && lhs.instance_uuid == rhs.instance_uuid &&
           lhs.needs_demote == rhs.needs_demote;
  }
};

// NOTE: Currently coordinator instance doesn't change from the registration. Hence, just wraps
// CoordinatorToCoordinatorConfig.
struct CoordinatorInstanceState {
  CoordinatorToCoordinatorConfig config;

  friend auto operator==(CoordinatorInstanceState const &lhs, CoordinatorInstanceState const &rhs) -> bool {
    return lhs.config == rhs.config;
  }
};

void to_json(nlohmann::json &j, ReplicationInstanceState const &instance_state);
void from_json(nlohmann::json const &j, ReplicationInstanceState &instance_state);

using TRaftLog = std::variant<std::string, utils::UUID, CoordinatorToReplicaConfig, CoordinatorToCoordinatorConfig,
                              InstanceUUIDUpdate, std::monostate>;

void to_json(nlohmann::json &j, CoordinatorInstanceState const &instance_state);
void from_json(nlohmann::json const &j, CoordinatorInstanceState &instance_state);

// Represents the state of the cluster from the coordinator's perspective.
// Source of truth since it is modified only as the result of RAFT's commiting
class CoordinatorClusterState {
 public:
  CoordinatorClusterState() = default;
  explicit CoordinatorClusterState(std::map<std::string, ReplicationInstanceState, std::less<>> instances,
                                   std::vector<CoordinatorInstanceState> coordinators,
                                   utils::UUID const &current_main_uuid, bool is_lock_opened);

  CoordinatorClusterState(CoordinatorClusterState const &);
  CoordinatorClusterState &operator=(CoordinatorClusterState const &);

  CoordinatorClusterState(CoordinatorClusterState &&other) noexcept;
  CoordinatorClusterState &operator=(CoordinatorClusterState &&other) noexcept;
  ~CoordinatorClusterState() = default;

  auto MainExists() const -> bool;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto HasReplicaState(std::string_view instance_name) const -> bool;

  auto IsCurrentMain(std::string_view instance_name) const -> bool;

  auto DoAction(TRaftLog const &log_entry, RaftLogAction log_action) -> void;

  auto Serialize(ptr<buffer> &data) -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetReplicationInstances() const -> std::vector<ReplicationInstanceState>;

  auto GetCurrentMainUUID() const -> utils::UUID;

  auto GetInstanceUUID(std::string_view) const -> utils::UUID;

  auto IsLockOpened() const -> bool;

  auto CoordinatorExists(uint32_t coordinator_id) const -> bool;

  auto GetCoordinatorInstances() const -> std::vector<CoordinatorInstanceState>;

  friend auto operator==(CoordinatorClusterState const &lhs, CoordinatorClusterState const &rhs) -> bool {
    return lhs.repl_instances_ == rhs.repl_instances_ && lhs.coordinators_ == rhs.coordinators_ &&
           lhs.current_main_uuid_ == rhs.current_main_uuid_;
  }

 private:
  std::map<std::string, ReplicationInstanceState, std::less<>> repl_instances_{};
  std::vector<CoordinatorInstanceState> coordinators_{};
  utils::UUID current_main_uuid_{};
  bool is_lock_opened_{false};
  mutable utils::ResourceLock log_lock_{};
};

}  // namespace memgraph::coordination
#endif
