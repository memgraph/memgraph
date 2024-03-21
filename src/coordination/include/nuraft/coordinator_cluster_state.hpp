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

using replication_coordination_glue::ReplicationRole;

struct ReplicationInstanceState {
  CoordinatorToReplicaConfig config;
  ReplicationRole status;

  friend auto operator==(ReplicationInstanceState const &lhs, ReplicationInstanceState const &rhs) -> bool {
    return lhs.config == rhs.config && lhs.status == rhs.status;
  }
};

// NOTE: Currently instance of coordinator doesn't change from the registration. Hence, just wrap
// CoordinatorToCoordinatorConfig.
struct CoordinatorInstanceState {
  CoordinatorToCoordinatorConfig config;

  friend auto operator==(CoordinatorInstanceState const &lhs, CoordinatorInstanceState const &rhs) -> bool {
    return lhs.config == rhs.config;
  }
};

void to_json(nlohmann::json &j, ReplicationInstanceState const &instance_state);
void from_json(nlohmann::json const &j, ReplicationInstanceState &instance_state);

using TRaftLog = std::variant<CoordinatorToReplicaConfig, CoordinatorToCoordinatorConfig, std::string, utils::UUID>;

using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;

class CoordinatorClusterState {
 public:
  CoordinatorClusterState() = default;
  explicit CoordinatorClusterState(std::map<std::string, ReplicationInstanceState, std::less<>> instances);

  CoordinatorClusterState(CoordinatorClusterState const &);
  CoordinatorClusterState &operator=(CoordinatorClusterState const &);

  CoordinatorClusterState(CoordinatorClusterState &&other) noexcept;
  CoordinatorClusterState &operator=(CoordinatorClusterState &&other) noexcept;
  ~CoordinatorClusterState() = default;

  auto MainExists() const -> bool;

  auto IsMain(std::string_view instance_name) const -> bool;

  auto IsReplica(std::string_view instance_name) const -> bool;

  auto InsertInstance(std::string instance_name, ReplicationInstanceState instance_state) -> void;

  auto DoAction(TRaftLog log_entry, RaftLogAction log_action) -> void;

  auto Serialize(ptr<buffer> &data) -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetReplicationInstances() const -> std::vector<ReplicationInstanceState>;

  auto GetCoordinatorInstances() const -> std::vector<CoordinatorInstanceState>;

  auto GetUUID() const -> utils::UUID;

 private:
  std::vector<CoordinatorInstanceState> coordinators_{};
  std::map<std::string, ReplicationInstanceState, std::less<>> repl_instances_{};
  utils::UUID uuid_{};
  mutable utils::ResourceLock log_lock_{};
};

}  // namespace memgraph::coordination
#endif
