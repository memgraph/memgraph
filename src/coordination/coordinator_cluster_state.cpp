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

#include "nuraft/coordinator_cluster_state.hpp"

namespace memgraph::coordination {

auto CoordinatorClusterState::MainExists() const -> bool {
  return std::any_of(instance_roles.begin(), instance_roles.end(), [](auto const &entry) {
    return entry.second == replication_coordination_glue::ReplicationRole::MAIN;
  });
}

auto CoordinatorClusterState::IsMain(std::string const &instance_name) const -> bool {
  auto const it = instance_roles.find(instance_name);
  return it != instance_roles.end() && it->second == replication_coordination_glue::ReplicationRole::MAIN;
}

auto CoordinatorClusterState::IsReplica(std::string const &instance_name) const -> bool {
  auto const it = instance_roles.find(instance_name);
  return it != instance_roles.end() && it->second == replication_coordination_glue::ReplicationRole::REPLICA;
}

auto CoordinatorClusterState::InsertInstance(std::string const &instance_name,
                                             replication_coordination_glue::ReplicationRole role) -> void {
  instance_roles[instance_name] = role;
}

auto CoordinatorClusterState::DoAction(std::string const &instance_name, RaftLogAction log_action) -> void {
  switch (log_action) {
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE:
      instance_roles[instance_name] = replication_coordination_glue::ReplicationRole::REPLICA;
      break;
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE:
      spdlog::info("Unregistering instance: {}", instance_name);
      instance_roles.erase(instance_name);
      spdlog::info("Instance {} unregistered", instance_name);
      break;
    case RaftLogAction::SET_INSTANCE_AS_MAIN:
      instance_roles[instance_name] = replication_coordination_glue::ReplicationRole::MAIN;
      break;
    case RaftLogAction::SET_INSTANCE_AS_REPLICA:
      instance_roles[instance_name] = replication_coordination_glue::ReplicationRole::REPLICA;
      break;
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto const role_to_string = [](auto const &role) {
    switch (role) {
      case replication_coordination_glue::ReplicationRole::MAIN:
        return "main";
      case replication_coordination_glue::ReplicationRole::REPLICA:
        return "replica";
    }
  };

  auto const entry_to_string = [&role_to_string](auto const &entry) {
    return entry.first + "_" + role_to_string(entry.second);
  };

  auto instances_str_view = instance_roles | ranges::views::transform(entry_to_string);
  uint32_t size =
      std::accumulate(instances_str_view.begin(), instances_str_view.end(), 0,
                      [](uint32_t acc, auto const &entry) { return acc + sizeof(uint32_t) + entry.size(); });

  data = buffer::alloc(size);
  buffer_serializer bs(data);
  std::for_each(instances_str_view.begin(), instances_str_view.end(), [&bs](auto const &entry) { bs.put_str(entry); });
}

auto CoordinatorClusterState::Deserialize(buffer &data) -> CoordinatorClusterState {
  auto const str_to_role = [](auto const &str) {
    if (str == "main") {
      return replication_coordination_glue::ReplicationRole::MAIN;
    }
    return replication_coordination_glue::ReplicationRole::REPLICA;
  };

  CoordinatorClusterState cluster_state;
  buffer_serializer bs(data);
  while (bs.size() > 0) {
    auto const entry = bs.get_str();
    auto const first_dash = entry.find('_');
    auto const instance_name = entry.substr(0, first_dash);
    auto const role_str = entry.substr(first_dash + 1);
    cluster_state.InsertInstance(instance_name, str_to_role(role_str));
  }
  return cluster_state;
}

auto CoordinatorClusterState::GetInstances() const -> std::vector<std::pair<std::string, std::string>> {
  auto const role_to_string = [](auto const &role) -> std::string {
    switch (role) {
      case replication_coordination_glue::ReplicationRole::MAIN:
        return "main";
      case replication_coordination_glue::ReplicationRole::REPLICA:
        return "replica";
    }
  };

  auto const entry_to_pair = [&role_to_string](auto const &entry) {
    return std::make_pair(entry.first, role_to_string(entry.second));
  };

  return instance_roles | ranges::views::transform(entry_to_pair) | ranges::to<std::vector>();
}

}  // namespace memgraph::coordination
#endif
