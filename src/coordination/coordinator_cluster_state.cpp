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

using replication_coordination_glue::ReplicationRole;

auto CoordinatorClusterState::MainExists() const -> bool {
  return std::ranges::any_of(instance_roles, [](auto const &entry) { return entry.second == ReplicationRole::MAIN; });
}

auto CoordinatorClusterState::IsMain(std::string_view instance_name) const -> bool {
  auto const it = instance_roles.find(instance_name);
  return it != instance_roles.end() && it->second == ReplicationRole::MAIN;
}

auto CoordinatorClusterState::IsReplica(std::string_view instance_name) const -> bool {
  auto const it = instance_roles.find(instance_name);
  return it != instance_roles.end() && it->second == ReplicationRole::REPLICA;
}

auto CoordinatorClusterState::InsertInstance(std::string_view instance_name, ReplicationRole role) -> void {
  instance_roles[instance_name.data()] = role;
}

auto CoordinatorClusterState::DoAction(TRaftLog log_entry, RaftLogAction log_action) -> void {
  switch (log_action) {
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<CoordinatorClientConfig>(log_entry).instance_name;
      instance_roles[instance_name] = ReplicationRole::REPLICA;
      break;
    }
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);
      instance_roles.erase(instance_name);
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = instance_roles.find(instance_name);
      MG_ASSERT(it != instance_roles.end(), "Instance does not exist as part of raft state!");
      it->second = ReplicationRole::MAIN;
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = instance_roles.find(instance_name);
      MG_ASSERT(it != instance_roles.end(), "Instance does not exist as part of raft state!");
      it->second = ReplicationRole::REPLICA;
      break;
    }
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto const role_to_string = [](auto const &role) -> std::string_view {
    switch (role) {
      case ReplicationRole::MAIN:
        return "main";
      case ReplicationRole::REPLICA:
        return "replica";
    }
  };

  auto const entry_to_string = [&role_to_string](auto const &entry) {
    return fmt::format("{}_{}", entry.first, role_to_string(entry.second));
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
  auto const str_to_role = [](auto const &str) -> ReplicationRole {
    if (str == "main") {
      return ReplicationRole::MAIN;
    }
    return ReplicationRole::REPLICA;
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
      case ReplicationRole::MAIN:
        return "main";
      case ReplicationRole::REPLICA:
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
