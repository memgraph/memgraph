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
#include "utils/logging.hpp"

#include <shared_mutex>

namespace memgraph::coordination {

void to_json(nlohmann::json &j, ReplicationInstanceState const &instance_state) {
  j = nlohmann::json{{"config", instance_state.config}, {"status", instance_state.status}};
}

void from_json(nlohmann::json const &j, ReplicationInstanceState &instance_state) {
  j.at("config").get_to(instance_state.config);
  j.at("status").get_to(instance_state.status);
}

CoordinatorClusterState::CoordinatorClusterState(std::map<std::string, ReplicationInstanceState, std::less<>> instances)
    : repl_instances_{std::move(instances)} {}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState const &other)
    : repl_instances_{other.repl_instances_} {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState const &other) {
  if (this == &other) {
    return *this;
  }
  repl_instances_ = other.repl_instances_;
  return *this;
}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState &&other) noexcept
    : repl_instances_{std::move(other.repl_instances_)} {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  repl_instances_ = std::move(other.repl_instances_);
  return *this;
}

auto CoordinatorClusterState::MainExists() const -> bool {
  auto lock = std::shared_lock{log_lock_};
  return std::ranges::any_of(repl_instances_,
                             [](auto const &entry) { return entry.second.status == ReplicationRole::MAIN; });
}

auto CoordinatorClusterState::IsMain(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = repl_instances_.find(instance_name);
  return it != repl_instances_.end() && it->second.status == ReplicationRole::MAIN;
}

auto CoordinatorClusterState::IsReplica(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = repl_instances_.find(instance_name);
  return it != repl_instances_.end() && it->second.status == ReplicationRole::REPLICA;
}

auto CoordinatorClusterState::InsertInstance(std::string instance_name, ReplicationInstanceState instance_state)
    -> void {
  auto lock = std::lock_guard{log_lock_};
  repl_instances_.insert_or_assign(std::move(instance_name), std::move(instance_state));
}

auto CoordinatorClusterState::DoAction(TRaftLog log_entry, RaftLogAction log_action) -> void {
  auto lock = std::lock_guard{log_lock_};
  switch (log_action) {
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE: {
      auto const &config = std::get<CoordinatorToReplicaConfig>(log_entry);
      repl_instances_[config.instance_name] = ReplicationInstanceState{config, ReplicationRole::REPLICA};
      break;
    }
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);
      repl_instances_.erase(instance_name);
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = repl_instances_.find(instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::MAIN;
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = repl_instances_.find(instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::REPLICA;
      break;
    }
    case RaftLogAction::UPDATE_UUID: {
      uuid_ = std::get<utils::UUID>(log_entry);
      break;
    }
    case RaftLogAction::ADD_COORDINATOR_INSTANCE: {
      auto const &config = std::get<CoordinatorToCoordinatorConfig>(log_entry);
      coordinators_.emplace_back(CoordinatorInstanceState{config});
      break;
    }
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto lock = std::shared_lock{log_lock_};

  auto const log = nlohmann::json(repl_instances_).dump();

  data = buffer::alloc(sizeof(uint32_t) + log.size());
  buffer_serializer bs(data);
  bs.put_str(log);
}

auto CoordinatorClusterState::Deserialize(buffer &data) -> CoordinatorClusterState {
  buffer_serializer bs(data);
  auto const j = nlohmann::json::parse(bs.get_str());
  auto instances = j.get<std::map<std::string, ReplicationInstanceState, std::less<>>>();

  return CoordinatorClusterState{std::move(instances)};
}

auto CoordinatorClusterState::GetReplicationInstances() const -> std::vector<ReplicationInstanceState> {
  auto lock = std::shared_lock{log_lock_};
  return repl_instances_ | ranges::views::values | ranges::to<std::vector<ReplicationInstanceState>>;
}

auto CoordinatorClusterState::GetCoordinatorInstances() const -> std::vector<CoordinatorInstanceState> {
  auto lock = std::shared_lock{log_lock_};
  return coordinators_;
}

auto CoordinatorClusterState::GetUUID() const -> utils::UUID { return uuid_; }

}  // namespace memgraph::coordination
#endif
