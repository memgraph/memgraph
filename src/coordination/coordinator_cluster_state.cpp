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
  j = nlohmann::json{
      {"config", instance_state.config}, {"status", instance_state.status}, {"uuid", instance_state.instance_uuid}};
}

void from_json(nlohmann::json const &j, ReplicationInstanceState &instance_state) {
  j.at("config").get_to(instance_state.config);
  j.at("status").get_to(instance_state.status);
  j.at("uuid").get_to(instance_state.instance_uuid);
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

auto CoordinatorClusterState::HasMainState(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = repl_instances_.find(instance_name);
  return it != repl_instances_.end() && it->second.status == ReplicationRole::MAIN;
}

auto CoordinatorClusterState::HasReplicaState(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = repl_instances_.find(instance_name);
  return it != repl_instances_.end() && it->second.status == ReplicationRole::REPLICA;
}

auto CoordinatorClusterState::IsCurrentMain(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = repl_instances_.find(instance_name);
  return it != repl_instances_.end() && it->second.status == ReplicationRole::MAIN &&
         it->second.instance_uuid == current_main_uuid_;
}

auto CoordinatorClusterState::DoAction(TRaftLog log_entry, RaftLogAction log_action) -> void {
  auto lock = std::lock_guard{log_lock_};
  switch (log_action) {
      // end of OPEN_LOCK_REGISTER_REPLICATION_INSTANCE
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE: {
      auto const &config = std::get<CoordinatorToReplicaConfig>(log_entry);
      // Setting instance uuid to random, if registration fails, we are still in random state
      repl_instances_.emplace(config.instance_name,
                              ReplicationInstanceState{config, ReplicationRole::REPLICA, utils::UUID{}});
      is_healthy_ = true;
      break;
    }
      // end of OPEN_LOCK_UNREGISTER_REPLICATION_INSTANCE
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);
      repl_instances_.erase(instance_name);
      is_healthy_ = true;
      break;
    }
      // end of OPEN_LOCK_SET_INSTANCE_AS_MAIN and OPEN_LOCK_FAILOVER
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_uuid_change = std::get<InstanceUUIDUpdate>(log_entry);
      auto it = repl_instances_.find(instance_uuid_change.instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::MAIN;
      it->second.instance_uuid = instance_uuid_change.uuid;
      is_healthy_ = true;
      break;
    }
      // end of OPEN_LOCK_SET_INSTANCE_AS_REPLICA
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = repl_instances_.find(instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::REPLICA;
      is_healthy_ = true;
      break;
    }
    case RaftLogAction::UPDATE_UUID_OF_NEW_MAIN: {
      current_main_uuid_ = std::get<utils::UUID>(log_entry);
      break;
    }
    case RaftLogAction::UPDATE_UUID_FOR_INSTANCE: {
      auto const instance_uuid_change = std::get<InstanceUUIDUpdate>(log_entry);
      auto it = repl_instances_.find(instance_uuid_change.instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance doesn't exist as part of RAFT state");
      it->second.instance_uuid = instance_uuid_change.uuid;
      break;
    }
    case RaftLogAction::ADD_COORDINATOR_INSTANCE: {
      auto const &config = std::get<CoordinatorToCoordinatorConfig>(log_entry);
      coordinators_.emplace_back(CoordinatorInstanceState{config});
      break;
    }
    case RaftLogAction::OPEN_LOCK_REGISTER_REPLICATION_INSTANCE: {
      is_healthy_ = false;
      // TODO(antoniofilipovic) save what we are doing to be able to undo....
    }
    case RaftLogAction::OPEN_LOCK_UNREGISTER_REPLICATION_INSTANCE: {
      is_healthy_ = false;
      // TODO(antoniofilipovic) save what we are doing
    }
    case RaftLogAction::OPEN_LOCK_SET_INSTANCE_AS_MAIN: {
      is_healthy_ = false;
      // TODO(antoniofilipovic) save what we are doing
    }
    case RaftLogAction::OPEN_LOCK_FAILOVER: {
      is_healthy_ = false;
      // TODO(antoniofilipovic) save what we are doing
    }
    case RaftLogAction::OPEN_LOCK_SET_INSTANCE_AS_REPLICA: {
      is_healthy_ = false;
      // TODO(antoniofilipovic) save what we need to undo
    }
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto lock = std::shared_lock{log_lock_};

  auto const log = nlohmann::json(repl_instances_).dump();

  data = buffer::alloc(sizeof(uint32_t) + log.size());
  buffer_serializer bs(data);
  bs.put_str(log);
  // TODO(antoniofilipovic) Do we need to serialize here anything else
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

auto CoordinatorClusterState::GetCurrentMainUUID() const -> utils::UUID { return current_main_uuid_; }

auto CoordinatorClusterState::GetInstanceUUID(std::string_view instance_name) const -> utils::UUID {
  auto lock = std::shared_lock{log_lock_};
  auto const it = repl_instances_.find(instance_name);
  MG_ASSERT(it != repl_instances_.end(), "Instance with that name doesn't exist.");
  return it->second.instance_uuid;
}

auto CoordinatorClusterState::GetCoordinatorInstances() const -> std::vector<CoordinatorInstanceState> {
  auto lock = std::shared_lock{log_lock_};
  return coordinators_;
}

auto CoordinatorClusterState::IsHealthy() const -> bool {
  auto lock = std::shared_lock{log_lock_};
  return is_healthy_;
}

}  // namespace memgraph::coordination
#endif
