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

CoordinatorClusterState::CoordinatorClusterState(std::map<std::string, ReplicationInstanceState, std::less<>> instances,
                                                 utils::UUID const &current_main_uuid, bool is_lock_opened)
    : repl_instances_{std::move(instances)}, current_main_uuid_(current_main_uuid), is_lock_opened_(is_lock_opened) {}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState const &other)
    : repl_instances_{other.repl_instances_},
      current_main_uuid_(other.current_main_uuid_),
      is_lock_opened_(other.is_lock_opened_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState const &other) {
  if (this == &other) {
    return *this;
  }
  repl_instances_ = other.repl_instances_;
  current_main_uuid_ = other.current_main_uuid_;
  is_lock_opened_ = other.is_lock_opened_;
  return *this;
}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState &&other) noexcept
    : repl_instances_{std::move(other.repl_instances_)},
      current_main_uuid_(other.current_main_uuid_),
      is_lock_opened_(other.is_lock_opened_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  repl_instances_ = std::move(other.repl_instances_);
  current_main_uuid_ = other.current_main_uuid_;
  is_lock_opened_ = other.is_lock_opened_;
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
      spdlog::trace("DoAction: register replication instance {}", config.instance_name);
      // Setting instance uuid to random, if registration fails, we are still in random state
      repl_instances_.emplace(config.instance_name,
                              ReplicationInstanceState{config, ReplicationRole::REPLICA, utils::UUID{}});
      is_lock_opened_ = false;
      break;
    }
      // end of OPEN_LOCK_UNREGISTER_REPLICATION_INSTANCE
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);
      spdlog::trace("DoAction: unregister replication instance {}", instance_name);
      repl_instances_.erase(instance_name);
      is_lock_opened_ = false;
      break;
    }
      // end of OPEN_LOCK_SET_INSTANCE_AS_MAIN and OPEN_LOCK_FAILOVER
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_uuid_change = std::get<InstanceUUIDUpdate>(log_entry);
      auto it = repl_instances_.find(instance_uuid_change.instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::MAIN;
      it->second.instance_uuid = instance_uuid_change.uuid;
      is_lock_opened_ = false;
      spdlog::trace("DoAction: set replication instance {} as main with uuid {}", instance_uuid_change.instance_name,
                    std::string{instance_uuid_change.uuid});
      break;
    }
      // end of OPEN_LOCK_SET_INSTANCE_AS_REPLICA
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = repl_instances_.find(instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::REPLICA;
      is_lock_opened_ = false;
      spdlog::trace("DoAction: set replication instance {} as replica", instance_name);
      break;
    }
    case RaftLogAction::UPDATE_UUID_OF_NEW_MAIN: {
      current_main_uuid_ = std::get<utils::UUID>(log_entry);
      spdlog::trace("DoAction: update uuid of new main {}", std::string{current_main_uuid_});
      break;
    }
    case RaftLogAction::UPDATE_UUID_FOR_INSTANCE: {
      auto const instance_uuid_change = std::get<InstanceUUIDUpdate>(log_entry);
      auto it = repl_instances_.find(instance_uuid_change.instance_name);
      MG_ASSERT(it != repl_instances_.end(), "Instance doesn't exist as part of RAFT state");
      it->second.instance_uuid = instance_uuid_change.uuid;
      spdlog::trace("DoAction: update uuid for instance {} to {}", instance_uuid_change.instance_name,
                    std::string{instance_uuid_change.uuid});
      break;
    }
    case RaftLogAction::ADD_COORDINATOR_INSTANCE: {
      auto const &config = std::get<CoordinatorToCoordinatorConfig>(log_entry);
      coordinators_.emplace_back(CoordinatorInstanceState{config});
      spdlog::trace("DoAction: add coordinator instance {}", config.coordinator_server_id);
      break;
    }
    case RaftLogAction::OPEN_LOCK: {
      is_lock_opened_ = true;
      spdlog::trace("DoAction: Opened lock");
      break;
    }
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto lock = std::shared_lock{log_lock_};
  nlohmann::json j = {{"repl_instances", repl_instances_},
                      {"is_lock_opened", is_lock_opened_},
                      {"current_main_uuid", current_main_uuid_}};
  auto const log = j.dump();
  data = buffer::alloc(sizeof(uint32_t) + log.size());
  buffer_serializer bs(data);
  bs.put_str(log);
}

auto CoordinatorClusterState::Deserialize(buffer &data) -> CoordinatorClusterState {
  buffer_serializer bs(data);
  auto const j = nlohmann::json::parse(bs.get_str());
  auto instances = j["repl_instances"].get<std::map<std::string, ReplicationInstanceState, std::less<>>>();
  auto current_main_uuid = j["current_main_uuid"].get<utils::UUID>();
  bool is_lock_opened = j["is_lock_opened"].get<int>();
  return CoordinatorClusterState{std::move(instances), current_main_uuid, is_lock_opened};
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

auto CoordinatorClusterState::IsLockOpened() const -> bool {
  auto lock = std::shared_lock{log_lock_};
  return is_lock_opened_;
}

}  // namespace memgraph::coordination
#endif
