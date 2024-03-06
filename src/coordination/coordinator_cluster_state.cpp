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

void to_json(nlohmann::json &j, InstanceState const &instance_state) {
  j = nlohmann::json{{"config", instance_state.config}, {"status", instance_state.status}};
}

void from_json(nlohmann::json const &j, InstanceState &instance_state) {
  j.at("config").get_to(instance_state.config);
  j.at("status").get_to(instance_state.status);
}

CoordinatorClusterState::CoordinatorClusterState(std::map<std::string, InstanceState, std::less<>> instances)
    : instances_{std::move(instances)} {}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState const &other) : instances_{other.instances_} {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState const &other) {
  if (this == &other) {
    return *this;
  }
  instances_ = other.instances_;
  return *this;
}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState &&other) noexcept
    : instances_{std::move(other.instances_)} {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  instances_ = std::move(other.instances_);
  return *this;
}

auto CoordinatorClusterState::MainExists() const -> bool {
  auto lock = std::shared_lock{log_lock_};
  return std::ranges::any_of(instances_,
                             [](auto const &entry) { return entry.second.status == ReplicationRole::MAIN; });
}

auto CoordinatorClusterState::IsMain(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = instances_.find(instance_name);
  return it != instances_.end() && it->second.status == ReplicationRole::MAIN;
}

auto CoordinatorClusterState::IsReplica(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = instances_.find(instance_name);
  return it != instances_.end() && it->second.status == ReplicationRole::REPLICA;
}

auto CoordinatorClusterState::InsertInstance(std::string instance_name, InstanceState instance_state) -> void {
  auto lock = std::lock_guard{log_lock_};
  instances_.insert_or_assign(std::move(instance_name), std::move(instance_state));
}

auto CoordinatorClusterState::DoAction(TRaftLog log_entry, RaftLogAction log_action) -> void {
  auto lock = std::lock_guard{log_lock_};
  switch (log_action) {
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE: {
      auto const &config = std::get<CoordinatorClientConfig>(log_entry);
      instances_[config.instance_name] = InstanceState{config, ReplicationRole::REPLICA};
      break;
    }
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);
      instances_.erase(instance_name);
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = instances_.find(instance_name);
      MG_ASSERT(it != instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::MAIN;
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = instances_.find(instance_name);
      MG_ASSERT(it != instances_.end(), "Instance does not exist as part of raft state!");
      it->second.status = ReplicationRole::REPLICA;
      break;
    }
    case RaftLogAction::UPDATE_UUID: {
      uuid_ = std::get<utils::UUID>(log_entry);
      break;
    }
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto lock = std::shared_lock{log_lock_};

  // .at(0) is hack to solve the problem with json serialization of map
  auto const log = nlohmann::json{instances_}.at(0).dump();

  data = buffer::alloc(sizeof(uint32_t) + log.size());
  buffer_serializer bs(data);
  bs.put_str(log);
}

auto CoordinatorClusterState::Deserialize(buffer &data) -> CoordinatorClusterState {
  buffer_serializer bs(data);
  auto const j = nlohmann::json::parse(bs.get_str());
  auto instances = j.get<std::map<std::string, InstanceState, std::less<>>>();

  return CoordinatorClusterState{std::move(instances)};
}

auto CoordinatorClusterState::GetInstances() const -> std::vector<InstanceState> {
  auto lock = std::shared_lock{log_lock_};
  return instances_ | ranges::views::values | ranges::to<std::vector<InstanceState>>;
}

auto CoordinatorClusterState::GetUUID() const -> utils::UUID { return uuid_; }

auto CoordinatorClusterState::FindCurrentMainInstanceName() const -> std::optional<std::string> {
  auto lock = std::shared_lock{log_lock_};
  auto const it =
      std::ranges::find_if(instances_, [](auto const &entry) { return entry.second.status == ReplicationRole::MAIN; });
  if (it == instances_.end()) {
    return {};
  }
  return it->first;
}

}  // namespace memgraph::coordination
#endif
