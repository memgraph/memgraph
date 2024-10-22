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

void to_json(nlohmann::json &j, DataInstanceState const &instance_state) {
  j = nlohmann::json{{"config", instance_state.config},
                     {"status", instance_state.status},
                     {"uuid", instance_state.instance_uuid},
                     {"needs_demote", instance_state.needs_demote}};
}

void from_json(nlohmann::json const &j, DataInstanceState &instance_state) {
  j.at("config").get_to(instance_state.config);
  j.at("status").get_to(instance_state.status);
  j.at("uuid").get_to(instance_state.instance_uuid);
  j.at("needs_demote").get_to(instance_state.needs_demote);
}

CoordinatorClusterState::CoordinatorClusterState(std::vector<DataInstanceState> instances,
                                                 utils::UUID current_main_uuid, bool is_lock_opened)
    : data_instances_{std::move(instances)}, current_main_uuid_(current_main_uuid), is_lock_opened_(is_lock_opened) {}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState const &other)
    : data_instances_{other.data_instances_},
      current_main_uuid_(other.current_main_uuid_),
      is_lock_opened_(other.is_lock_opened_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState const &other) {
  if (this == &other) {
    return *this;
  }
  data_instances_ = other.data_instances_;
  current_main_uuid_ = other.current_main_uuid_;
  is_lock_opened_ = other.is_lock_opened_;
  return *this;
}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState &&other) noexcept
    : data_instances_{std::move(other.data_instances_)},
      current_main_uuid_(other.current_main_uuid_),
      is_lock_opened_(other.is_lock_opened_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  data_instances_ = std::move(other.data_instances_);
  current_main_uuid_ = other.current_main_uuid_;
  is_lock_opened_ = other.is_lock_opened_;
  return *this;
}

auto CoordinatorClusterState::MainExists() const -> bool {
  auto lock = std::shared_lock{log_lock_};
  return std::ranges::any_of(data_instances_,
                             [](auto &&data_instance) { return data_instance.status == ReplicationRole::MAIN; });
}

// Ideally, we delete this.
auto CoordinatorClusterState::HasMainState(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = std::ranges::find_if(data_instances_, [instance_name](auto &&data_instance) {
    return data_instance.config.instance_name == instance_name;
  });

  return it != data_instances_.end() && it->status == ReplicationRole::MAIN;
}

auto CoordinatorClusterState::IsCurrentMain(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{log_lock_};
  auto const it = std::ranges::find_if(data_instances_, [instance_name](auto &&data_instance) {
    return data_instance.config.instance_name == instance_name;
  });
  return it != data_instances_.end() && it->status == ReplicationRole::MAIN && it->instance_uuid == current_main_uuid_;
}

auto CoordinatorClusterState::DoAction(TRaftLog const &log_entry, RaftLogAction log_action) -> void {
  auto lock = std::lock_guard{log_lock_};
  switch (log_action) {
    case RaftLogAction::UPDATE_CLUSTER_STATE: {
      spdlog::trace("DoAction: update cluster state.");
      auto data = std::get<std::pair<std::vector<DataInstanceState>, utils::UUID>>(log_entry);
      data_instances_ = std::move(data.first);
      current_main_uuid_ = data.second;
      break;
    }
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE: {
      auto const &config = std::get<CoordinatorToReplicaConfig>(log_entry);
      spdlog::trace("DoAction: register replication instance {}.", config.instance_name);
      // Setting instance uuid to random, if registration fails, we are still in random state
      data_instances_.emplace_back(config, ReplicationRole::REPLICA, utils::UUID{}, false);
      break;
    }
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);
      spdlog::trace("DoAction: unregister replication instance {}.", instance_name);
      std::ranges::remove_if(data_instances_, [&instance_name](auto &&data_instance) {
        return data_instance.config.instance_name == instance_name;
      });
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_uuid_change = std::get<InstanceUUIDUpdate>(log_entry);
      auto it = std::ranges::find_if(data_instances_,
                                     [instance_name = instance_uuid_change.instance_name](auto &&data_instance) {
                                       return data_instance.config.instance_name == instance_name;
                                     });
      MG_ASSERT(it != data_instances_.end(), "Instance does not exist as part of raft state!");
      it->status = ReplicationRole::MAIN;
      it->instance_uuid = instance_uuid_change.uuid;
      spdlog::trace("DoAction: set replication instance {} as main with uuid {}", instance_uuid_change.instance_name,
                    std::string{instance_uuid_change.uuid});
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = std::ranges::find_if(data_instances_, [&instance_name](auto &&data_instance) {
        return data_instance.config.instance_name == instance_name;
      });
      MG_ASSERT(it != data_instances_.end(), "Instance does not exist as part of raft state!");
      it->status = ReplicationRole::REPLICA;
      it->needs_demote = false;
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
      auto it = std::ranges::find_if(data_instances_,
                                     [instance_name = instance_uuid_change.instance_name](auto &&data_instance) {
                                       return data_instance.config.instance_name == instance_name;
                                     });

      MG_ASSERT(it != data_instances_.end(), "Instance doesn't exist as part of RAFT state");
      it->instance_uuid = instance_uuid_change.uuid;
      spdlog::trace("DoAction: update uuid for instance {} to {}", instance_uuid_change.instance_name,
                    std::string{instance_uuid_change.uuid});
      break;
    }
    case RaftLogAction::INSTANCE_NEEDS_DEMOTE: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto it = std::ranges::find_if(data_instances_, [&instance_name](auto &&data_instance) {
        return data_instance.config.instance_name == instance_name;
      });
      MG_ASSERT(it != data_instances_.end(), "Instance does not exist as part of raft state!");
      it->needs_demote = true;
      spdlog::trace("Added action that instance {} needs demote to replica", instance_name);
      break;
    }
    case RaftLogAction::OPEN_LOCK: {
      is_lock_opened_ = true;
      spdlog::trace("DoAction: Opened lock");
      break;
    }
    case RaftLogAction::CLOSE_LOCK: {
      is_lock_opened_ = false;
      spdlog::trace("DoAction: Closed lock");
      break;
    }
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) -> void {
  auto lock = std::shared_lock{log_lock_};
  nlohmann::json json;
  to_json(json, *this);
  auto const log = json.dump();
  data = buffer::alloc(sizeof(uint32_t) + log.size());
  buffer_serializer bs(data);
  bs.put_str(log);
}

auto CoordinatorClusterState::Deserialize(buffer &data) -> CoordinatorClusterState {
  buffer_serializer bs(data);
  auto const j = nlohmann::json::parse(bs.get_str());

  CoordinatorClusterState cluster_state;
  j.get_to(cluster_state);
  return cluster_state;
}

auto CoordinatorClusterState::GetDataInstances() const -> std::vector<DataInstanceState> {
  auto lock = std::shared_lock{log_lock_};
  return data_instances_;
}

auto CoordinatorClusterState::TryGetCurrentMainName() const -> std::optional<std::string> {
  auto lock = std::shared_lock{log_lock_};

  auto curr_main = std::ranges::find_if(data_instances_, [this](auto &&data_instance) {
    return data_instance.status == ReplicationRole::MAIN && data_instance.instance_uuid == current_main_uuid_;
  });
  return curr_main == data_instances_.end() ? std::nullopt : std::make_optional(curr_main->config.instance_name);
}

auto CoordinatorClusterState::GetCurrentMainUUID() const -> utils::UUID { return current_main_uuid_; }

auto CoordinatorClusterState::IsLockOpened() const -> bool {
  auto lock = std::shared_lock{log_lock_};
  return is_lock_opened_;
}

void CoordinatorClusterState::SetIsLockOpened(bool is_lock_opened) {
  auto lock = std::unique_lock{log_lock_};
  is_lock_opened_ = is_lock_opened;
}

void CoordinatorClusterState::SetDataInstances(std::vector<DataInstanceState> data_instances) {
  auto lock = std::unique_lock{log_lock_};
  data_instances_ = std::move(data_instances);
}

void CoordinatorClusterState::SetCurrentMainUUID(utils::UUID current_main_uuid) {
  auto lock = std::unique_lock{log_lock_};
  current_main_uuid_ = current_main_uuid;
}

void to_json(nlohmann::json &j, CoordinatorClusterState const &state) {
  j = nlohmann::json{{"data_instances", state.GetDataInstances()},
                     {"is_lock_opened", state.IsLockOpened()},
                     {"current_main_uuid", state.GetCurrentMainUUID()}};
}

void from_json(nlohmann::json const &j, CoordinatorClusterState &instance_state) {
  instance_state.SetDataInstances(j.at("data_instances").get<std::vector<DataInstanceState>>());
  instance_state.SetIsLockOpened(j.at("is_lock_opened").get<int>());
  instance_state.SetCurrentMainUUID(j.at("current_main_uuid").get<utils::UUID>());
}

}  // namespace memgraph::coordination
#endif
