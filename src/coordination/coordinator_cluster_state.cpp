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
  j = nlohmann::json{
      {"config", instance_state.config}, {"status", instance_state.status}, {"uuid", instance_state.instance_uuid}};
}

void from_json(nlohmann::json const &j, DataInstanceState &instance_state) {
  j.at("config").get_to(instance_state.config);
  j.at("status").get_to(instance_state.status);
  j.at("uuid").get_to(instance_state.instance_uuid);
}

CoordinatorClusterState::CoordinatorClusterState(std::vector<DataInstanceState> instances,
                                                 utils::UUID current_main_uuid)
    : data_instances_{std::move(instances)}, current_main_uuid_(current_main_uuid) {}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState const &other)
    : data_instances_{other.data_instances_}, current_main_uuid_(other.current_main_uuid_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState const &other) {
  if (this == &other) {
    return *this;
  }
  data_instances_ = other.data_instances_;
  current_main_uuid_ = other.current_main_uuid_;
  return *this;
}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState &&other) noexcept
    : data_instances_{std::move(other.data_instances_)}, current_main_uuid_(other.current_main_uuid_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  data_instances_ = std::move(other.data_instances_);
  current_main_uuid_ = other.current_main_uuid_;
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

auto CoordinatorClusterState::DoAction(std::pair<std::vector<DataInstanceState>, utils::UUID> log_entry) -> void {
  auto lock = std::lock_guard{log_lock_};
  spdlog::trace("DoAction: update cluster state.");
  data_instances_ = std::move(log_entry.first);
  current_main_uuid_ = log_entry.second;
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

void CoordinatorClusterState::SetDataInstances(std::vector<DataInstanceState> data_instances) {
  auto lock = std::unique_lock{log_lock_};
  data_instances_ = std::move(data_instances);
}

void CoordinatorClusterState::SetCurrentMainUUID(utils::UUID current_main_uuid) {
  auto lock = std::unique_lock{log_lock_};
  current_main_uuid_ = current_main_uuid;
}

void to_json(nlohmann::json &j, CoordinatorClusterState const &state) {
  j = nlohmann::json{{"data_instances", state.GetDataInstances()}, {"current_main_uuid", state.GetCurrentMainUUID()}};
}

void from_json(nlohmann::json const &j, CoordinatorClusterState &instance_state) {
  instance_state.SetDataInstances(j.at("data_instances").get<std::vector<DataInstanceState>>());
  instance_state.SetCurrentMainUUID(j.at("current_main_uuid").get<utils::UUID>());
}

}  // namespace memgraph::coordination
#endif
