// Copyright 2025 Memgraph Ltd.
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

#include "coordination/coordinator_cluster_state.hpp"
#include "coordination/constants.hpp"
#include "utils/logging.hpp"

#include <shared_mutex>

namespace memgraph::coordination {

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState const &other)
    : data_instances_{other.data_instances_},
      coordinator_instances_(other.coordinator_instances_),
      current_main_uuid_(other.current_main_uuid_),
      enabled_reads_on_main_(other.enabled_reads_on_main_),
      sync_failover_only_(other.sync_failover_only_) {}
CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState const &other) {
  if (this == &other) {
    return *this;
  }
  data_instances_ = other.data_instances_;
  coordinator_instances_ = other.coordinator_instances_;
  current_main_uuid_ = other.current_main_uuid_;
  enabled_reads_on_main_ = other.enabled_reads_on_main_;
  sync_failover_only_ = other.sync_failover_only_;
  return *this;
}

CoordinatorClusterState::CoordinatorClusterState(CoordinatorClusterState &&other) noexcept
    : data_instances_{std::move(other.data_instances_)},
      coordinator_instances_{std::move(other.coordinator_instances_)},
      current_main_uuid_{other.current_main_uuid_},
      enabled_reads_on_main_{other.enabled_reads_on_main_},
      sync_failover_only_(other.sync_failover_only_) {}

CoordinatorClusterState &CoordinatorClusterState::operator=(CoordinatorClusterState &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  data_instances_ = std::move(other.data_instances_);
  coordinator_instances_ = std::move(other.coordinator_instances_);
  current_main_uuid_ = other.current_main_uuid_;
  enabled_reads_on_main_ = other.enabled_reads_on_main_;
  sync_failover_only_ = other.sync_failover_only_;
  return *this;
}

auto CoordinatorClusterState::MainExists() const -> bool {
  auto lock = std::shared_lock{app_lock_};
  return std::ranges::any_of(data_instances_,
                             [](auto const &data_instance) { return data_instance.status == ReplicationRole::MAIN; });
}

// Ideally, we delete this.
auto CoordinatorClusterState::HasMainState(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{app_lock_};
  auto const it = std::ranges::find_if(data_instances_, [instance_name](auto const &data_instance) {
    return data_instance.config.instance_name == instance_name;
  });

  return it != data_instances_.end() && it->status == ReplicationRole::MAIN;
}

auto CoordinatorClusterState::IsCurrentMain(std::string_view instance_name) const -> bool {
  auto lock = std::shared_lock{app_lock_};
  auto const it = std::ranges::find_if(data_instances_, [instance_name](auto const &data_instance) {
    return data_instance.config.instance_name == instance_name;
  });
  return it != data_instances_.end() && it->status == ReplicationRole::MAIN && it->instance_uuid == current_main_uuid_;
}

auto CoordinatorClusterState::DoAction(CoordinatorClusterStateDelta delta_state) -> void {
  auto lock = std::lock_guard{app_lock_};
  if (delta_state.data_instances_.has_value()) {
    data_instances_ = std::move(*delta_state.data_instances_);
  }
  if (delta_state.coordinator_instances_.has_value()) {
    coordinator_instances_ = std::move(*delta_state.coordinator_instances_);
  }
  if (delta_state.current_main_uuid_.has_value()) {
    current_main_uuid_ = *delta_state.current_main_uuid_;
  }
  if (delta_state.enabled_reads_on_main_.has_value()) {
    enabled_reads_on_main_ = *delta_state.enabled_reads_on_main_;
  }
  if (delta_state.sync_failover_only_.has_value()) {
    sync_failover_only_ = *delta_state.sync_failover_only_;
  }
}

auto CoordinatorClusterState::Serialize(ptr<buffer> &data) const -> void {
  auto lock = std::shared_lock{app_lock_};
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

auto CoordinatorClusterState::GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext> {
  auto lock = std::shared_lock{app_lock_};
  return coordinator_instances_;
}

auto CoordinatorClusterState::GetDataInstancesContext() const -> std::vector<DataInstanceContext> {
  auto lock = std::shared_lock{app_lock_};
  return data_instances_;
}

auto CoordinatorClusterState::TryGetCurrentMainName() const -> std::optional<std::string> {
  auto lock = std::shared_lock{app_lock_};

  const auto curr_main = std::ranges::find_if(data_instances_, [this](auto &&data_instance) {
    return data_instance.status == ReplicationRole::MAIN && data_instance.instance_uuid == current_main_uuid_;
  });
  return curr_main == data_instances_.end() ? std::nullopt : std::make_optional(curr_main->config.instance_name);
}

auto CoordinatorClusterState::GetCurrentMainUUID() const -> utils::UUID {
  auto lock = std::shared_lock{app_lock_};
  return current_main_uuid_;
}

auto CoordinatorClusterState::GetEnabledReadsOnMain() const -> bool {
  auto lock = std::shared_lock{app_lock_};
  return enabled_reads_on_main_;
}

auto CoordinatorClusterState::GetSyncFailoverOnly() const -> bool {
  auto lock = std::shared_lock{app_lock_};
  return sync_failover_only_;
}

void CoordinatorClusterState::SetCoordinatorInstances(std::vector<CoordinatorInstanceContext> coordinator_instances) {
  auto lock = std::lock_guard{app_lock_};
  coordinator_instances_ = std::move(coordinator_instances);
}

void CoordinatorClusterState::SetDataInstances(std::vector<DataInstanceContext> data_instances) {
  auto lock = std::lock_guard{app_lock_};
  data_instances_ = std::move(data_instances);
}

void CoordinatorClusterState::SetCurrentMainUUID(utils::UUID current_main_uuid) {
  auto lock = std::lock_guard{app_lock_};
  current_main_uuid_ = current_main_uuid;
}

void CoordinatorClusterState::SetEnabledReadsOnMain(bool const enabled_reads_on_main) {
  auto lock = std::lock_guard{app_lock_};
  enabled_reads_on_main_ = enabled_reads_on_main;
}

void CoordinatorClusterState::SetSyncFailoverOnly(bool const sync_failover_only) {
  auto lock = std::lock_guard{app_lock_};
  sync_failover_only_ = sync_failover_only;
}

void to_json(nlohmann::json &j, CoordinatorClusterState const &state) {
  j = nlohmann::json{{kDataInstances.data(), state.GetDataInstancesContext()},
                     {kMainUUID.data(), state.GetCurrentMainUUID()},
                     {kCoordinatorInstances.data(), state.GetCoordinatorInstancesContext()},
                     {kEnabledReadsOnMain.data(), state.GetEnabledReadsOnMain()},
                     {kSyncFailoverOnly.data(), state.GetSyncFailoverOnly()}};
}

void from_json(nlohmann::json const &j, CoordinatorClusterState &instance_state) {
  instance_state.SetDataInstances(j.at(kDataInstances.data()).get<std::vector<DataInstanceContext>>());
  instance_state.SetCurrentMainUUID(j.at(kMainUUID.data()).get<utils::UUID>());
  instance_state.SetCoordinatorInstances(
      j.at(kCoordinatorInstances.data()).get<std::vector<CoordinatorInstanceContext>>());

  bool const enabled_reads_on_main = j.value(kEnabledReadsOnMain.data(), false);
  instance_state.SetEnabledReadsOnMain(enabled_reads_on_main);

  bool const sync_failover_only = j.value(kSyncFailoverOnly.data(), true);
  instance_state.SetSyncFailoverOnly(sync_failover_only);
}

}  // namespace memgraph::coordination
#endif
