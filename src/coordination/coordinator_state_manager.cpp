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

#include "coordination/coordinator_state_manager.hpp"
#include "coordination/coordination_observer.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/utils.hpp"
#include "utils/logging.hpp"

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>
#include <range/v3/view.hpp>

namespace memgraph::coordination {
using nuraft::cluster_config;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;

namespace {
constexpr std::string_view kClusterConfigKey = "cluster_config";  // Key prefix for cluster_config durability
constexpr std::string_view kServerStateKey = "server_state";      // Key prefix for server state durability
constexpr std::string_view kVotedFor = "voted_for";
constexpr std::string_view kTerm = "term";
constexpr std::string_view kElectionTimer = "election_timer";
constexpr std::string_view kStateManagerDurabilityVersionKey = "state_manager_durability_version";

// kV2 includes changes to management server on coordinators
enum class StateManagerDurabilityVersion : uint8_t { kV1 = 1, kV2 = 2 };  // update kV3 for new version
constexpr auto kActiveStateManagerDurabilityVersion{StateManagerDurabilityVersion::kV2};

constexpr std::string_view kServers = "servers";
constexpr std::string_view kPrevLogIdx = "prev_log_idx";
constexpr std::string_view kLogIdx = "log_idx";
constexpr std::string_view kAsyncReplication = "async_replication";
constexpr std::string_view kUserCtx = "user_ctx";
}  // namespace

void from_json(nlohmann::json const &json_cluster_config, std::shared_ptr<cluster_config> &config) {
  auto servers = json_cluster_config.at(kServers.data()).get<std::vector<std::tuple<int, std::string, std::string>>>();

  auto const prev_log_idx = json_cluster_config.at(kPrevLogIdx.data()).get<int64_t>();
  auto const log_idx = json_cluster_config.at(kLogIdx.data()).get<int64_t>();
  auto const async_replication = json_cluster_config.at(kAsyncReplication.data()).get<bool>();
  auto const user_ctx = json_cluster_config.at(kUserCtx.data()).get<std::string>();

  auto const new_cluster_config = std::make_shared<cluster_config>(log_idx, prev_log_idx, async_replication);
  new_cluster_config->set_user_ctx(user_ctx);
  for (auto &[coord_id, endpoint, aux] : servers) {
    auto one_server_config = std::make_shared<srv_config>(coord_id, 0, std::move(endpoint), std::move(aux), false);
    new_cluster_config->get_servers().push_back(std::move(one_server_config));
  }
  config = new_cluster_config;
}

void to_json(nlohmann::json &j, cluster_config const &cluster_config) {
  auto const servers_vec =
      ranges::views::transform(
          cluster_config.get_servers(),
          [](auto const &server) {
            return std::tuple{static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux()};
          }) |
      ranges::to_vector;
  j = nlohmann::json{{kServers, servers_vec},
                     {kPrevLogIdx, cluster_config.get_prev_log_idx()},
                     {kLogIdx, cluster_config.get_log_idx()},
                     {kAsyncReplication, cluster_config.is_async_replication()},
                     {kUserCtx, cluster_config.get_user_ctx()}};
}

auto CoordinatorStateManager::HandleVersionMigration() -> void {
  auto const version = GetOrSetDefaultVersion(durability_, kStateManagerDurabilityVersionKey,
                                              static_cast<int>(kActiveStateManagerDurabilityVersion), logger_);

  if constexpr (kActiveStateManagerDurabilityVersion == StateManagerDurabilityVersion::kV2) {
    if (version == static_cast<int>(StateManagerDurabilityVersion::kV1)) {
      throw VersionMigrationException(
          "Version migration for state manager from V1 to V2 is not supported. Cleanup all high availability "
          "directories "
          "and run queries to add instances and coordinators to cluster.");
    }
  }
}

CoordinatorStateManager::CoordinatorStateManager(CoordinatorStateManagerConfig const &config, LoggerWrapper logger,
                                                 std::optional<CoordinationClusterChangeObserver> const &observer)
    : my_id_(config.coordinator_id_),
      cur_log_store_(std::make_shared<CoordinatorLogStore>(logger, config.log_store_durability_)),
      logger_(logger),
      durability_(config.state_manager_durability_dir_),
      observer_(observer) {
  auto const coord_instance_aux = CoordinatorInstanceAux{
      .id = config.coordinator_id_,
      .coordinator_server = fmt::format("{}:{}", config.coordinator_hostname, config.coordinator_port_),
      .management_server = fmt::format("{}:{}", config.coordinator_hostname, config.management_port_),
  };

  bool constexpr learner{false};
  my_srv_config_ = std::make_shared<srv_config>(config.coordinator_id_, 0, coord_instance_aux.coordinator_server,
                                                nlohmann::json(coord_instance_aux).dump(), learner);

  cluster_config_ = std::make_shared<cluster_config>();
  cluster_config_->get_servers().push_back(my_srv_config_);

  HandleVersionMigration();
  TryUpdateClusterConfigFromDisk();
}

void CoordinatorStateManager::TryUpdateClusterConfigFromDisk() {
  auto const maybe_cluster_config = durability_.Get(kClusterConfigKey);
  if (!maybe_cluster_config) {
    spdlog::trace("Didn't find anything stored on disk for cluster config.");
    return;
  }
  try {
    const auto cluster_config_json = nlohmann::json::parse(maybe_cluster_config.value());
    from_json(cluster_config_json, cluster_config_);
  } catch (std::exception const &e) {
    LOG_FATAL("Error occurred while parsing cluster config {}", e.what());
  }

  spdlog::trace("Loaded cluster config from the durable storage.");
}

// Called when application is starting up
auto CoordinatorStateManager::load_config() -> std::shared_ptr<cluster_config> {
  spdlog::trace("Got request to update config from disk");
  TryUpdateClusterConfigFromDisk();
  return cluster_config_;
}

auto CoordinatorStateManager::GetCoordinatorInstancesAux() const -> std::vector<CoordinatorInstanceAux> {
  auto const &cluster_config_servers = cluster_config_->get_servers();
  std::vector<CoordinatorInstanceAux> coord_instances_aux;
  coord_instances_aux.reserve(cluster_config_servers.size());

  try {
    std::ranges::transform(cluster_config_servers, std::back_inserter(coord_instances_aux),
                           [](auto const &server) -> CoordinatorInstanceAux {
                             auto j = nlohmann::json::parse(server->get_aux());
                             return j.template get<CoordinatorInstanceAux>();
                           });
  } catch (std::exception const &e) {
    LOG_FATAL("Error occurred while parsing aux field {}", e.what());
  }

  return coord_instances_aux;
}

auto CoordinatorStateManager::save_config(cluster_config const &config) -> void {
  spdlog::trace("Got request to save config.");
  std::shared_ptr<buffer> const buf = config.serialize();
  cluster_config_ = cluster_config::deserialize(*buf);
  nlohmann::json json;
  to_json(json, config);

  if (auto const ok = durability_.Put(kClusterConfigKey, json.dump()); !ok) {
    throw StoreClusterConfigException("Failed to store cluster config in RocksDb");
  }

  spdlog::trace("Successfully saved cluster config to the durable storage.");

  NotifyObserver(GetCoordinatorInstancesAux());
  spdlog::trace("Successfully notified observer about changes in the cluster configuration.");
}

void CoordinatorStateManager::NotifyObserver(std::vector<CoordinatorInstanceAux> const &coord_instances_aux) const {
  spdlog::trace("Notifying observer about cluster config change.");
  if (observer_) {
    observer_.value().Update(coord_instances_aux);
  }
}

auto CoordinatorStateManager::save_state(srv_state const &state) -> void {
  spdlog::trace("Saving server state in coordinator state manager.");

  nlohmann::json json;
  to_json(json, state);
  durability_.Put(kServerStateKey, json.dump());

  std::shared_ptr<buffer> const buf = state.serialize();
  saved_state_ = srv_state::deserialize(*buf);
}

auto CoordinatorStateManager::read_state() -> std::shared_ptr<srv_state> {
  spdlog::trace("Reading server state in coordinator state manager.");

  auto const maybe_server_state = durability_.Get(kServerStateKey);
  if (!maybe_server_state) {
    logger_.Log(nuraft_log_level::INFO, "Didn't find anything stored on disk for server state.");
    return saved_state_;
  }

  try {
    saved_state_ = std::make_shared<srv_state>();
    auto const server_state_json = nlohmann::json::parse(maybe_server_state.value());
    from_json(server_state_json, *saved_state_);
  } catch (std::exception const &e) {
    LOG_FATAL("Error occurred while reading server state {}", e.what());
  }

  return saved_state_;
}

auto CoordinatorStateManager::load_log_store() -> std::shared_ptr<log_store> { return cur_log_store_; }

auto CoordinatorStateManager::server_id() -> int32 { return my_id_; }

auto CoordinatorStateManager::system_exit(int const exit_code) -> void {}

auto CoordinatorStateManager::GetSrvConfig() const -> std::shared_ptr<srv_config> { return my_srv_config_; }

void to_json(nlohmann::json &j, srv_state const &state) {
  j = nlohmann::json{{kTerm.data(), state.get_term()},
                     {kVotedFor.data(), state.get_voted_for()},
                     {kElectionTimer.data(), state.is_election_timer_allowed()}};
}

void from_json(const nlohmann::json &j, srv_state &state) {
  state.set_term(j.at(kTerm.data()).get<ulong>());
  state.set_voted_for(j.at(kVotedFor.data()).get<int>());
  state.allow_election_timer(j.at(kElectionTimer.data()).get<bool>());
}
}  // namespace memgraph::coordination
#endif
