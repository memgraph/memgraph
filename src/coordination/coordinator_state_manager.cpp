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

#include "nuraft/coordinator_state_manager.hpp"
#include "coordination/coordination_observer.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "utils.hpp"
#include "utils/file.hpp"

#include <spdlog/spdlog.h>

#include <range/v3/view.hpp>

namespace memgraph::coordination {

using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;

namespace {
constexpr std::string_view kClusterConfigKey = "cluster_config";  // Key prefix for cluster_config durability

constexpr std::string_view kServerStateKey = "server_state";  // Key prefix for server state durability
constexpr std::string_view kVotedFor = "voted_for";
constexpr std::string_view kTerm = "term";
constexpr std::string_view kElectionTimer = "election_timer";

constexpr std::string_view kStateManagerDurabilityVersionKey = "state_manager_durability_version";

// kV2 includes changes to management server on coordinators
enum class StateManagerDurabilityVersion : uint8_t { kV1 = 1, kV2 = 2 };  // update kV3 for new version
constexpr StateManagerDurabilityVersion kActiveStateManagerDurabilityVersion{StateManagerDurabilityVersion::kV2};

constexpr std::string_view kServers = "servers";
constexpr std::string_view kPrevLogIdx = "prev_log_idx";
constexpr std::string_view kLogIdx = "log_idx";
constexpr std::string_view kAsyncReplication = "async_replication";
constexpr std::string_view kUserCtx = "user_ctx";

}  // namespace

// TODO: (andi) Wrong, don't deserialize into pointer
void from_json(nlohmann::json const &json_cluster_config, ptr<cluster_config> &config) {
  auto servers = json_cluster_config.at(kServers.data()).get<std::vector<std::tuple<int, std::string, std::string>>>();

  auto const prev_log_idx = json_cluster_config.at(kPrevLogIdx.data()).get<int64_t>();
  auto const log_idx = json_cluster_config.at(kLogIdx.data()).get<int64_t>();
  auto const async_replication = json_cluster_config.at(kAsyncReplication.data()).get<bool>();
  auto const user_ctx = json_cluster_config.at(kUserCtx.data()).get<std::string>();
  auto new_cluster_config = cs_new<cluster_config>(log_idx, prev_log_idx, async_replication);
  new_cluster_config->set_user_ctx(user_ctx);
  for (auto &[coord_id, endpoint, aux] : servers) {
    auto one_server_config = cs_new<srv_config>(coord_id, 0, std::move(endpoint), std::move(aux), false);
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
      ranges::to<std::vector>();
  j = nlohmann::json{{kServers, servers_vec},
                     {kPrevLogIdx, cluster_config.get_prev_log_idx()},
                     {kLogIdx, cluster_config.get_log_idx()},
                     {kAsyncReplication, cluster_config.is_async_replication()},
                     {kUserCtx, cluster_config.get_user_ctx()}};
}

auto CoordinatorStateManager::HandleVersionMigration() -> void {
  auto const version = memgraph::coordination::GetOrSetDefaultVersion(
      durability_, kStateManagerDurabilityVersionKey, static_cast<int>(kActiveStateManagerDurabilityVersion), logger_);

  // TODO update when changed
  if (kActiveStateManagerDurabilityVersion == StateManagerDurabilityVersion::kV2 &&
      version == static_cast<int>(StateManagerDurabilityVersion::kV1)) {
    throw VersionMigrationException(
        "Version migration for state manager from V1 to V2 is not supported. Cleanup all high availability directories "
        "and run queries to add instances and coordinators to cluster.");
  }
}
CoordinatorStateManager::CoordinatorStateManager(CoordinatorStateManagerConfig const &config, LoggerWrapper logger,
                                                 std::optional<CoordinationClusterChangeObserver> observer)
    : my_id_(static_cast<int>(config.coordinator_id_)),
      cur_log_store_(cs_new<CoordinatorLogStore>(logger, config.log_store_durability_)),
      logger_(logger),
      durability_(config.state_manager_durability_dir_),
      observer_(observer) {
  auto const c2c = CoordinatorToCoordinatorConfig{
      config.coordinator_id_, io::network::Endpoint(config.coordinator_hostname, config.bolt_port_),
      io::network::Endpoint{config.coordinator_hostname, static_cast<uint16_t>(config.coordinator_port_)},
      io::network::Endpoint{config.coordinator_hostname, static_cast<uint16_t>(config.management_port_)},
      config.coordinator_hostname};
  my_srv_config_ = cs_new<srv_config>(config.coordinator_id_, 0, c2c.coordinator_server.SocketAddress(),
                                      nlohmann::json(c2c).dump(), false);

  cluster_config_ = cs_new<cluster_config>();
  cluster_config_->get_servers().push_back(my_srv_config_);

  HandleVersionMigration();
  TryUpdateClusterConfigFromDisk();
}

auto CoordinatorStateManager::GetCoordinatorToCoordinatorConfigs() const
    -> std::vector<CoordinatorToCoordinatorConfig> {
  std::vector<CoordinatorToCoordinatorConfig> coordinator_to_coordinator_mappings;
  auto const &cluster_config_servers = cluster_config_->get_servers();
  coordinator_to_coordinator_mappings.reserve(cluster_config_servers.size());

  std::ranges::transform(
      cluster_config_servers, std::back_inserter(coordinator_to_coordinator_mappings),
      [](auto &&server) -> CoordinatorToCoordinatorConfig {
        return nlohmann::json::parse(server->get_aux()).template get<CoordinatorToCoordinatorConfig>();
      });
  return coordinator_to_coordinator_mappings;
}

void CoordinatorStateManager::TryUpdateClusterConfigFromDisk() {
  logger_.Log(nuraft_log_level::TRACE, "Loading cluster config from RocksDb");
  auto const maybe_cluster_config = durability_.Get(kClusterConfigKey);
  if (!maybe_cluster_config.has_value()) {
    logger_.Log(nuraft_log_level::TRACE, "Didn't find anything stored on disk for cluster config.");
    return;
  }
  auto cluster_config_json = nlohmann::json::parse(maybe_cluster_config.value());

  from_json(cluster_config_json, cluster_config_);
  logger_.Log(nuraft_log_level::TRACE, "Loaded all cluster configs from RocksDb");
}
auto CoordinatorStateManager::load_config() -> ptr<cluster_config> {
  TryUpdateClusterConfigFromDisk();
  return cluster_config_;
}

auto CoordinatorStateManager::save_config(cluster_config const &config) -> void {
  ptr<buffer> buf = config.serialize();
  cluster_config_ = cluster_config::deserialize(*buf);
  logger_.Log(nuraft_log_level::TRACE, "Saving cluster config to RocksDb");
  nlohmann::json json;
  to_json(json, config);
  auto const ok = durability_.Put(kClusterConfigKey, json.dump());
  if (!ok) {
    throw StoreClusterConfigException("Failed to store cluster config in RocksDb");
  }

  NotifyObserver(GetCoordinatorToCoordinatorConfigs());
}

void CoordinatorStateManager::NotifyObserver(std::vector<CoordinatorToCoordinatorConfig> const &configs) {
  logger_.Log(nuraft_log_level::TRACE, "Notifying observer about cluster config change.");
  if (observer_) {
    observer_.value().Update(configs);
  }
}

auto CoordinatorStateManager::save_state(srv_state const &state) -> void {
  logger_.Log(nuraft_log_level::TRACE, "Saving server state in coordinator state manager.");

  nlohmann::json json;
  to_json(json, state);
  durability_.Put(kServerStateKey, json.dump());

  ptr<buffer> buf = state.serialize();
  saved_state_ = srv_state::deserialize(*buf);
}

auto CoordinatorStateManager::read_state() -> ptr<srv_state> {
  logger_.Log(nuraft_log_level::TRACE, "Reading server state in coordinator state manager.");

  auto const maybe_server_state = durability_.Get(kServerStateKey);
  if (!maybe_server_state.has_value()) {
    logger_.Log(nuraft_log_level::INFO, "Didn't find anything stored on disk for server state.");
    return saved_state_;
  }

  saved_state_ = cs_new<srv_state>();
  auto server_state_json = nlohmann::json::parse(maybe_server_state.value());
  from_json(server_state_json, *saved_state_);

  return saved_state_;
}

auto CoordinatorStateManager::load_log_store() -> ptr<log_store> { return cur_log_store_; }

auto CoordinatorStateManager::server_id() -> int32 { return my_id_; }

auto CoordinatorStateManager::system_exit(int const exit_code) -> void {}

auto CoordinatorStateManager::GetSrvConfig() const -> ptr<srv_config> { return my_srv_config_; }

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
