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

#include <utility>

#include <range/v3/view.hpp>
#include "kvstore/kvstore.hpp"
#include "nuraft/coordinator_state_manager.hpp"
#include "utils/file.hpp"

#include <spdlog/spdlog.h>

#include "utils.hpp"

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
constexpr int kActiveStateManagerDurabilityVersion = 1;

}  // namespace

CoordinatorStateManager::CoordinatorStateManager(CoordinatorStateManagerConfig const &config, LoggerWrapper logger)
    : my_id_(static_cast<int>(config.coordinator_id_)),
      cur_log_store_(cs_new<CoordinatorLogStore>(config.log_store_durability_dir_)),
      logger_(logger),
      kv_store_(config.state_manager_durability_dir_) {
  auto const c2c =
      CoordinatorToCoordinatorConfig{config.coordinator_id_, io::network::Endpoint("0.0.0.0", config.bolt_port_),
                                     io::network::Endpoint{"0.0.0.0", static_cast<uint16_t>(config.coordinator_port_)}};
  my_srv_config_ = cs_new<srv_config>(config.coordinator_id_, 0, c2c.coordinator_server.SocketAddress(),
                                      nlohmann::json(c2c).dump(), false);

  cluster_config_ = cs_new<cluster_config>();
  cluster_config_->get_servers().push_back(my_srv_config_);

  int version{0};
  auto maybe_version = kv_store_.Get(kStateManagerDurabilityVersionKey);
  if (maybe_version.has_value()) {
    version = std::stoi(maybe_version.value());
  } else {
    spdlog::trace("Assuming first start of state manager with durability as version is missing, storing version 1.");
    MG_ASSERT(kv_store_.Put(kStateManagerDurabilityVersionKey, std::to_string(kActiveStateManagerDurabilityVersion)),
              "Failed to store version to disk");
    version = 1;
  }

  MG_ASSERT(version <= kActiveStateManagerDurabilityVersion && version > 0,
            "Unsupported version of log store with durability");
}

auto CoordinatorStateManager::load_config() -> ptr<cluster_config> {
  logger_.Log(nuraft_log_level::TRACE, "Loading cluster config from RocksDb");
  auto const maybe_cluster_config = kv_store_.Get(kClusterConfigKey);
  if (!maybe_cluster_config.has_value()) {
    logger_.Log(nuraft_log_level::TRACE, "Didn't find anything stored on disk for cluster config.");
    return cluster_config_;
  }
  auto cluster_config_json = nlohmann::json::parse(maybe_cluster_config.value());

  cluster_config_ = DeserializeClusterConfig(cluster_config_json);
  logger_.Log(nuraft_log_level::TRACE, "Loaded all cluster configs from RocksDb");
  return cluster_config_;
}

auto CoordinatorStateManager::save_config(cluster_config const &config) -> void {
  ptr<buffer> buf = config.serialize();
  cluster_config_ = cluster_config::deserialize(*buf);
  logger_.Log(nuraft_log_level::TRACE, "Saving cluster config to RocksDb");
  auto json = SerializeClusterConfig(config);
  MG_ASSERT(kv_store_.Put(kClusterConfigKey, json.dump()), "Failed to save servers to disk");
}

auto CoordinatorStateManager::save_state(srv_state const &state) -> void {
  logger_.Log(nuraft_log_level::TRACE, "Saving server state in coordinator state manager.");

  auto const server_state_json = nlohmann::json{{kTerm, state.get_term()},
                                                {kVotedFor, state.get_voted_for()},
                                                {kElectionTimer, state.is_election_timer_allowed()}};
  MG_ASSERT(kv_store_.Put(kServerStateKey, server_state_json.dump()), "Couldn't store server state to disk.");

  ptr<buffer> buf = state.serialize();
  saved_state_ = srv_state::deserialize(*buf);
}

auto CoordinatorStateManager::read_state() -> ptr<srv_state> {
  logger_.Log(nuraft_log_level::TRACE, "Reading server state in coordinator state manager.");

  auto const maybe_server_state = kv_store_.Get(kServerStateKey);
  if (!maybe_server_state.has_value()) {
    spdlog::trace("Didn't find anything stored on disk for server state.");
    return saved_state_;
  }
  auto server_state_json = nlohmann::json::parse(maybe_server_state.value());
  auto const term = server_state_json.at(kTerm.data()).get<ulong>();
  auto const voted_for = server_state_json.at(kVotedFor.data()).get<int>();
  auto const election_timer = server_state_json.at(kElectionTimer.data()).get<bool>();
  saved_state_ = cs_new<srv_state>(term, voted_for, election_timer);
  return saved_state_;
}

auto CoordinatorStateManager::load_log_store() -> ptr<log_store> { return cur_log_store_; }

auto CoordinatorStateManager::server_id() -> int32 { return my_id_; }

auto CoordinatorStateManager::system_exit(int const exit_code) -> void {}

auto CoordinatorStateManager::GetSrvConfig() const -> ptr<srv_config> { return my_srv_config_; }

}  // namespace memgraph::coordination
#endif
