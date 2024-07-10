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

#pragma once

#ifdef MG_ENTERPRISE

#include "coordination/coordination_observer.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "kvstore/kvstore.hpp"
#include "nuraft/constants_log_durability.hpp"
#include "nuraft/coordinator_log_store.hpp"
#include "nuraft/logger_wrapper.hpp"

#include <spdlog/spdlog.h>

namespace memgraph::coordination {

using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::logger;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;

class CoordinatorStateManager : public state_mgr {
 public:
  explicit CoordinatorStateManager(CoordinatorStateManagerConfig const &config, LoggerWrapper logger,
                                   std::optional<CoordinationClusterChangeObserver> observer = {});

  CoordinatorStateManager(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager(CoordinatorStateManager &&) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager &&) = delete;

  ~CoordinatorStateManager() override = default;

  auto load_config() -> ptr<cluster_config> override;

  auto save_config(cluster_config const &config) -> void override;

  auto save_state(srv_state const &state) -> void override;

  auto read_state() -> ptr<srv_state> override;

  auto load_log_store() -> ptr<log_store> override;

  auto server_id() -> int32 override;

  auto system_exit(int exit_code) -> void override;

  [[nodiscard]] auto GetSrvConfig() const -> ptr<srv_config>;

  auto GetCoordinatorToCoordinatorConfigs() const -> std::vector<CoordinatorToCoordinatorConfig>;

 private:
  void NotifyObserver(std::vector<CoordinatorToCoordinatorConfig> const &configs);
  void HandleVersionMigration();
  void TryUpdateClusterConfigFromDisk();

  int my_id_;
  ptr<CoordinatorLogStore> cur_log_store_;
  LoggerWrapper logger_;
  ptr<srv_config> my_srv_config_;
  ptr<cluster_config> cluster_config_;
  ptr<srv_state> saved_state_;
  kvstore::KVStore durability_;
  std::optional<CoordinationClusterChangeObserver> observer_;
};

void from_json(nlohmann::json const &json_cluster_config, ptr<cluster_config> &config);
void to_json(nlohmann::json &j, cluster_config const &cluster_config);

void from_json(nlohmann::json const &json_cluster_config, srv_state &srv_state);
void to_json(nlohmann::json &j, srv_state const &srv_state);

}  // namespace memgraph::coordination
#endif
