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

module;

#include "libnuraft/nuraft.hxx"
#include "nlohmann/json_fwd.hpp"

#include "kvstore/kvstore.hpp"

export module memgraph.coordination.coordinator_state_manager;

#ifdef MG_ENTERPRISE

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_instance_aux;
import memgraph.coordination.logger_wrapper;
import memgraph.coordination.coordinator_log_store;
import memgraph.coordination.coordinator_observer;

namespace memgraph::coordination {
using nuraft::buffer;
using nuraft::cluster_config;
using nuraft::int32;
using nuraft::log_store;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;
}  // namespace memgraph::coordination

export namespace memgraph::coordination {

class CoordinatorStateManager final : public state_mgr {
 public:
  explicit CoordinatorStateManager(CoordinatorStateManagerConfig const &config, LoggerWrapper logger,
                                   std::optional<CoordinationClusterChangeObserver> const &observer = {});

  CoordinatorStateManager(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager(CoordinatorStateManager &&) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager &&) = delete;
  ~CoordinatorStateManager() override = default;

  // Goes over all connected servers and returns aux field parsed as `CoordinatorInstanceAux`.
  auto GetCoordinatorInstancesAux() const -> std::vector<CoordinatorInstanceAux>;

  auto load_config() -> std::shared_ptr<cluster_config> override;

  auto save_config(cluster_config const &config) -> void override;

  auto save_state(srv_state const &state) -> void override;

  auto read_state() -> std::shared_ptr<srv_state> override;

  auto load_log_store() -> std::shared_ptr<log_store> override;

  auto server_id() -> int32 override;

  auto system_exit(int exit_code) -> void override;

  [[nodiscard]] auto GetSrvConfig() const -> std::shared_ptr<srv_config>;

 private:
  void NotifyObserver(std::vector<CoordinatorInstanceAux> const &coord_instances_aux) const;

  void HandleVersionMigration();

  void TryUpdateClusterConfigFromDisk();

  int32_t my_id_;
  std::shared_ptr<CoordinatorLogStore> cur_log_store_;
  LoggerWrapper logger_;
  std::shared_ptr<srv_config> my_srv_config_;
  std::shared_ptr<cluster_config> cluster_config_;
  std::shared_ptr<srv_state> saved_state_;
  kvstore::KVStore durability_;
  std::optional<CoordinationClusterChangeObserver> observer_;
};

void from_json(nlohmann::json const &json_cluster_config, std::shared_ptr<cluster_config> &config);

void to_json(nlohmann::json &j, cluster_config const &cluster_config);

void from_json(nlohmann::json const &json_cluster_config, srv_state &srv_state);

void to_json(nlohmann::json &j, srv_state const &srv_state);
}  // namespace memgraph::coordination
#endif
