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

namespace memgraph::coordination {

using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;

CoordinatorStateManager::CoordinatorStateManager(CoordinatorInstanceInitConfig const &config)
    : my_id_(static_cast<int>(config.coordinator_id)), cur_log_store_(cs_new<CoordinatorLogStore>()) {
  auto const c2c =
      CoordinatorToCoordinatorConfig{config.coordinator_id, io::network::Endpoint("0.0.0.0", config.bolt_port),
                                     io::network::Endpoint{"0.0.0.0", static_cast<uint16_t>(config.coordinator_port)}};
  my_srv_config_ = cs_new<srv_config>(config.coordinator_id, 0, c2c.coordinator_server.SocketAddress(),
                                      nlohmann::json(c2c).dump(), false);

  cluster_config_ = cs_new<cluster_config>();
  cluster_config_->get_servers().push_back(my_srv_config_);
}

auto CoordinatorStateManager::load_config() -> ptr<cluster_config> {
  // Just return in-memory data in this example.
  // May require reading from disk here, if it has been written to disk.
  spdlog::trace("Loading cluster config");
  return cluster_config_;
}

auto CoordinatorStateManager::save_config(cluster_config const &config) -> void {
  // Just keep in memory in this example.
  // Need to write to disk here, if want to make it durable.
  ptr<buffer> buf = config.serialize();
  cluster_config_ = cluster_config::deserialize(*buf);
  spdlog::info("Saving cluster config.");
  auto servers = cluster_config_->get_servers();
  for (auto const &server : servers) {
    spdlog::trace("Server id: {}, endpoint: {}", server->get_id(), server->get_endpoint());
  }
}

auto CoordinatorStateManager::save_state(srv_state const &state) -> void {
  // Just keep in memory in this example.
  // Need to write to disk here, if want to make it durable.
  spdlog::trace("save state");
  ptr<buffer> buf = state.serialize();
  saved_state_ = srv_state::deserialize(*buf);
}

auto CoordinatorStateManager::read_state() -> ptr<srv_state> {
  // Just return in-memory data in this example.
  // May require reading from disk here, if it has been written to disk.
  spdlog::trace("read state");
  return saved_state_;
}

auto CoordinatorStateManager::load_log_store() -> ptr<log_store> { return cur_log_store_; }

auto CoordinatorStateManager::server_id() -> int32 { return my_id_; }

auto CoordinatorStateManager::system_exit(int const exit_code) -> void {}

auto CoordinatorStateManager::GetSrvConfig() const -> ptr<srv_config> { return my_srv_config_; }

}  // namespace memgraph::coordination
#endif
