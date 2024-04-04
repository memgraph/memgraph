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
#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

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
  utils::EnsureDirOrDie(config.durability_dir);
  kv_store_ = std::make_unique<kvstore::KVStore>(config.durability_dir);
}

auto CoordinatorStateManager::load_config() -> ptr<cluster_config> {
  MG_ASSERT(kv_store_, "Durability folder should be created.");
  spdlog::trace("Loading cluster config from RocksDb");
  auto servers = kv_store_->Get("servers");
  if (!servers.has_value()) {
    spdlog::trace("Didn't find anything stored on disk for cluster config.");
    return cluster_config_;
  }
  spdlog::trace("Loading cluster config from disk.");
  auto const json = nlohmann::json::parse(servers.value());
  auto const real_servers = json.get<std::vector<std::tuple<int, std::string, std::string>>>();
  cluster_config_->get_servers().clear();
  for (auto const &real_server : real_servers) {
    auto const &[coord_id, endpoint, aux] = real_server;
    spdlog::trace("Recreating cluster config with id: {}, endpoint: {} and aux data: {} from disk.", coord_id, endpoint,
                  aux);
    auto one_server_config = cs_new<srv_config>(coord_id, 0, endpoint, aux, false);
    cluster_config_->get_servers().push_back(std::move(one_server_config));
  }

  return cluster_config_;
}

auto CoordinatorStateManager::save_config(cluster_config const &config) -> void {
  MG_ASSERT(kv_store_, "Disk folder should be created");
  ptr<buffer> buf = config.serialize();
  cluster_config_ = cluster_config::deserialize(*buf);
  spdlog::info("Saving cluster config to disk.");
  auto servers = cluster_config_->get_servers();
  std::vector<std::tuple<int, std::string, std::string>> servers_vec;
  for (auto const &server : servers) {
    servers_vec.emplace_back(static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux());
    spdlog::trace("Stored cluster config with id: {}, endpoint: {} and aux data: {} to disk.",
                  static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux());
  }
  nlohmann::json json(servers_vec);
  kv_store_->Put("servers", json.dump());
}

auto CoordinatorStateManager::save_state(srv_state const &state) -> void {
  // TODO(antoniofilipovic): Implement storing of server state to disk. For now
  // as server state is just term and voted_for, we don't have to store it
  spdlog::trace("Saving server state in coordinator state manager.");
  ptr<buffer> buf = state.serialize();
  saved_state_ = srv_state::deserialize(*buf);
}

auto CoordinatorStateManager::read_state() -> ptr<srv_state> {
  spdlog::trace("Reading server state in coordinator state manager.");
  return saved_state_;
}

auto CoordinatorStateManager::load_log_store() -> ptr<log_store> { return cur_log_store_; }

auto CoordinatorStateManager::server_id() -> int32 { return my_id_; }

auto CoordinatorStateManager::system_exit(int const exit_code) -> void {}

auto CoordinatorStateManager::GetSrvConfig() const -> ptr<srv_config> { return my_srv_config_; }

}  // namespace memgraph::coordination
#endif
