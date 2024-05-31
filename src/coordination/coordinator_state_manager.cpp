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

namespace memgraph::coordination {

using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;

namespace {
constexpr std::string_view kServersKey = "servers";  // Key prefix for servers durability
}  // namespace

CoordinatorStateManager::CoordinatorStateManager(CoordinatorInstanceInitConfig const &config, ptr<logger> logger)
    : my_id_(static_cast<int>(config.coordinator_id)),
      cur_log_store_(cs_new<CoordinatorLogStore>()),
      logger_(std::move(logger)),
      kv_store_(config.durability_dir) {
  auto const c2c =
      CoordinatorToCoordinatorConfig{config.coordinator_id, io::network::Endpoint("0.0.0.0", config.bolt_port),
                                     io::network::Endpoint{"0.0.0.0", static_cast<uint16_t>(config.coordinator_port)}};
  my_srv_config_ = cs_new<srv_config>(config.coordinator_id, 0, c2c.coordinator_server.SocketAddress(),
                                      nlohmann::json(c2c).dump(), false);  // non-resolved IP in SocketAddress

  cluster_config_ = cs_new<cluster_config>();
  cluster_config_->get_servers().push_back(my_srv_config_);
}

auto CoordinatorStateManager::load_config() -> ptr<cluster_config> {
  logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__, "Loading cluster config from RocksDb");
  auto const servers = kv_store_.Get(kServersKey);
  if (!servers.has_value()) {
    logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__,
                         "Didn't find anything stored on disk for cluster config.");
    return cluster_config_;
  }
  auto const json = nlohmann::json::parse(servers.value());
  auto real_servers = json.get<std::vector<std::tuple<int, std::string, std::string>>>();
  auto new_cluster_config = cs_new<cluster_config>(cluster_config_->get_log_idx(), cluster_config_->get_prev_log_idx());

  for (auto &real_server : real_servers) {
    auto &[coord_id, endpoint, aux] = real_server;
    logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__,
                         fmt::format("Recreating cluster config with id: {}, endpoint: {} and aux data: {} from disk.",
                                     coord_id, endpoint, aux));
    auto one_server_config = cs_new<srv_config>(coord_id, 0, std::move(endpoint), std::move(aux), false);
    new_cluster_config->get_servers().push_back(std::move(one_server_config));
  }
  cluster_config_ = new_cluster_config;
  logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__, "Loaded all cluster configs from RocksDb");
  return cluster_config_;
}

auto CoordinatorStateManager::save_config(cluster_config const &config) -> void {
  ptr<buffer> buf = config.serialize();
  cluster_config_ = cluster_config::deserialize(*buf);
  logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__, "Saving cluster config to RocksDb");
  auto const servers_vec =
      ranges::views::transform(
          cluster_config_->get_servers(),
          [this](auto const &server) {
            logger_->put_details(
                6, __FILE__, "CoordinatorStateManager::save_config", __LINE__,
                fmt::format("Creating cluster config with id: {}, endpoint: {} and aux data: {} to disk.",
                            static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux()));
            return std::tuple{static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux()};
          }) |
      ranges::to<std::vector>();
  kv_store_.Put(kServersKey, nlohmann::json(servers_vec).dump());
}

auto CoordinatorStateManager::save_state(srv_state const &state) -> void {
  // TODO(antoniofilipovic): Implement storing of server state to disk. For now
  // as server state is just term and voted_for, we don't have to store it
  logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__, "Saving server state in coordinator state manager.");
  ptr<buffer> buf = state.serialize();
  saved_state_ = srv_state::deserialize(*buf);
}

auto CoordinatorStateManager::read_state() -> ptr<srv_state> {
  logger_->put_details(6, __FILE__, __FUNCTION__, __LINE__, "Reading server state in coordinator state manager.");
  return saved_state_;
}

auto CoordinatorStateManager::load_log_store() -> ptr<log_store> { return cur_log_store_; }

auto CoordinatorStateManager::server_id() -> int32 { return my_id_; }

auto CoordinatorStateManager::system_exit(int const exit_code) -> void {}

auto CoordinatorStateManager::GetSrvConfig() const -> ptr<srv_config> { return my_srv_config_; }

}  // namespace memgraph::coordination
#endif
