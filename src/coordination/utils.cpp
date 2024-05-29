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

#include <utility>

#include <spdlog/spdlog.h>
#include <json/json.hpp>
#include <libnuraft/nuraft.hxx>
#include <range/v3/view.hpp>

#include "utils.hpp"

using nuraft::async_result;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::int32;
using nuraft::ptr;
using nuraft::snapshot;
using nuraft::srv_config;
using nuraft::state_machine;

namespace {
constexpr std::string_view kServers = "servers";
constexpr std::string_view kPrevLogIdx = "prev_log_idx";
constexpr std::string_view kLogIdx = "log_idx";
constexpr std::string_view kAsyncReplication = "async_replication";
constexpr std::string_view kUserCtx = "user_ctx";

}  // namespace

namespace memgraph::coordination {

auto DeserializeClusterConfig(nlohmann::json const &json_cluster_config) -> ptr<cluster_config> {
  auto servers = json_cluster_config.at(kServers.data()).get<std::vector<std::tuple<int, std::string, std::string>>>();

  auto prev_log_idx = json_cluster_config.at(kPrevLogIdx.data()).get<int64_t>();
  auto log_idx = json_cluster_config.at(kLogIdx.data()).get<int64_t>();
  auto async_replication = json_cluster_config.at(kAsyncReplication.data()).get<bool>();
  auto user_ctx = json_cluster_config.at(kUserCtx.data()).get<std::string>();
  auto new_cluster_config = cs_new<cluster_config>(log_idx, prev_log_idx, async_replication);
  new_cluster_config->set_user_ctx(user_ctx);
  for (auto &real_server : servers) {
    auto &[coord_id, endpoint, aux] = real_server;
    spdlog::trace("Recreating cluster config with id: {}, endpoint: {} and aux data: {} from disk.", coord_id, endpoint,
                  aux);
    auto one_server_config = cs_new<srv_config>(coord_id, 0, std::move(endpoint), std::move(aux), false);
    new_cluster_config->get_servers().push_back(std::move(one_server_config));
  }
  return new_cluster_config;
}

auto SerializeClusterConfig(cluster_config const &cluster_config) -> nlohmann::json {
  auto const servers_vec =
      ranges::views::transform(
          cluster_config.get_servers(),
          [](auto const &server) {
            spdlog::trace("Created cluster config with id: {}, endpoint: {} and aux data: {} to disk.",
                          static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux());
            return std::tuple{static_cast<int>(server->get_id()), server->get_endpoint(), server->get_aux()};
          }) |
      ranges::to<std::vector>();
  auto json = nlohmann::json{{kServers, servers_vec},
                             {kPrevLogIdx, cluster_config.get_prev_log_idx()},
                             {kLogIdx, cluster_config.get_log_idx()},
                             {kAsyncReplication, cluster_config.is_async_replication()},
                             {kUserCtx, cluster_config.get_user_ctx()}};
  return json;
}
}  // namespace memgraph::coordination
