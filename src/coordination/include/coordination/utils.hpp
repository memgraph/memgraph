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

#pragma once

#include "coordination/coordinator_instance_context.hpp"
#include "coordination/data_instance_context.hpp"
#include "coordination/logger_wrapper.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/logging.hpp"

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>

#ifdef MG_ENTERPRISE

namespace memgraph::coordination {
auto GetOrSetDefaultVersion(kvstore::KVStore &durability, std::string_view key, int default_value, LoggerWrapper logger)
    -> int;

using RoutingTable = std::vector<std::pair<std::vector<std::string>, std::string>>;

auto CreateRoutingTable(std::vector<DataInstanceContext> const &raft_log_data_instances,
                        std::vector<CoordinatorInstanceContext> const &coord_servers, auto const &is_instance_main_func,
                        bool const enabled_reads_on_main, uint64_t const max_replica_read_lag,
                        std::string const &db_name,
                        std::map<std::string, std::map<std::string, int64_t>> const &replicas_lag) {
  auto res = RoutingTable{};

  auto const repl_instance_to_bolt = [](auto const &instance) {
    return instance.config.BoltSocketAddress();  // non-resolved IP
  };

  auto writers = raft_log_data_instances | ranges::views::filter(is_instance_main_func) |
                 ranges::views::transform(repl_instance_to_bolt) | ranges::to_vector;
  MG_ASSERT(writers.size() <= 1, "There can be at most one main instance active!");

  spdlog::trace("WRITERS");
  for (auto const &writer : writers) {
    spdlog::trace("  {}", writer);
  }

  auto const lag_filter = [&max_replica_read_lag, &replicas_lag, &db_name](auto const &instance) {
    auto const replica_it = std::find(replicas_lag.begin(), replicas_lag.end(), instance.config.instance_name);
    MG_ASSERT(replica_it != replicas_lag.end(), "Couldn't find instance {} in Raft log when creating the routing table",
              instance.config.instance_name);

    auto const db_it = std::find(replica_it.second.begin(), replica_it.second.end(), db_name);
  };

  auto readers = raft_log_data_instances | ranges::views::filter(std::not_fn(is_instance_main_func)) |
                 ranges::views::transform(repl_instance_to_bolt) | ranges::to_vector;

  if (enabled_reads_on_main && writers.size() == 1) {
    readers.emplace_back(writers[0]);
  }

  spdlog::trace("READERS:");
  for (auto const &reader : readers) {
    spdlog::trace("  {}", reader);
  }

  if (!std::ranges::empty(writers)) {
    res.emplace_back(std::move(writers), "WRITE");
  }

  if (!std::ranges::empty(readers)) {
    res.emplace_back(std::move(readers), "READ");
  }

  auto const get_bolt_server = [](CoordinatorInstanceContext const &context) { return context.bolt_server; };

  auto routers = coord_servers | ranges::views::transform(get_bolt_server) | ranges::to_vector;
  spdlog::trace("ROUTERS:");
  for (auto const &server : routers) {
    spdlog::trace("  {}", server);
  }

  res.emplace_back(std::move(routers), "ROUTE");

  return res;
}

}  // namespace memgraph::coordination

#endif
