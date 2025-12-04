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

#include "coordination/coordinator_instance_context.hpp"
#include "coordination/data_instance_context.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/logging.hpp"

#include <map>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/spdlog.h>

module memgraph.coordination.utils;

import memgraph.coordination.log_level;
import memgraph.coordination.logger_wrapper;

#ifdef MG_ENTERPRISE

namespace memgraph::coordination {
auto GetOrSetDefaultVersion(kvstore::KVStore &durability, std::string_view key, int const default_value,
                            LoggerWrapper logger) -> int {
  if (auto const maybe_version = durability.Get(key); maybe_version.has_value()) {
    return std::stoi(maybe_version.value());
  }
  logger.Log(
      nuraft_log_level::INFO,
      fmt::format("Assuming first start of durability as key value {} for version is missing, storing version {}.", key,
                  default_value));
  MG_ASSERT(durability.Put(key, std::to_string(default_value)), "Failed to store durability version.");
  return default_value;
}

auto CreateRoutingTable(std::vector<DataInstanceContext> const &raft_log_data_instances,
                        std::vector<CoordinatorInstanceContext> const &coord_servers,
                        std::function<bool(DataInstanceContext const &)> is_instance_main_func,
                        bool const enabled_reads_on_main, uint64_t const max_replica_read_lag,
                        std::string_view const db_name,
                        std::map<std::string, std::map<std::string, int64_t>> const &replicas_lag) -> RoutingTable {
  auto res = RoutingTable{};

  auto const repl_instance_to_bolt = [](auto const &instance) {
    return instance.config.BoltSocketAddress();  // non-resolved IP
  };

  auto writers = raft_log_data_instances | std::ranges::views::filter(is_instance_main_func) |
                 std::ranges::views::transform(repl_instance_to_bolt) | std::ranges::to<std::vector>();
  MG_ASSERT(writers.size() <= 1, "There can be at most one main instance active!");

  spdlog::trace("WRITERS");
  for (auto const &writer : writers) {
    spdlog::trace("  {}", writer);
  }

  auto const lag_filter = [&max_replica_read_lag, &replicas_lag,
                           str_db_name = std::string{db_name}](auto const &instance) {
    auto const replica_it = replicas_lag.find(instance.config.instance_name);
    // We don't want to forbid routing to the replica if don't have any information cached
    if (replica_it == replicas_lag.end()) {
      return true;
    }

    auto const db_it = replica_it->second.find(str_db_name);
    // We don't want to forbid routing to the replica if don't have any information cached
    if (db_it == replica_it->second.end()) {
      return true;
    }

    // return true if cached lag is smaller than max_allowed_replica_read_lag
    return static_cast<uint64_t>(db_it->second) < max_replica_read_lag;
  };

  auto readers = raft_log_data_instances | std::ranges::views::filter(std::not_fn(is_instance_main_func)) |
                 std::ranges::views::filter(lag_filter) | std::ranges::views::transform(repl_instance_to_bolt) |
                 std::ranges::to<std::vector>();

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

  auto routers = coord_servers | std::ranges::views::transform(get_bolt_server) | std::ranges::to<std::vector>();
  spdlog::trace("ROUTERS:");
  for (auto const &server : routers) {
    spdlog::trace("  {}", server);
  }

  res.emplace_back(std::move(routers), "ROUTE");

  return res;
}

}  // namespace memgraph::coordination

#endif
