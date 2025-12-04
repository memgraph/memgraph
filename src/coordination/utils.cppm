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

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/spdlog.h>

export module memgraph.coordination.utils;

import memgraph.coordination.logger_wrapper;

#ifdef MG_ENTERPRISE

export namespace memgraph::coordination {

using RoutingTable = std::vector<std::pair<std::vector<std::string>, std::string>>;

auto GetOrSetDefaultVersion(kvstore::KVStore &durability, std::string_view key, int default_value, LoggerWrapper logger)
    -> int;

auto CreateRoutingTable(std::vector<DataInstanceContext> const &raft_log_data_instances,
                        std::vector<CoordinatorInstanceContext> const &coord_servers,
                        std::function<bool(DataInstanceContext const &)> is_instance_main_func,
                        bool enabled_reads_on_main, uint64_t max_replica_read_lag, std::string_view db_name,
                        std::map<std::string, std::map<std::string, int64_t>> const &replicas_lag) -> RoutingTable;

}  // namespace memgraph::coordination

#endif
