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

namespace memgraph::coordination {

auto DeserializeClusterConfig(nlohmann::json const &json_cluster_config) -> ptr<cluster_config>;
auto SerializeClusterConfig(cluster_config const &config) -> nlohmann::json;

}  // namespace memgraph::coordination
