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

#ifdef MG_ENTERPRISE

#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "utils/uuid.hpp"

#include <nlohmann/json.hpp>
#include <string>

#endif

module memgraph.coordination.coordinator_communication_config;

import memgraph.coordination.constants;

#ifdef MG_ENTERPRISE

namespace memgraph::coordination {

void to_json(nlohmann::json &j, CoordinatorInstanceConfig const &config) {
  j = nlohmann::json{{kCoordinatorId, config.coordinator_id},
                     {kCoordinatorServer, config.coordinator_server},
                     {kBoltServer, config.bolt_server},
                     {kManagementServer, config.management_server},
                     {kCoordinatorHostname, config.coordinator_hostname}};
}

void from_json(nlohmann::json const &j, CoordinatorInstanceConfig &config) {
  config.coordinator_id = j.at(kCoordinatorId).get<int32_t>();
  config.coordinator_server = j.at(kCoordinatorServer).get<io::network::Endpoint>();
  config.management_server = j.at(kManagementServer).get<io::network::Endpoint>();
  config.bolt_server = j.at(kBoltServer).get<io::network::Endpoint>();
  config.coordinator_hostname = j.at(kCoordinatorHostname).get<std::string>();
}

void to_json(nlohmann::json &j, ReplicationClientInfo const &config) {
  j = nlohmann::json{{kInstanceName, config.instance_name},
                     {kReplicationMode, config.replication_mode},
                     {kReplicationServer, config.replication_server}};
}

void from_json(nlohmann::json const &j, ReplicationClientInfo &config) {
  config.instance_name = j.at(kInstanceName).get<std::string>();
  config.replication_mode = j.at(kReplicationMode).get<replication_coordination_glue::ReplicationMode>();
  config.replication_server = j.at(kReplicationServer).get<io::network::Endpoint>();
}

void to_json(nlohmann::json &j, DataInstanceConfig const &config) {
  j = nlohmann::json{{kInstanceName, config.instance_name},
                     {kMgtServer, config.mgt_server},
                     {kBoltServer, config.bolt_server},
                     {kReplicationClientInfo, config.replication_client_info}};
}

void from_json(nlohmann::json const &j, DataInstanceConfig &config) {
  config.instance_name = j.at(kInstanceName).get<std::string>();
  config.mgt_server = j.at(kMgtServer).get<io::network::Endpoint>();
  config.bolt_server = j.at(kBoltServer).get<io::network::Endpoint>();
  config.replication_client_info = j.at(kReplicationClientInfo).get<ReplicationClientInfo>();
}

void from_json(nlohmann::json const &j, InstanceUUIDUpdate &instance_uuid_update) {
  instance_uuid_update.uuid = j.at(kUuid).get<utils::UUID>();
  instance_uuid_update.instance_name = j.at(kInstanceName).get<std::string>();
}

void to_json(nlohmann::json &j, InstanceUUIDUpdate const &instance_uuid_update) {
  j = nlohmann::json{{kInstanceName, instance_uuid_update.instance_name}, {kUuid, instance_uuid_update.uuid}};
}

}  // namespace memgraph::coordination
#endif
