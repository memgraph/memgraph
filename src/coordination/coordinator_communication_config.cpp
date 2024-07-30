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

#include "coordination/coordinator_communication_config.hpp"
#include "utils/uuid.hpp"

#include <chrono>
#include <json/json.hpp>
#include <string>

namespace memgraph::coordination {

void to_json(nlohmann::json &j, CoordinatorToCoordinatorConfig const &config) {
  j = nlohmann::json{{"coordinator_id", config.coordinator_id},
                     {"coordinator_server", config.coordinator_server},
                     {"bolt_server", config.bolt_server},
                     {"management_server", config.management_server},
                     {"instance_down_timeout_sec", config.instance_down_timeout_sec.count()},
                     {"coordinator_hostname", config.coordinator_hostname}};
}

void from_json(nlohmann::json const &j, CoordinatorToCoordinatorConfig &config) {
  config.coordinator_id = j.at("coordinator_id").get<uint32_t>();
  config.coordinator_server = j.at("coordinator_server").get<io::network::Endpoint>();
  config.management_server = j.at("management_server").get<io::network::Endpoint>();
  config.bolt_server = j.at("bolt_server").get<io::network::Endpoint>();
  config.instance_down_timeout_sec = std::chrono::seconds{j.at("instance_down_timeout_sec").get<int>()};
  config.coordinator_hostname = j.at("coordinator_hostname").get<std::string>();
}

void to_json(nlohmann::json &j, ReplicationClientInfo const &config) {
  j = nlohmann::json{{"instance_name", config.instance_name},
                     {"replication_mode", config.replication_mode},
                     {"replication_server", config.replication_server}};
}

void from_json(nlohmann::json const &j, ReplicationClientInfo &config) {
  config.instance_name = j.at("instance_name").get<std::string>();
  config.replication_mode = j.at("replication_mode").get<replication_coordination_glue::ReplicationMode>();
  config.replication_server = j.at("replication_server").get<io::network::Endpoint>();
}

void to_json(nlohmann::json &j, CoordinatorToReplicaConfig const &config) {
  j = nlohmann::json{{"instance_name", config.instance_name},
                     {"mgt_server", config.mgt_server},
                     {"bolt_server", config.bolt_server},
                     {"instance_health_check_frequency_sec", config.instance_health_check_frequency_sec.count()},
                     {"instance_down_timeout_sec", config.instance_down_timeout_sec.count()},
                     {"instance_get_uuid_frequency_sec", config.instance_get_uuid_frequency_sec.count()},
                     {"replication_client_info", config.replication_client_info}};
}

void from_json(nlohmann::json const &j, CoordinatorToReplicaConfig &config) {
  config.instance_name = j.at("instance_name").get<std::string>();
  config.mgt_server = j.at("mgt_server").get<io::network::Endpoint>();
  config.bolt_server = j.at("bolt_server").get<io::network::Endpoint>();
  config.instance_health_check_frequency_sec =
      std::chrono::seconds{j.at("instance_health_check_frequency_sec").get<int>()};
  config.instance_down_timeout_sec = std::chrono::seconds{j.at("instance_down_timeout_sec").get<int>()};
  config.instance_get_uuid_frequency_sec = std::chrono::seconds{j.at("instance_get_uuid_frequency_sec").get<int>()};
  config.replication_client_info = j.at("replication_client_info").get<ReplicationClientInfo>();
}

void from_json(nlohmann::json const &j, InstanceUUIDUpdate &instance_uuid_update) {
  instance_uuid_update.uuid = j.at("uuid").get<utils::UUID>();
  instance_uuid_update.instance_name = j.at("instance_name").get<std::string>();
}

void to_json(nlohmann::json &j, InstanceUUIDUpdate const &instance_uuid_update) {
  j = nlohmann::json{{"instance_name", instance_uuid_update.instance_name}, {"uuid", instance_uuid_update.uuid}};
}

}  // namespace memgraph::coordination
#endif
