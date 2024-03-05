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

#include "coordination/coordinator_config.hpp"

namespace memgraph::coordination {

void to_json(nlohmann::json &j, ReplClientInfo const &config) {
  j = nlohmann::json{{"instance_name", config.instance_name},
                     {"replication_mode", config.replication_mode},
                     {"replication_ip_address", config.replication_ip_address},
                     {"replication_port", config.replication_port}};
}

void from_json(nlohmann::json const &j, ReplClientInfo &config) {
  config.instance_name = j.at("instance_name").get<std::string>();
  config.replication_mode = j.at("replication_mode").get<replication_coordination_glue::ReplicationMode>();
  config.replication_ip_address = j.at("replication_ip_address").get<std::string>();
  config.replication_port = j.at("replication_port").get<uint16_t>();
}

void to_json(nlohmann::json &j, CoordinatorClientConfig const &config) {
  j = nlohmann::json{{"instance_name", config.instance_name},
                     {"ip_address", config.ip_address},
                     {"port", config.port},
                     {"instance_health_check_frequency_sec", config.instance_health_check_frequency_sec.count()},
                     {"instance_down_timeout_sec", config.instance_down_timeout_sec.count()},
                     {"instance_get_uuid_frequency_sec", config.instance_get_uuid_frequency_sec.count()},
                     {"replication_client_info", config.replication_client_info}};
}

void from_json(nlohmann::json const &j, CoordinatorClientConfig &config) {
  config.instance_name = j.at("instance_name").get<std::string>();
  config.ip_address = j.at("ip_address").get<std::string>();
  config.port = j.at("port").get<uint16_t>();
  config.instance_health_check_frequency_sec =
      std::chrono::seconds{j.at("instance_health_check_frequency_sec").get<int>()};
  config.instance_down_timeout_sec = std::chrono::seconds{j.at("instance_down_timeout_sec").get<int>()};
  config.instance_get_uuid_frequency_sec = std::chrono::seconds{j.at("instance_get_uuid_frequency_sec").get<int>()};
  config.replication_client_info = j.at("replication_client_info").get<ReplClientInfo>();
}

}  // namespace memgraph::coordination
#endif
