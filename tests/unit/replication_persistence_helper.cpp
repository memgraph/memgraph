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

#include "formatters.hpp"
#include "replication/state.hpp"
#include "replication/status.hpp"
#include "utils/uuid.hpp"

#include <gtest/gtest.h>
#include <iostream>
#include <optional>
#include <string>

using namespace memgraph::replication::durability;
using namespace memgraph::replication;
using namespace memgraph::replication_coordination_glue;
using namespace memgraph::io::network;
using memgraph::utils::UUID;

TEST(ReplicationDurability, V3Main) {
  auto const role_entry = ReplicationRoleEntry{.version = DurabilityVersion::V3, .role = MainRole{.main_uuid = UUID{}}};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V1Replica) {
  auto const role_entry =
      ReplicationRoleEntry{.version = DurabilityVersion::V1,
                           .role = ReplicaRole{
                               .config = ReplicationServerConfig{.repl_server = Endpoint("000.123.456.789", 2023)},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V2Replica) {
  auto const role_entry =
      ReplicationRoleEntry{.version = DurabilityVersion::V2,
                           .role = ReplicaRole{
                               .config = ReplicationServerConfig{.repl_server = Endpoint("000.123.456.789", 2023)},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V3ReplicaNoMain) {
  auto const role_entry =
      ReplicationRoleEntry{.version = DurabilityVersion::V3,
                           .role = ReplicaRole{
                               .config = ReplicationServerConfig{.repl_server = Endpoint("000.123.456.789", 2023)},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V3ReplicaMain) {
  auto const role_entry =
      ReplicationRoleEntry{.version = DurabilityVersion::V3,
                           .role = ReplicaRole{
                               .config = ReplicationServerConfig{.repl_server = Endpoint("000.123.456.789", 2023)},
                               .main_uuid = memgraph::utils::UUID{},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V4Replica) {
  auto const role_entry =
      ReplicationRoleEntry{.version = DurabilityVersion::V4,
                           .role = ReplicaRole{
                               .config = ReplicationServerConfig{.repl_server = Endpoint("memgraph.dns.example", 2023)},
                               .main_uuid = memgraph::utils::UUID{},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V4Main) {
  auto const role_entry = ReplicationRoleEntry{.version = DurabilityVersion::V4,
                                               .role = MainRole{
                                                   .main_uuid = memgraph::utils::UUID{},
                                               }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, HandleMigrationV3ToV4ReplicationRoleEntry) {
  auto json = nlohmann::json{{"durability_version", DurabilityVersion::V3},
                             {"replication_role", memgraph::replication_coordination_glue::ReplicationRole::REPLICA},
                             {"replica_ip_address", "000.123.456.789"},
                             {"replica_port", 2023}};

  ReplicationRoleEntry role_entry;
  from_json(json, role_entry);
  ASSERT_EQ(role_entry.version, DurabilityVersion::V3);
  ASSERT_EQ(std::get<ReplicaRole>(role_entry.role).config.repl_server.GetAddress(), "000.123.456.789");
  ASSERT_EQ(std::get<ReplicaRole>(role_entry.role).config.repl_server.GetPort(), 2023);
}

TEST(ReplicationDurability, ReplicaEntrySync) {
  using namespace std::chrono_literals;
  using namespace std::string_literals;
  auto const replica_entry = ReplicationReplicaEntry{.config = ReplicationClientConfig{
                                                         .name = "TEST_NAME"s,
                                                         .mode = ReplicationMode::SYNC,
                                                         .repl_server_endpoint = Endpoint("000.123.456.789", 2023),
                                                         .replica_check_frequency = 3s,
                                                     }};
  nlohmann::json j;
  to_json(j, replica_entry);
  ReplicationReplicaEntry deser;
  from_json(j, deser);
  ASSERT_EQ(replica_entry, deser);
}

TEST(ReplicationDurability, ReplicaEntryAsync) {
  using namespace std::chrono_literals;
  using namespace std::string_literals;
  auto const replica_entry = ReplicationReplicaEntry{.config = ReplicationClientConfig{
                                                         .name = "TEST_NAME"s,
                                                         .mode = ReplicationMode::ASYNC,
                                                         .repl_server_endpoint = Endpoint("000.123.456.789", 2023),
                                                         .replica_check_frequency = 3s,
                                                     }};
  nlohmann::json j;
  to_json(j, replica_entry);
  ReplicationReplicaEntry deser;
  from_json(j, deser);
  ASSERT_EQ(replica_entry, deser);
}

TEST(ReplicationDurability, ReplicaEntryMigrationNoVersionToV4) {
  using namespace std::chrono_literals;
  using namespace std::string_literals;
  nlohmann::json j;
  j["replica_name"] = "TEST_NAME";
  j["replica_sync_mode"] = ReplicationMode::ASYNC;
  j["replica_ip_address"] = "000.123.456.789";
  j["replica_port"] = 2023;
  j["replica_check_frequency"] = 3;
  j["replica_ssl_key_file"] = nullptr;
  j["replica_ssl_cert_file"] = nullptr;
  ReplicationReplicaEntry deser;
  from_json(j, deser);
  // Default version is V3
  ASSERT_EQ(deser.config.name, "TEST_NAME");
  ASSERT_EQ(deser.config.mode, ReplicationMode::ASYNC);
  ASSERT_EQ(deser.config.repl_server_endpoint.GetAddress(), "000.123.456.789");
  ASSERT_EQ(deser.config.repl_server_endpoint.GetPort(), 2023);
  ASSERT_EQ(deser.config.replica_check_frequency, 3s);
}
