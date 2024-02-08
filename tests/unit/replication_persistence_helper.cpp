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

#include "formatters.hpp"
#include "replication/state.hpp"
#include "replication/status.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

using namespace memgraph::replication::durability;
using namespace memgraph::replication;
using namespace memgraph::replication_coordination_glue;

TEST(ReplicationDurability, V1Main) {
  auto const role_entry = ReplicationRoleEntry{.version = DurabilityVersion::V1,
                                               .role = MainRole{
                                                   .epoch = ReplicationEpoch{"TEST_STRING"},
                                               }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V2Main) {
  auto const role_entry = ReplicationRoleEntry{.version = DurabilityVersion::V2,
                                               .role = MainRole{
                                                   .epoch = ReplicationEpoch{"TEST_STRING"},
                                               }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V3Main) {
  auto const role_entry = ReplicationRoleEntry{
      .version = DurabilityVersion::V3,
      .role = MainRole{.epoch = ReplicationEpoch{"TEST_STRING"}, .main_uuid = memgraph::utils::UUID{}}};
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
                               .config = ReplicationServerConfig{.ip_address = "000.123.456.789", .port = 2023},
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
                               .config = ReplicationServerConfig{.ip_address = "000.123.456.789", .port = 2023},
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
                               .config = ReplicationServerConfig{.ip_address = "000.123.456.789", .port = 2023},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V3ReplicaMain) {
  auto const role_entry =
      ReplicationRoleEntry{.version = DurabilityVersion::V2,
                           .role = ReplicaRole{
                               .config = ReplicationServerConfig{.ip_address = "000.123.456.789", .port = 2023},
                               .main_uuid = memgraph::utils::UUID{},
                           }};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, ReplicaEntrySync) {
  using namespace std::chrono_literals;
  using namespace std::string_literals;
  auto const replica_entry = ReplicationReplicaEntry{.config = ReplicationClientConfig{
                                                         .name = "TEST_NAME"s,
                                                         .mode = ReplicationMode::SYNC,
                                                         .ip_address = "000.123.456.789"s,
                                                         .port = 2023,
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
                                                         .ip_address = "000.123.456.789"s,
                                                         .port = 2023,
                                                         .replica_check_frequency = 3s,
                                                     }};
  nlohmann::json j;
  to_json(j, replica_entry);
  ReplicationReplicaEntry deser;
  from_json(j, deser);
  ASSERT_EQ(replica_entry, deser);
}
