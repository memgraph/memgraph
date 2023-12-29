// Copyright 2023 Memgraph Ltd.
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

#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

using namespace memgraph::replication::durability;
using namespace memgraph::replication;

TEST(ReplicationDurability, V1Main) {
  auto const role_entry =
#ifdef MG_ENTERPRISE
      ReplicationRoleEntry{.version = DurabilityVersion::V1,
                           .role = MainRole{
                               .epoch = ReplicationEpoch{"TEST_STRING"},
                               .config = ReplicationServerConfig{.ip_address = "000.123.456.789", .port = 2023},
                           }};
#else
      ReplicationRoleEntry{.version = DurabilityVersion::V1,
                           .role = MainRole{.epoch = ReplicationEpoch{"TEST_STRING"}}};
#endif
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}

TEST(ReplicationDurability, V2Main) {
  auto const role_entry =
#ifdef MG_ENTERPRISE
      ReplicationRoleEntry{.version = DurabilityVersion::V2,
                           .role = MainRole{
                               .epoch = ReplicationEpoch{"TEST_STRING"},
                               .config = ReplicationServerConfig{.ip_address = "000.123.456.789", .port = 2023},
                           }};
#else
      ReplicationRoleEntry{.version = DurabilityVersion::V2,
                           .role = MainRole{.epoch = ReplicationEpoch{"TEST_STRING"}}};
#endif
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

#ifdef MG_ENTERPRISE
TEST(ReplicationDurability, V2Coordinator) {
  auto const role_entry = ReplicationRoleEntry{.version = DurabilityVersion::V2, .role = CoordinatorRole{}};
  nlohmann::json j;
  to_json(j, role_entry);
  ReplicationRoleEntry deser;
  from_json(j, deser);
  ASSERT_EQ(role_entry, deser);
}
#endif

TEST(ReplicationDurability, ReplicaClientConfigEntrySync) {
  using namespace std::chrono_literals;
  using namespace std::string_literals;
  auto const replica_entry = ReplicationClientConfigEntry{.config = ReplicationClientConfig{
                                                              .name = "TEST_NAME"s,
                                                              .mode = ReplicationMode::SYNC,
                                                              .ip_address = "000.123.456.789"s,
                                                              .port = 2023,
                                                              .check_frequency = 3s,
                                                          }};
  nlohmann::json j;
  to_json(j, replica_entry);
  ReplicationClientConfigEntry deser;
  from_json(j, deser);
  ASSERT_EQ(replica_entry, deser);
}

TEST(ReplicationDurability, ReplicaClientConfigEntryAsync) {
  using namespace std::chrono_literals;
  using namespace std::string_literals;
  auto const replica_entry = ReplicationClientConfigEntry{.config = ReplicationClientConfig{
                                                              .name = "TEST_NAME"s,
                                                              .mode = ReplicationMode::ASYNC,
                                                              .ip_address = "000.123.456.789"s,
                                                              .port = 2023,
                                                              .check_frequency = 3s,
                                                          }};
  nlohmann::json j;
  to_json(j, replica_entry);
  ReplicationClientConfigEntry deser;
  from_json(j, deser);
  ASSERT_EQ(replica_entry, deser);
}
