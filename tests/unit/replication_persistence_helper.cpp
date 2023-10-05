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

#include "storage/v2/replication/replication_persistence_helper.hpp"
#include "formatters.hpp"
#include "replication/replication_state.hpp"
#include "utils/logging.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

using namespace memgraph::storage::replication;
using memgraph::replication::ReplicationRole;

class ReplicationPersistanceHelperTest : public testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  ReplicationStatus CreateReplicationStatus(std::string name, std::string ip_address, uint16_t port,
                                            ReplicationMode sync_mode, std::chrono::seconds replica_check_frequency,
                                            std::optional<ReplicationClientConfig::SSL> ssl,
                                            std::optional<ReplicationRole> role) const {
    return ReplicationStatus{.name = name,
                             .ip_address = ip_address,
                             .port = port,
                             .sync_mode = sync_mode,
                             .replica_check_frequency = replica_check_frequency,
                             .ssl = ssl,
                             .role = role};
  }

  static_assert(
      sizeof(ReplicationStatus) == 160,
      "Most likely you modified ReplicationStatus without updating the tests. Please modify CreateReplicationStatus. ");
};

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesInitialized) {
  auto replicas_status = CreateReplicationStatus(
      "name", "ip_address", 0, ReplicationMode::SYNC, std::chrono::seconds(1),
      ReplicationClientConfig::SSL{.key_file = "key_file", .cert_file = "cert_file"}, ReplicationRole::REPLICA);

  auto json_status = ReplicationStatusToJSON(ReplicationStatus(replicas_status));
  auto replicas_status_converted = JSONToReplicationStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestOnlyMandatoryAttributesInitialized) {
  auto replicas_status = CreateReplicationStatus("name", "ip_address", 0, ReplicationMode::SYNC,
                                                 std::chrono::seconds(1), std::nullopt, std::nullopt);

  auto json_status = ReplicationStatusToJSON(ReplicationStatus(replicas_status));
  auto replicas_status_converted = JSONToReplicationStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesButSSLInitialized) {
  auto replicas_status = CreateReplicationStatus("name", "ip_address", 0, ReplicationMode::SYNC,
                                                 std::chrono::seconds(1), std::nullopt, ReplicationRole::MAIN);

  auto json_status = ReplicationStatusToJSON(ReplicationStatus(replicas_status));
  auto replicas_status_converted = JSONToReplicationStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesButTimeoutInitialized) {
  auto replicas_status = CreateReplicationStatus(
      "name", "ip_address", 0, ReplicationMode::SYNC, std::chrono::seconds(1),
      ReplicationClientConfig::SSL{.key_file = "key_file", .cert_file = "cert_file"}, ReplicationRole::REPLICA);

  auto json_status = ReplicationStatusToJSON(ReplicationStatus(replicas_status));
  auto replicas_status_converted = JSONToReplicationStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesButReplicationRoleInitialized) {
  // this one is importand for backwards compatibility
  auto replicas_status = CreateReplicationStatus(
      "name", "ip_address", 0, ReplicationMode::SYNC, std::chrono::seconds(1),
      ReplicationClientConfig::SSL{.key_file = "key_file", .cert_file = "cert_file"}, std::nullopt);

  auto json_status = ReplicationStatusToJSON(ReplicationStatus(replicas_status));
  auto replicas_status_converted = JSONToReplicationStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}
