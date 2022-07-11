// Copyright 2022 Memgraph Ltd.
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
#include "utils/logging.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

class ReplicationPersistanceHelperTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  memgraph::storage::replication::ReplicaStatus CreateReplicaStatus(
      std::string name, std::string ip_address, uint16_t port,
      memgraph::storage::replication::ReplicationMode sync_mode, std::chrono::seconds replica_check_frequency,
      std::optional<memgraph::storage::replication::ReplicationClientConfig::SSL> ssl) const {
    return memgraph::storage::replication::ReplicaStatus{.name = name,
                                                         .ip_address = ip_address,
                                                         .port = port,
                                                         .sync_mode = sync_mode,
                                                         .replica_check_frequency = replica_check_frequency,
                                                         .ssl = ssl};
  }

  static_assert(
      sizeof(memgraph::storage::replication::ReplicaStatus) == 152,
      "Most likely you modified ReplicaStatus without updating the tests. Please modify CreateReplicaStatus. ");
};

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesInitialized) {
  auto replicas_status = CreateReplicaStatus(
      "name", "ip_address", 0, memgraph::storage::replication::ReplicationMode::SYNC, std::chrono::seconds(1),
      memgraph::storage::replication::ReplicationClientConfig::SSL{.key_file = "key_file", .cert_file = "cert_file"});

  auto json_status = memgraph::storage::replication::ReplicaStatusToJSON(
      memgraph::storage::replication::ReplicaStatus(replicas_status));
  auto replicas_status_converted = memgraph::storage::replication::JSONToReplicaStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestOnlyMandatoryAttributesInitialized) {
  auto replicas_status =
      CreateReplicaStatus("name", "ip_address", 0, memgraph::storage::replication::ReplicationMode::SYNC,
                          std::chrono::seconds(1), std::nullopt);

  auto json_status = memgraph::storage::replication::ReplicaStatusToJSON(
      memgraph::storage::replication::ReplicaStatus(replicas_status));
  auto replicas_status_converted = memgraph::storage::replication::JSONToReplicaStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesButSSLInitialized) {
  auto replicas_status =
      CreateReplicaStatus("name", "ip_address", 0, memgraph::storage::replication::ReplicationMode::SYNC,
                          std::chrono::seconds(1), std::nullopt);

  auto json_status = memgraph::storage::replication::ReplicaStatusToJSON(
      memgraph::storage::replication::ReplicaStatus(replicas_status));
  auto replicas_status_converted = memgraph::storage::replication::JSONToReplicaStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesButTimeoutInitialized) {
  auto replicas_status = CreateReplicaStatus(
      "name", "ip_address", 0, memgraph::storage::replication::ReplicationMode::SYNC, std::chrono::seconds(1),
      memgraph::storage::replication::ReplicationClientConfig::SSL{.key_file = "key_file", .cert_file = "cert_file"});

  auto json_status = memgraph::storage::replication::ReplicaStatusToJSON(
      memgraph::storage::replication::ReplicaStatus(replicas_status));
  auto replicas_status_converted = memgraph::storage::replication::JSONToReplicaStatus(std::move(json_status));

  ASSERT_EQ(replicas_status, *replicas_status_converted);
}
