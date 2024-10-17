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

#include "coordination/replication_instance_connector.hpp"
#include "coordination/coordinator_instance.hpp"

#include "auth/auth.hpp"
#include "flags/run_time_configurable.hpp"
#include "interpreter_faker.hpp"
#include "io/network/endpoint.hpp"
#include "license/license.hpp"
#include "replication_handler/replication_handler.hpp"
#include "storage/v2/config.hpp"

#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::ReplicationClientsInfo;
using memgraph::coordination::ReplicationInstanceClient;
using memgraph::coordination::ReplicationInstanceConnector;
using memgraph::utils::UUID;

using testing::_;

class ReplicationInstanceClientMock : public ReplicationInstanceClient {
 public:
  ReplicationInstanceClientMock() : ReplicationInstanceClient({}, nullptr) {}
  MOCK_METHOD(std::chrono::seconds, InstanceDownTimeoutSec, (), (override, const));
  MOCK_METHOD(std::chrono::seconds, InstanceGetUUIDFrequencySec, (), (override, const));
  MOCK_METHOD(std::string, InstanceName, (), (override, const));
  MOCK_METHOD(std::string, ManagementSocketAddress, (), (override, const));
  MOCK_METHOD(std::string, ReplicationSocketAddress, (), (override, const));
  MOCK_METHOD(bool, SendPromoteReplicaToMainRpc, (UUID const &uuid, ReplicationClientsInfo replication_clients_info),
              (override, const));
  MOCK_METHOD(bool, SendDemoteToReplicaRpc, (), (override, const));
  MOCK_METHOD(void, StartStateCheck, (), (override));
  MOCK_METHOD(void, StopStateCheck, (), (override));
  MOCK_METHOD(void, PauseStateCheck, (), (override));
  MOCK_METHOD(void, ResumeStateCheck, (), (override));
  MOCK_METHOD(bool, SendUnregisterReplicaRpc, (std::string_view instance_name), (override, const));
  MOCK_METHOD(bool, SendEnableWritingOnMainRpc, (), (override, const));
};

class ReplicationInstanceConnectorTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(ReplicationInstanceConnectorTest, OnFailPing) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, InstanceDownTimeoutSec()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.OnFailPing();
}

TEST_F(ReplicationInstanceConnectorTest, InstanceName) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, InstanceName()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.InstanceName();
}

TEST_F(ReplicationInstanceConnectorTest, CoordinatorSocketAddress) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, ManagementSocketAddress()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.ManagementSocketAddress();
}

TEST_F(ReplicationInstanceConnectorTest, ReplicationSocketAddress) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, ReplicationSocketAddress()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.ReplicationSocketAddress();
}

TEST_F(ReplicationInstanceConnectorTest, SendDemoteToReplicaRpc) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, SendDemoteToReplicaRpc()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.SendDemoteToReplicaRpc();
}

TEST_F(ReplicationInstanceConnectorTest, ManipulatingChecks) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, StartStateCheck()).Times(1);
  EXPECT_CALL(*client, StopStateCheck()).Times(1);
  EXPECT_CALL(*client, PauseStateCheck()).Times(1);
  EXPECT_CALL(*client, ResumeStateCheck()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.StartStateCheck();
  connector.StopStateCheck();
  connector.PauseStateCheck();
  connector.ResumeStateCheck();
}

TEST_F(ReplicationInstanceConnectorTest, SendEnableWritingOnMainRpc) {
  auto client = std::make_unique<ReplicationInstanceClientMock>();
  EXPECT_CALL(*client, SendEnableWritingOnMainRpc()).Times(1);

  auto connector = ReplicationInstanceConnector({}, nullptr);
  connector.SendEnableWritingOnMainRpc();
}
