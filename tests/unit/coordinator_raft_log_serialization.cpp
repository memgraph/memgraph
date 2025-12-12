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

#include "io/network/endpoint.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

import memgraph.coordination.coordinator_cluster_state;
import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_instance_context;
import memgraph.coordination.coordinator_state_machine;
import memgraph.coordination.data_instance_context;

using memgraph::coordination::CoordinatorClusterStateDelta;
using memgraph::coordination::CoordinatorInstanceContext;
using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::DataInstanceConfig;
using memgraph::coordination::DataInstanceContext;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::utils::UUID;

// No networking communication in this test.
class RaftLogSerialization : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_raft_log_serialization"};
};

TEST_F(RaftLogSerialization, ReplClientInfo) {
  ReplicationClientInfo info{.instance_name = "instance_name",
                             .replication_mode = ReplicationMode::SYNC,
                             .replication_server = Endpoint{"127.0.0.1", 10111}};

  nlohmann::json j = info;
  ReplicationClientInfo info2 = j.get<memgraph::coordination::ReplicationClientInfo>();

  ASSERT_EQ(info, info2);
}

TEST_F(RaftLogSerialization, DataInstanceConfig) {
  DataInstanceConfig config{.instance_name = "instance3",
                            .mgt_server = Endpoint{"127.0.0.1", 10112},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = Endpoint{"127.0.0.1", 10001}}};

  nlohmann::json j = config;
  DataInstanceConfig config2 = j.get<memgraph::coordination::DataInstanceConfig>();

  ASSERT_EQ(config, config2);
}

TEST_F(RaftLogSerialization, SerializeUpdateClusterState) {
  DataInstanceConfig config{.instance_name = "instance3",
                            .mgt_server = Endpoint{"127.0.0.1", 10112},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = Endpoint{"127.0.0.1", 10001}}};

  std::vector<DataInstanceContext> data_instances;
  data_instances.emplace_back(config, ReplicationRole::REPLICA, UUID{});

  std::vector<CoordinatorInstanceContext> coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances,
                                                 .coordinator_instances_ = coord_instances,
                                                 .current_main_uuid_ = UUID{},
                                                 .enabled_reads_on_main_ = false};
  auto const buffer = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state);
  auto const decoded_log_state = CoordinatorStateMachine::DecodeLog(*buffer);
  ASSERT_EQ(delta_state, decoded_log_state);
}
