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
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::CoordinatorInstance;
using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftState;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationHandler;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::storage::Config;

class CoordinatorInstanceTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path main_data_directory{std::filesystem::temp_directory_path() /
                                            "MG_tests_unit_coordinator_instance"};
};

TEST_F(CoordinatorInstanceTest, ShowInstancesEmptyTest) {
  auto const init_config =
      CoordinatorInstanceInitConfig{.coordinator_id = 4, .coordinator_port = 10110, .bolt_port = 7686};

  auto const instance1 = CoordinatorInstance{init_config};
  auto const instances = instance1.ShowInstances();
  ASSERT_EQ(instances.size(), 1);
  ASSERT_EQ(instances[0].instance_name, "coordinator_4");
  ASSERT_EQ(instances[0].health, "unknown");
  ASSERT_EQ(instances[0].raft_socket_address, "127.0.0.1:10110");
  ASSERT_EQ(instances[0].coord_socket_address, "");
  ASSERT_EQ(instances[0].cluster_role, "coordinator");
}

TEST_F(CoordinatorInstanceTest, ConnectCoordinators) {
  auto const init_config2 =
      CoordinatorInstanceInitConfig{.coordinator_id = 2, .coordinator_port = 10112, .bolt_port = 7688};

  auto const instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 =
      CoordinatorInstanceInitConfig{.coordinator_id = 3, .coordinator_port = 10113, .bolt_port = 7689};

  auto const instance3 = CoordinatorInstance{init_config3};

  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7687};

  auto instance1 = CoordinatorInstance{init_config1};

  instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{.coordinator_id = 2,
                                                                  .bolt_server = Endpoint{"127.0.0.1", 7688},
                                                                  .coordinator_server = Endpoint{"127.0.0.1", 10112}});

  instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{.coordinator_id = 3,
                                                                  .bolt_server = Endpoint{"127.0.0.1", 7689},
                                                                  .coordinator_server = Endpoint{"127.0.0.1", 10113}});

  auto const instances = instance1.ShowInstances();
  ASSERT_EQ(instances.size(), 3);
  ASSERT_EQ(instances[0].instance_name, "coordinator_1");
  ASSERT_EQ(instances[1].instance_name, "coordinator_2");
  ASSERT_EQ(instances[2].instance_name, "coordinator_3");
}
