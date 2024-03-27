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
using memgraph::coordination::HealthCheckClientCallback;
using memgraph::coordination::HealthCheckInstanceCallback;
using memgraph::io::network::Endpoint;

class CoordinatorInstanceTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path main_data_directory{std::filesystem::temp_directory_path() /
                                            "MG_tests_unit_coordinator_instance"};
};

// TEST_F(CoordinatorInstanceTest, RegisterReplicationInstance) {
//   auto const init_config =
//       CoordinatorInstanceInitConfig{.coordinator_id = 4, .coordinator_port = 10110, .bolt_port = 7686};
//   auto instance1 = CoordinatorInstance{init_config};
//
//   auto const coord_to_replica_config =
//       CoordinatorToReplicaConfig{.instance_name = "instance3",
//                                  .mgt_server = Endpoint{"0.0.0.0", 10112},
//                                  .bolt_server = Endpoint{"0.0.0.0", 7687},
//                                  .replication_client_info = {.instance_name = "instance_name",
//                                                              .replication_mode = ReplicationMode::ASYNC,
//                                                              .replication_server = Endpoint{"0.0.0.0", 10001}},
//                                  .instance_health_check_frequency_sec = std::chrono::seconds{1},
//                                  .instance_down_timeout_sec = std::chrono::seconds{5},
//                                  .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
//                                  .ssl = std::nullopt};
//
//   auto status = instance1.RegisterReplicationInstance(coord_to_replica_config);
//   EXPECT_EQ(status, RegisterInstanceCoordinatorStatus::RPC_FAILED);
// }

// Empty until you run 1st RegisterReplicationInstance or AddCoordinatorInstance
TEST_F(CoordinatorInstanceTest, ShowInstancesEmptyTest) {
  auto const init_config =
      CoordinatorInstanceInitConfig{.coordinator_id = 4, .coordinator_port = 10110, .bolt_port = 7686};

  auto const instance1 = CoordinatorInstance{init_config};
  auto const instances = instance1.ShowInstances();
  ASSERT_EQ(instances.size(), 0);
}

TEST_F(CoordinatorInstanceTest, ConnectCoordinators) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7687};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 =
      CoordinatorInstanceInitConfig{.coordinator_id = 2, .coordinator_port = 10112, .bolt_port = 7688};

  auto const instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 =
      CoordinatorInstanceInitConfig{.coordinator_id = 3, .coordinator_port = 10113, .bolt_port = 7689};

  auto const instance3 = CoordinatorInstance{init_config3};

  instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{
      .coordinator_id = 2, .bolt_server = Endpoint{"0.0.0.0", 7688}, .coordinator_server = Endpoint{"0.0.0.0", 10112}});

  instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{
      .coordinator_id = 3, .bolt_server = Endpoint{"0.0.0.0", 7689}, .coordinator_server = Endpoint{"0.0.0.0", 10113}});
  {
    auto const instances = instance1.ShowInstances();
    ASSERT_EQ(instances.size(), 3);
    auto const coord1_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_1"; });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_2"; });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_3"; });
    ASSERT_NE(coord3_it, instances.end());
  }
  {
    auto const instances = instance2.ShowInstances();
    ASSERT_EQ(instances.size(), 3);
    auto const coord1_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_1"; });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_2"; });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_3"; });
    ASSERT_NE(coord3_it, instances.end());
  }
  {
    auto const instances = instance3.ShowInstances();
    ASSERT_EQ(instances.size(), 3);
    auto const coord1_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_1"; });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_2"; });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it =
        std::ranges::find_if(instances, [](auto const &instance) { return instance.instance_name == "coordinator_3"; });
    ASSERT_NE(coord3_it, instances.end());
  }
}

TEST_F(CoordinatorInstanceTest, GetRoutingTable) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7687};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 =
      CoordinatorInstanceInitConfig{.coordinator_id = 2, .coordinator_port = 10112, .bolt_port = 7688};

  auto const instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 =
      CoordinatorInstanceInitConfig{.coordinator_id = 3, .coordinator_port = 10113, .bolt_port = 7689};

  auto const instance3 = CoordinatorInstance{init_config3};

  instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{
      .coordinator_id = 2, .bolt_server = Endpoint{"0.0.0.0", 7688}, .coordinator_server = Endpoint{"0.0.0.0", 10112}});

  instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{
      .coordinator_id = 3, .bolt_server = Endpoint{"0.0.0.0", 7689}, .coordinator_server = Endpoint{"0.0.0.0", 10113}});

  {
    auto const routing_table = instance1.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7687"; });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7688"; });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7689"; });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }

  {
    auto const routing_table = instance2.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7687"; });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7688"; });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7689"; });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }

  {
    auto const routing_table = instance3.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7687"; });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7688"; });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it =
        std::ranges::find_if(routers.first, [](auto const &route) { return route == "0.0.0.0:7689"; });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }
}
