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

// Networking is used in this test, be careful with ports used.
class CoordinatorInstanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::filesystem::exists(main_data_directory)) return;
    std::filesystem::remove_all(main_data_directory);
  }

  void TearDown() override {
    if (!std::filesystem::exists(main_data_directory)) return;
    std::filesystem::remove_all(main_data_directory);
  }

  std::filesystem::path main_data_directory{std::filesystem::temp_directory_path() /
                                            "MG_tests_unit_coordinator_instance"};

  std::vector<uint32_t> const coordinator_ids = {41, 42, 43};
  std::vector<uint16_t> const bolt_ports = {2687, 2688, 2689};
  std::vector<uint16_t> const coordinator_ports = {40113, 40114, 40115};
};

// Empty until you run 1st RegisterReplicationInstance or AddCoordinatorInstance
TEST_F(CoordinatorInstanceTest, ShowInstancesEmptyTest) {
  auto const init_config = CoordinatorInstanceInitConfig{coordinator_ids[0], coordinator_ports[0], bolt_ports[0],
                                                         main_data_directory / "high_availability" / "coordinator"};

  auto const instance1 = CoordinatorInstance{init_config};
  auto const instances = instance1.ShowInstances();
  ASSERT_EQ(instances.size(), 1);
}

TEST_F(CoordinatorInstanceTest, ConnectCoordinators) {
  auto const wait_until_added = [](auto const &instance) {
    while (instance.ShowInstances().size() != 3) {
    }
  };

  auto const init_config1 = CoordinatorInstanceInitConfig{coordinator_ids[0], coordinator_ports[0], bolt_ports[0],
                                                          main_data_directory / "high_availability1" / "coordinator"};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 = CoordinatorInstanceInitConfig{coordinator_ids[1], coordinator_ports[1], bolt_ports[1],
                                                          main_data_directory / "high_availability2" / "coordinator"};

  auto const instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 = CoordinatorInstanceInitConfig{coordinator_ids[2], coordinator_ports[2], bolt_ports[2],
                                                          main_data_directory / "high_availability3" / "coordinator"};

  auto const instance3 = CoordinatorInstance{init_config3};

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[1],
                                     .bolt_server = Endpoint{"0.0.0.0", bolt_ports[1]},
                                     .coordinator_server = Endpoint{"0.0.0.0", coordinator_ports[1]}});

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[2],
                                     .bolt_server = Endpoint{"0.0.0.0", bolt_ports[2]},
                                     .coordinator_server = Endpoint{"0.0.0.0", coordinator_ports[2]}});
  {
    wait_until_added(instance1);
    auto const instances = instance1.ShowInstances();
    auto const coord1_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[0]);
    });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[1]);
    });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[2]);
    });
    ASSERT_NE(coord3_it, instances.end());
  }
  {
    wait_until_added(instance2);
    auto const instances = instance2.ShowInstances();
    auto const coord1_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[0]);
    });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[1]);
    });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[2]);
    });
    ASSERT_NE(coord3_it, instances.end());
  }
  {
    wait_until_added(instance3);
    auto const instances = instance3.ShowInstances();
    auto const coord1_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[0]);
    });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[1]);
    });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it = std::ranges::find_if(instances, [this](auto const &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[2]);
    });
    ASSERT_NE(coord3_it, instances.end());
  }
}

TEST_F(CoordinatorInstanceTest, GetRoutingTable) {
  auto const wait_until_added = [](auto const &instance) {
    while (instance.ShowInstances().size() != 3) {
    }
  };

  auto const init_config1 = CoordinatorInstanceInitConfig{coordinator_ids[0], coordinator_ports[0], bolt_ports[0],
                                                          main_data_directory / "high_availability1" / "coordinator"};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 = CoordinatorInstanceInitConfig{coordinator_ids[1], coordinator_ports[1], bolt_ports[1],
                                                          main_data_directory / "high_availability2" / "coordinator"};

  auto const instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 = CoordinatorInstanceInitConfig{coordinator_ids[2], coordinator_ports[2], bolt_ports[2],
                                                          main_data_directory / "high_availability3" / "coordinator"};

  auto const instance3 = CoordinatorInstance{init_config3};

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[1],
                                     .bolt_server = Endpoint{"0.0.0.0", bolt_ports[1]},
                                     .coordinator_server = Endpoint{"0.0.0.0", coordinator_ports[1]}});

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[2],
                                     .bolt_server = Endpoint{"0.0.0.0", bolt_ports[2]},
                                     .coordinator_server = Endpoint{"0.0.0.0", coordinator_ports[2]}});

  {
    wait_until_added(instance1);
    auto const routing_table = instance1.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[0]); });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[1]); });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[2]); });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }

  {
    wait_until_added(instance2);
    auto const routing_table = instance2.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[0]); });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[1]); });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[2]); });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }

  {
    wait_until_added(instance3);
    auto const routing_table = instance3.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[0]); });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[1]); });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("0.0.0.0:{}", bolt_ports[2]); });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }
}
