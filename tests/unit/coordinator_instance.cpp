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
#include "interpreter_faker.hpp"
#include "io/network/endpoint.hpp"
#include "utils/counter.hpp"

#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::CoordinatorInstance;
using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
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
  std::vector<uint16_t> const management_ports = {20113, 20114, 20115};
};

namespace {

auto HasBecomeEqual(std::function<int()> const &func, int expected_value) -> bool {
  constexpr int max_tries{200};
  constexpr int waiting_period_ms{100};
  auto maybe_stop = memgraph::utils::ResettableCounter<max_tries>();
  while (!maybe_stop()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(waiting_period_ms));
    if (func() == expected_value) {
      return true;
    }
  }
  return false;
};

}  // namespace

// Empty until you run 1st RegisterReplicationInstance or AddCoordinatorInstance
TEST_F(CoordinatorInstanceTest, ShowInstancesEmptyTest) {
  GTEST_SKIP() << "skip flaky issue #https://github.com/memgraph/memgraph/issues/2212";
  auto const init_config = CoordinatorInstanceInitConfig{coordinator_ids[0],
                                                         coordinator_ports[0],
                                                         bolt_ports[0],
                                                         management_ports[0],
                                                         main_data_directory / "high_availability" / "coordinator",
                                                         "localhost"};

  auto instance1 = CoordinatorInstance{init_config};
  auto const instances = instance1.ShowInstances();
  ASSERT_EQ(instances.size(), 1);
}

TEST_F(CoordinatorInstanceTest, ConnectCoordinators) {
  GTEST_SKIP() << "skip flaky issue #https://github.com/memgraph/memgraph/issues/2212";
  auto const wait_until_added = [](auto &instance) {
    while (instance.ShowInstances().size() != 3) {
    }
  };
  auto const init_config1 = CoordinatorInstanceInitConfig{coordinator_ids[0],
                                                          coordinator_ports[0],
                                                          bolt_ports[0],
                                                          management_ports[0],
                                                          main_data_directory / "high_availability1" / "coordinator",
                                                          "localhost"};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 = CoordinatorInstanceInitConfig{coordinator_ids[1],
                                                          coordinator_ports[1],
                                                          bolt_ports[1],
                                                          management_ports[1],
                                                          main_data_directory / "high_availability2" / "coordinator",
                                                          "localhost"};

  auto instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 = CoordinatorInstanceInitConfig{coordinator_ids[2],
                                                          coordinator_ports[2],
                                                          bolt_ports[2],
                                                          management_ports[2],
                                                          main_data_directory / "high_availability3" / "coordinator",
                                                          "localhost"};

  auto instance3 = CoordinatorInstance{init_config3};

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[1],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[1]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[1]},
                                     .management_server = Endpoint{"localhost", management_ports[1]},
                                     .coordinator_hostname = "localhost"});

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[2],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[2]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[2]},
                                     .management_server = Endpoint{"localhost", management_ports[2]},
                                     .coordinator_hostname = "localhost"});

  auto const wait_and_assert = [this](auto const &instance) {
    ASSERT_TRUE(HasBecomeEqual([&instance]() { return instance.ShowInstances().size(); }, 3));
    auto const instances = instance.ShowInstances();
    auto const coord1_it = std::ranges::find_if(instances, [this](auto &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[0]);
    });
    ASSERT_NE(coord1_it, instances.end());
    auto const coord2_it = std::ranges::find_if(instances, [this](auto &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[1]);
    });
    ASSERT_NE(coord2_it, instances.end());
    auto const coord3_it = std::ranges::find_if(instances, [this](auto &instance) {
      return instance.instance_name == fmt::format("coordinator_{}", coordinator_ids[2]);
    });
    ASSERT_NE(coord3_it, instances.end());
  };

  { wait_and_assert(instance1); }
  { wait_and_assert(instance2); }
  { wait_and_assert(instance3); }
}

TEST_F(CoordinatorInstanceTest, GetConnectedCoordinatorsConfigs) {
  GTEST_SKIP() << "skip flaky issue #https://github.com/memgraph/memgraph/issues/2212";
  auto const wait_until_added = [](auto &instance) {
    while (instance.ShowInstances().size() != 3) {
    }
  };
  auto const init_config1 = CoordinatorInstanceInitConfig{coordinator_ids[0],
                                                          coordinator_ports[0],
                                                          bolt_ports[0],
                                                          management_ports[0],
                                                          main_data_directory / "high_availability1" / "coordinator",
                                                          "localhost"};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 = CoordinatorInstanceInitConfig{coordinator_ids[1],
                                                          coordinator_ports[1],
                                                          bolt_ports[1],
                                                          management_ports[1],
                                                          main_data_directory / "high_availability2" / "coordinator",
                                                          "localhost"};

  auto instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 = CoordinatorInstanceInitConfig{coordinator_ids[2],
                                                          coordinator_ports[2],
                                                          bolt_ports[2],
                                                          management_ports[2],
                                                          main_data_directory / "high_availability3" / "coordinator",
                                                          "localhost"};

  auto instance3 = CoordinatorInstance{init_config3};

  auto const coord2_coord_config =
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[1],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[1]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[1]},
                                     .management_server = Endpoint{"localhost", management_ports[1]},
                                     .coordinator_hostname = "localhost"};
  instance1.AddCoordinatorInstance(coord2_coord_config);

  auto const coord3_coord_config =
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[2],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[2]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[2]},
                                     .management_server = Endpoint{"localhost", management_ports[2]},
                                     .coordinator_hostname = "localhost"};

  instance1.AddCoordinatorInstance(coord3_coord_config);

  auto const coord1_coord_config =
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[0],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[0]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[0]},
                                     .management_server = Endpoint{"localhost", management_ports[0]},
                                     .coordinator_hostname = "localhost"};
  auto const wait_and_assert = [&](auto const &instance) {
    ASSERT_TRUE(HasBecomeEqual([&instance]() { return instance.ShowInstances().size(); }, 3));
    ASSERT_TRUE(HasBecomeEqual([&instance]() { return instance.GetCoordinatorToCoordinatorConfigs().size(); }, 3));
    auto const coord_to_coord_configs = instance.GetCoordinatorToCoordinatorConfigs();
    auto const coord1_config_it = std::ranges::find_if(
        coord_to_coord_configs, [this](auto const &config) { return config.coordinator_id == coordinator_ids[0]; });
    auto const coord2_config_it = std::ranges::find_if(
        coord_to_coord_configs, [this](auto const &config) { return config.coordinator_id == coordinator_ids[1]; });
    auto const coord3_config_it = std::ranges::find_if(
        coord_to_coord_configs, [this](auto const &config) { return config.coordinator_id == coordinator_ids[2]; });

    ASSERT_NE(coord1_config_it, coord_to_coord_configs.end());
    ASSERT_NE(coord2_config_it, coord_to_coord_configs.end());
    ASSERT_NE(coord3_config_it, coord_to_coord_configs.end());

    ASSERT_EQ(*coord1_config_it, coord1_coord_config);
    ASSERT_EQ(*coord2_config_it, coord2_coord_config);
    ASSERT_EQ(*coord3_config_it, coord3_coord_config);
  };
  { wait_and_assert(instance1); }
  { wait_and_assert(instance2); }
  { wait_and_assert(instance3); }
}

TEST_F(CoordinatorInstanceTest, GetRoutingTable) {
  GTEST_SKIP() << "skip flaky issue #https://github.com/memgraph/memgraph/issues/2212";
  auto const wait_until_added = [](auto &instance) {
    while (instance.ShowInstances().size() != 3) {
    }
  };
  auto const init_config1 = CoordinatorInstanceInitConfig{coordinator_ids[0],
                                                          coordinator_ports[0],
                                                          bolt_ports[0],
                                                          management_ports[0],
                                                          main_data_directory / "high_availability1" / "coordinator",
                                                          "localhost"};

  auto instance1 = CoordinatorInstance{init_config1};

  auto const init_config2 = CoordinatorInstanceInitConfig{coordinator_ids[1],
                                                          coordinator_ports[1],
                                                          bolt_ports[1],
                                                          management_ports[1],
                                                          main_data_directory / "high_availability2" / "coordinator",
                                                          "localhost"};

  auto instance2 = CoordinatorInstance{init_config2};

  auto const init_config3 = CoordinatorInstanceInitConfig{coordinator_ids[2],
                                                          coordinator_ports[2],
                                                          bolt_ports[2],
                                                          management_ports[2],
                                                          main_data_directory / "high_availability3" / "coordinator",
                                                          "localhost"};

  auto instance3 = CoordinatorInstance{init_config3};

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[1],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[1]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[1]},
                                     .management_server = Endpoint{"localhost", management_ports[1]},
                                     .coordinator_hostname = "localhost"});

  instance1.AddCoordinatorInstance(
      CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_ids[2],
                                     .bolt_server = Endpoint{"localhost", bolt_ports[2]},
                                     .coordinator_server = Endpoint{"localhost", coordinator_ports[2]},
                                     .management_server = Endpoint{"localhost", management_ports[2]},
                                     .coordinator_hostname = "localhost"});

  spdlog::trace("Added coordinator instances!");
  {
    ASSERT_TRUE(HasBecomeEqual([&instance1]() { return instance1.ShowInstances().size(); }, 3));
    ASSERT_TRUE(HasBecomeEqual([&instance1]() { return instance1.GetCoordinatorToCoordinatorConfigs().size(); }, 3));
    auto const routing_table = instance1.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[0]); });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[1]); });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[2]); });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }

  {
    ASSERT_TRUE(HasBecomeEqual([&instance2]() { return instance2.ShowInstances().size(); }, 3));
    ASSERT_TRUE(HasBecomeEqual([&instance2]() { return instance2.GetCoordinatorToCoordinatorConfigs().size(); }, 3));
    auto const routing_table = instance2.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[0]); });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[1]); });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[2]); });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }

  {
    ASSERT_TRUE(HasBecomeEqual([&instance3]() { return instance3.ShowInstances().size(); }, 3));
    ASSERT_TRUE(HasBecomeEqual([&instance3]() { return instance3.GetCoordinatorToCoordinatorConfigs().size(); }, 3));
    auto const routing_table = instance3.GetRoutingTable();
    ASSERT_EQ(routing_table.size(), 1);
    auto const &routers = routing_table[0];
    ASSERT_EQ(routers.second, "ROUTE");
    ASSERT_EQ(routers.first.size(), 3);
    auto const coord1_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[0]); });
    ASSERT_NE(coord1_route_it, routers.first.end());
    auto const coord2_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[1]); });
    ASSERT_NE(coord2_route_it, routers.first.end());
    auto const coord3_route_it = std::ranges::find_if(
        routers.first, [this](auto const &route) { return route == fmt::format("localhost:{}", bolt_ports[2]); });
    ASSERT_NE(coord3_route_it, routers.first.end());
  }
}
