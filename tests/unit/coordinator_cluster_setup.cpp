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
#include "coordination/coordinator_handlers.hpp"
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
using memgraph::coordination::CoordinatorState;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::HealthCheckClientCallback;
using memgraph::coordination::HealthCheckInstanceCallback;
using memgraph::coordination::RegisterInstanceCoordinatorStatus;
using memgraph::coordination::ReplicationInstanceInitConfig;
using memgraph::coordination::SetInstanceToMainCoordinatorStatus;
using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationHandler;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::storage::Config;

struct ReplicationInstance {
  ReplicationInstance(Config const &storage_config, ReplicationInstanceInitConfig const &replication_config)
      : auth{storage_config.durability.storage_directory / "auth", memgraph::auth::Auth::Config{/* default */}},
        dbms{storage_config, repl_state, auth, true},
        db_acc{dbms.Get()},
        db{*db_acc.get()},
        repl_state{ReplicationStateRootPath(storage_config)},
        repl_handler(repl_state, dbms, system_, auth),
        coordinator_state{replication_config} {
    memgraph::dbms::CoordinatorHandlers::Register(coordinator_state.GetCoordinatorServer(), repl_handler);
    MG_ASSERT(coordinator_state.GetCoordinatorServer().Start(), "Failed to start coordinator server!");
  }
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system_;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::dbms::DatabaseAccess db_acc;
  memgraph::dbms::Database &db;
  memgraph::replication::ReplicationState repl_state;
  ReplicationHandler repl_handler;
  CoordinatorState coordinator_state;
};

// Networking is used in this test, be careful with ports used.
class HighAvailabilityClusterSetupTest : public ::testing::Test {
 public:
  HighAvailabilityClusterSetupTest() {
    coordinator1.AddCoordinatorInstance(
        CoordinatorToCoordinatorConfig{.coordinator_id = 2,
                                       .bolt_server = Endpoint{"0.0.0.0", 7691},
                                       .coordinator_server = Endpoint{"0.0.0.0", 10112}});

    coordinator1.AddCoordinatorInstance(
        CoordinatorToCoordinatorConfig{.coordinator_id = 3,
                                       .bolt_server = Endpoint{"0.0.0.0", 7692},
                                       .coordinator_server = Endpoint{"0.0.0.0", 10113}});
    {
      auto const coord_to_instance_config =
          CoordinatorToReplicaConfig{.instance_name = "instance1",
                                     .mgt_server = Endpoint{"0.0.0.0", 10011},
                                     .bolt_server = Endpoint{"0.0.0.0", 7687},
                                     .replication_client_info = {.instance_name = "instance1",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"0.0.0.0", 10001}},
                                     .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                     .instance_down_timeout_sec = std::chrono::seconds{5},
                                     .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                     .ssl = std::nullopt};

      auto const status = coordinator1.RegisterReplicationInstance(coord_to_instance_config);
      MG_ASSERT(status == RegisterInstanceCoordinatorStatus::SUCCESS, "Failed to register instance1");
    }
    {
      auto const coord_to_instance_config =
          CoordinatorToReplicaConfig{.instance_name = "instance2",
                                     .mgt_server = Endpoint{"0.0.0.0", 10012},
                                     .bolt_server = Endpoint{"0.0.0.0", 7688},
                                     .replication_client_info = {.instance_name = "instance2",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"0.0.0.0", 10002}},
                                     .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                     .instance_down_timeout_sec = std::chrono::seconds{5},
                                     .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                     .ssl = std::nullopt};

      auto const status = coordinator1.RegisterReplicationInstance(coord_to_instance_config);
      MG_ASSERT(status == RegisterInstanceCoordinatorStatus::SUCCESS, "Failed to register instance2");
    }
    {
      auto const coord_to_instance_config =
          CoordinatorToReplicaConfig{.instance_name = "instance3",
                                     .mgt_server = Endpoint{"0.0.0.0", 10013},
                                     .bolt_server = Endpoint{"0.0.0.0", 7689},
                                     .replication_client_info = {.instance_name = "instance3",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"0.0.0.0", 10003}},
                                     .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                     .instance_down_timeout_sec = std::chrono::seconds{5},
                                     .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                     .ssl = std::nullopt};

      auto const status = coordinator1.RegisterReplicationInstance(coord_to_instance_config);
      MG_ASSERT(status == RegisterInstanceCoordinatorStatus::SUCCESS, "Failed to register instance3");
    }
    {
      auto const status = coordinator1.SetReplicationInstanceToMain("instance1");
      MG_ASSERT(status == SetInstanceToMainCoordinatorStatus::SUCCESS, "Failed to set instance1 to main");
    }
  }

 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  std::vector<std::filesystem::path> storage_directories{
      std::filesystem::temp_directory_path() / "MG_test_unit_storage_ha_instance1",
      std::filesystem::temp_directory_path() / "MG_test_unit_storage_ha_instance2",
      std::filesystem::temp_directory_path() / "MG_test_unit_storage_ha_instance3",
  };

  Config storage1_config = [&] {
    Config config;
    UpdatePaths(config, storage_directories[0]);
    return config;
  }();

  ReplicationInstanceInitConfig instance1_config{.management_port = 10011};

  Config storage2_config = [&] {
    Config config;
    UpdatePaths(config, storage_directories[1]);
    return config;
  }();

  ReplicationInstanceInitConfig instance2_config{.management_port = 10012};

  Config storage3_config = [&] {
    Config config;
    UpdatePaths(config, storage_directories[2]);
    return config;
  }();

  ReplicationInstanceInitConfig instance3_config{.management_port = 10013};

  ReplicationInstance instance1{storage1_config, instance1_config};
  ReplicationInstance instance2{storage2_config, instance2_config};
  ReplicationInstance instance3{storage3_config, instance3_config};
  CoordinatorInstance coordinator1{
      CoordinatorInstanceInitConfig{1, 10111, 7690, storage_directories[0] / "high_availability" / "coordinator"}};
  CoordinatorInstance coordinator2{
      CoordinatorInstanceInitConfig{2, 10112, 7691, storage_directories[1] / "high_availability" / "coordinator"}};
  CoordinatorInstance coordinator3{
      CoordinatorInstanceInitConfig{3, 10113, 7692, storage_directories[2] / "high_availability" / "coordinator"}};

 private:
  void Clear() {
    for (const auto &storage_directory : storage_directories) {
      if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
    }
  }
};

TEST_F(HighAvailabilityClusterSetupTest, CreateCluster1) {
  auto const leader_instances = coordinator1.ShowInstances();
  EXPECT_EQ(leader_instances.size(), 6);
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "coordinator_1"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "coordinator_2"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "coordinator_3"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "instance1"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "instance2"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "instance3"; }),
            leader_instances.end());

  auto const follower_instances = coordinator2.ShowInstances();
  EXPECT_EQ(follower_instances.size(), 6);
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "coordinator_1"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "coordinator_2"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "coordinator_3"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "instance1"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "instance2"; }),
            leader_instances.end());
  ASSERT_NE(std::ranges::find_if(leader_instances,
                                 [](auto const &instance) { return instance.instance_name == "instance3"; }),
            leader_instances.end());
}
