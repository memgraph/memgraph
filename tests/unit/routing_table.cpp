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

#include "auth/auth.hpp"
#include "coordination/coordinator_instance.hpp"
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
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftState;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationHandler;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::storage::Config;

// class MockCoordinatorInstance : CoordinatorInstance {
//   auto AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> void override {}
// };

class RoutingTableTest : public ::testing::Test {
 protected:
  std::filesystem::path main_data_directory{std::filesystem::temp_directory_path() /
                                            "MG_tests_unit_coordinator_cluster_state"};
  std::filesystem::path repl1_data_directory{std::filesystem::temp_directory_path() /
                                             "MG_test_unit_storage_v2_replication_repl"};
  std::filesystem::path repl2_data_directory{std::filesystem::temp_directory_path() /
                                             "MG_test_unit_storage_v2_replication_repl2"};
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  Config main_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true},
    };
    UpdatePaths(config, main_data_directory);
    return config;
  }();
  Config repl1_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true},
    };
    UpdatePaths(config, repl1_data_directory);
    return config;
  }();
  Config repl2_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true},
    };
    UpdatePaths(config, repl2_data_directory);
    return config;
  }();

  const std::string local_host = ("127.0.0.1");
  const std::array<uint16_t, 2> ports{10000, 20000};
  const std::array<std::string, 2> replicas = {"REPLICA1", "REPLICA2"};

 private:
  void Clear() {
    if (std::filesystem::exists(main_data_directory)) std::filesystem::remove_all(main_data_directory);
    if (std::filesystem::exists(repl1_data_directory)) std::filesystem::remove_all(repl1_data_directory);
    if (std::filesystem::exists(repl2_data_directory)) std::filesystem::remove_all(repl2_data_directory);
  }
};

struct MinMemgraph {
  MinMemgraph(const memgraph::storage::Config &conf)
      : auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{/* default */}},
        repl_state{ReplicationStateRootPath(conf)},
        dbms{conf, repl_state
#ifdef MG_ENTERPRISE
             ,
             auth, true
#endif
        },
        db_acc{dbms.Get()},
        db{*db_acc.get()},
        repl_handler(repl_state, dbms
#ifdef MG_ENTERPRISE
                     ,
                     system_, auth
#endif
        ) {
  }
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system_;
  memgraph::replication::ReplicationState repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::dbms::DatabaseAccess db_acc;
  memgraph::dbms::Database &db;
  ReplicationHandler repl_handler;
};
;

TEST_F(RoutingTableTest, GetSingleRouterRoutingTable) {
  CoordinatorInstance instance1;
  auto routing = std::map<std::string, std::string>{{"address", "localhost:7688"}};
  auto routing_table = instance1.GetRoutingTable(routing);

  ASSERT_EQ(routing_table.size(), 1);

  auto const routers = routing_table[0];
  ASSERT_EQ(routers.first, std::vector<std::string>{"localhost:7688"});
  ASSERT_EQ(routers.second, "ROUTE");
}

TEST_F(RoutingTableTest, GetMixedRoutingTable) {
  auto instance1 = RaftState::MakeRaftState([]() {}, []() {});
  auto routing = std::map<std::string, std::string>{{"address", "localhost:7690"}};
  instance1.AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig{
      .instance_name = "instance2",
      .mgt_server = Endpoint{"127.0.0.1", 10011},
      .bolt_server = Endpoint{"127.0.0.1", 7687},
      .replication_client_info = ReplicationClientInfo{.instance_name = "instance2",
                                                       .replication_mode = ReplicationMode::ASYNC,
                                                       .replication_server = Endpoint{"127.0.0.1", 10001}}});
  // auto routing_table = instance1.GetRoutingTable(routing);

  // ASSERT_EQ(routing_table.size(), 1);
  // auto const routers = routing_table[0];
  // ASSERT_EQ(routers.second, "ROUTE");
}

// TEST_F(RoutingTableTest, GetMultipleRoutersRoutingTable) {
//
//   CoordinatorInstance instance1;
//   instance1.AddCoordinatorInstance(CoordinatorToCoordinatorConfig{.coordinator_id = 1,
//                                                                   .bolt_server = Endpoint{"127.0.0.1", 7689},
//                                                                   .coordinator_server = Endpoint{"127.0.0.1",
//                                                                   10111}});
//
//   auto routing = std::map<std::string, std::string>{{"address", "localhost:7688"}};
//   auto routing_table = instance1.GetRoutingTable(routing);
//
//   ASSERT_EQ(routing_table.size(), 1);
//
//   auto const routers = routing_table[0];
//   ASSERT_EQ(routers.second, "ROUTE");
//   ASSERT_EQ(routers.first.size(), 2);
//   auto const expected_routers = std::vector<std::string>{"localhost:7689", "localhost:7688"};
//   ASSERT_EQ(routers.first, expected_routers);
// }
