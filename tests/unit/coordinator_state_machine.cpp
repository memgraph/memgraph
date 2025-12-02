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

#include "coordination/coordinator_state_machine.hpp"
#include "coordination/constants.hpp"
#include "coordination/coordinator_state_manager.hpp"
#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "replication_coordination_glue/mode.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <ranges>

import memgraph.coordination.coordinator_communication_config;

using memgraph::coordination::CoordinatorClusterState;
using memgraph::coordination::CoordinatorInstanceAux;
using memgraph::coordination::CoordinatorStateManager;
using memgraph::coordination::CoordinatorStateManagerConfig;
using memgraph::coordination::SnapshotCtx;

using memgraph::coordination::CoordinatorInstanceConfig;
using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::DataInstanceConfig;
using memgraph::coordination::InstanceUUIDUpdate;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::utils::UUID;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::cluster_config;
using nuraft::snapshot;
using nuraft::srv_config;

namespace {
void CompareServers(std::shared_ptr<srv_config> const &temp_server, std::shared_ptr<srv_config> const &loaded_server) {
  ASSERT_EQ(temp_server->get_id(), loaded_server->get_id());
  ASSERT_EQ(temp_server->get_endpoint(), loaded_server->get_endpoint());
  ASSERT_EQ(temp_server->get_aux(), loaded_server->get_aux());
}
}  // namespace

// No networking communication in this test.
class CoordinatorStateMachineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  void TearDown() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_state_machine"};

  uint16_t const bolt_port = 9687;
  uint16_t const mgt_port = 30112;
  uint16_t const replication_port = 30001;
};

class CoordinatorStateMachineTestParam : public ::testing::TestWithParam<memgraph::coordination::LogStoreVersion> {
 public:
  struct PrintLogVersionToInt {
    std::string operator()(const testing::TestParamInfo<memgraph::coordination::LogStoreVersion> &info) {
      return std::to_string(static_cast<int>(info.param));
    }
  };

 protected:
  void SetUp() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  void TearDown() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_state_machine"};

  uint16_t const bolt_port = 9687;
  uint16_t const mgt_port = 30112;
  uint16_t const replication_port = 30001;
};

TEST_P(CoordinatorStateMachineTestParam, SerializeDeserializeSnapshot) {
  std::shared_ptr<cluster_config> old_config;
  using memgraph::coordination::Logger;
  using memgraph::coordination::LoggerWrapper;

  const memgraph::coordination::LogStoreVersion version = GetParam();

  Logger logger("");
  LoggerWrapper my_logger(&logger);
  auto const path = test_folder_ / "serialize_deserialize_snapshot" / "state_machine";
  {
    auto kv_store_ = std::make_shared<memgraph::kvstore::KVStore>(path);

    memgraph::coordination::LogStoreDurability log_store_durability{kv_store_, version};
    CoordinatorStateMachine state_machine{my_logger, log_store_durability};
    CoordinatorStateManagerConfig config{
        .coordinator_id_ = 0,
        .coordinator_port_ = 12345,
        .bolt_port_ = 9090,
        .management_port_ = 20223,
        .coordinator_hostname = "localhost",
        .state_manager_durability_dir_ = test_folder_ / "high_availability" / "coordination" / "state_manager",
        .log_store_durability_ = log_store_durability};
    std::shared_ptr<CoordinatorStateManager> state_manager_ =
        std::make_shared<CoordinatorStateManager>(config, my_logger);
    old_config = state_manager_->load_config();

    auto const coord_instance_aux = CoordinatorInstanceAux{
        .id = config.coordinator_id_, .coordinator_server = "0.0.0.0:12346", .management_server = "0.0.0.0:20223"};

    auto temp_srv_config = std::make_shared<srv_config>(1, 0, coord_instance_aux.coordinator_server,
                                                        nlohmann::json(coord_instance_aux).dump(), false);
    // second coord stored here
    old_config->get_servers().push_back(temp_srv_config);
    state_manager_->save_config(*old_config);
    ASSERT_EQ(old_config->get_servers().size(), 2);

    auto nuraft_snapshot = std::make_shared<snapshot>(1, 1, old_config, 1);
    nuraft::async_result<bool>::handler_type handler = [](auto &e, auto &t) {};
    state_machine.create_snapshot(*nuraft_snapshot, handler);
  }

  {
    auto kv_store_ = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability log_store_durability{kv_store_, version};
    CoordinatorStateMachine state_machine{my_logger, log_store_durability};
    auto last_snapshot = state_machine.last_snapshot();
    ASSERT_EQ(last_snapshot->get_last_log_idx(), 1);
    auto zipped_view =
        std::ranges::views::zip(old_config->get_servers(), last_snapshot->get_last_config()->get_servers());
    std::ranges::for_each(zipped_view, [](auto const &pair) {
      auto &[temp_server, loaded_server] = pair;
      CompareServers(temp_server, loaded_server);
    });
  }
}

INSTANTIATE_TEST_SUITE_P(ParameterizedLogStoreVersionTests, CoordinatorStateMachineTestParam,
                         ::testing::Values(memgraph::coordination::LogStoreVersion::kV1,
                                           memgraph::coordination::LogStoreVersion::kV2),
                         CoordinatorStateMachineTestParam::PrintLogVersionToInt());

TEST_F(CoordinatorStateMachineTest, Marshalling) {
  CoordinatorClusterState cluster_state_ser;
  nlohmann::json cluster_state_json_ser;
  to_json(cluster_state_json_ser, cluster_state_ser);
  auto j = nlohmann::json{{"coord_cluster_state", cluster_state_json_ser.dump()}};

  auto cluster_state_json_deser = j.at("coord_cluster_state").get<std::string>();
  CoordinatorClusterState cluster_state_deser;
  from_json(nlohmann::json::parse(cluster_state_json_deser), cluster_state_deser);
}
