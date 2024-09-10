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

#include "nuraft/coordinator_state_machine.hpp"
#include "kvstore/kvstore.hpp"
#include "nuraft/constants_log_durability.hpp"
#include "nuraft/coordinator_state_manager.hpp"
#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

#include "libnuraft/nuraft.hxx"

#include <range/v3/view.hpp>
#include "coordination/coordinator_communication_config.hpp"

using memgraph::coordination::CoordinatorStateManager;
using memgraph::coordination::CoordinatorStateManagerConfig;
using memgraph::coordination::CoordinatorToCoordinatorConfig;

using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::InstanceUUIDUpdate;
using memgraph::coordination::RaftLogAction;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::utils::UUID;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::ptr;
using nuraft::snapshot;
using nuraft::srv_config;

namespace {
void CompareServers(ptr<srv_config> const &temp_server, ptr<srv_config> const &loaded_server) {
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

TEST_F(CoordinatorStateMachineTest, SerializeRegisterReplicationInstance) {
  auto config = CoordinatorToReplicaConfig{
      .instance_name = "instance3",
      .mgt_server = Endpoint{"127.0.0.1", mgt_port},
      .bolt_server = Endpoint{"127.0.0.1", bolt_port},
      .replication_client_info = {.instance_name = "instance_name",
                                  .replication_mode = ReplicationMode::ASYNC,
                                  .replication_server = Endpoint{"127.0.0.1", replication_port}},
      .instance_health_check_frequency_sec = std::chrono::seconds{1},
      .instance_down_timeout_sec = std::chrono::seconds{5},
      .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
      .ssl = std::nullopt};

  ptr<buffer> data = CoordinatorStateMachine::SerializeRegisterInstance(config);
  buffer_serializer bs(*data);
  auto const expected = nlohmann::json{{"action", RaftLogAction::REGISTER_REPLICATION_INSTANCE}, {"info", config}};
  ASSERT_EQ(bs.get_str(), expected.dump());
}

TEST_F(CoordinatorStateMachineTest, SerializeUnregisterReplicationInstance) {
  auto config = CoordinatorToReplicaConfig{
      .instance_name = "instance3",
      .mgt_server = Endpoint{"127.0.0.1", mgt_port},
      .bolt_server = Endpoint{"127.0.0.1", bolt_port},
      .replication_client_info = {.instance_name = "instance_name",
                                  .replication_mode = ReplicationMode::ASYNC,
                                  .replication_server = Endpoint{"127.0.0.1", replication_port}},
      .instance_health_check_frequency_sec = std::chrono::seconds{1},
      .instance_down_timeout_sec = std::chrono::seconds{5},
      .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
      .ssl = std::nullopt};

  CoordinatorStateMachine::SerializeRegisterInstance(config);
  ptr<buffer> data = CoordinatorStateMachine::SerializeUnregisterInstance("instance3");
  buffer_serializer bs(*data);

  auto const expected =
      nlohmann::json{{"action", RaftLogAction::UNREGISTER_REPLICATION_INSTANCE}, {"info", "instance3"}};
  ASSERT_EQ(bs.get_str(), expected.dump());
}

TEST_F(CoordinatorStateMachineTest, SerializeSetInstanceToMain) {
  auto config = CoordinatorToReplicaConfig{
      .instance_name = "instance3",
      .mgt_server = Endpoint{"127.0.0.1", mgt_port},
      .bolt_server = Endpoint{"127.0.0.1", bolt_port},
      .replication_client_info = {.instance_name = "instance_name",
                                  .replication_mode = ReplicationMode::ASYNC,
                                  .replication_server = Endpoint{"127.0.0.1", replication_port}},
      .instance_health_check_frequency_sec = std::chrono::seconds{1},
      .instance_down_timeout_sec = std::chrono::seconds{5},
      .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
      .ssl = std::nullopt};

  CoordinatorStateMachine::SerializeRegisterInstance(config);

  {
    auto const uuid_update_obj = InstanceUUIDUpdate{.instance_name = "instance3", .uuid = UUID{}};
    ptr<buffer> data = CoordinatorStateMachine::SerializeSetInstanceAsMain(uuid_update_obj);
    buffer_serializer bs(*data);
    auto const expected = nlohmann::json{{"action", RaftLogAction::SET_INSTANCE_AS_MAIN}, {"info", uuid_update_obj}};
    ASSERT_EQ(bs.get_str(), expected.dump());
  }

  {
    ptr<buffer> data = CoordinatorStateMachine::SerializeSetInstanceAsReplica("instance3");
    buffer_serializer bs(*data);
    auto const expected = nlohmann::json{{"action", RaftLogAction::SET_INSTANCE_AS_REPLICA}, {"info", "instance3"}};
    ASSERT_EQ(bs.get_str(), expected.dump());
  }
}

TEST_F(CoordinatorStateMachineTest, SerializeUpdateUUID) {
  auto uuid = UUID{};

  ptr<buffer> data = CoordinatorStateMachine::SerializeUpdateUUIDForNewMain(uuid);
  buffer_serializer bs(*data);
  auto const expected = nlohmann::json{{"action", RaftLogAction::UPDATE_UUID_OF_NEW_MAIN}, {"info", uuid}};
  ASSERT_EQ(bs.get_str(), expected.dump());
}

TEST_P(CoordinatorStateMachineTestParam, SerializeDeserializeSnapshot) {
  ptr<cluster_config> old_config;
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
    CoordinatorStateManagerConfig config{0,
                                         12345,
                                         9090,
                                         20223,
                                         test_folder_ / "high_availability" / "coordination" / "state_manager",
                                         "localhost",
                                         log_store_durability};
    ptr<CoordinatorStateManager> state_manager_ = cs_new<CoordinatorStateManager>(config, my_logger);
    old_config = state_manager_->load_config();
    auto const c2c =
        CoordinatorToCoordinatorConfig{config.coordinator_id_, memgraph::io::network::Endpoint("0.0.0.0", 9091),
                                       memgraph::io::network::Endpoint{"0.0.0.0", 12346},
                                       memgraph::io::network::Endpoint("0.0.0.0", 20223), "localhost"};
    auto temp_srv_config =
        cs_new<srv_config>(1, 0, c2c.coordinator_server.SocketAddress(), nlohmann::json(c2c).dump(), false);
    // second coord stored here
    old_config->get_servers().push_back(temp_srv_config);
    state_manager_->save_config(*old_config);
    ASSERT_EQ(old_config->get_servers().size(), 2);

    auto nuraft_snapshot = cs_new<snapshot>(1, 1, old_config, 1);
    nuraft::async_result<bool>::handler_type handler = [](auto &e, auto &t) {};
    state_machine.create_snapshot(*nuraft_snapshot, handler);
  }

  {
    auto kv_store_ = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability log_store_durability{kv_store_, version};
    CoordinatorStateMachine state_machine{my_logger, log_store_durability};
    auto last_snapshot = state_machine.last_snapshot();
    ASSERT_EQ(last_snapshot->get_last_log_idx(), 1);
    auto zipped_view = ranges::views::zip(old_config->get_servers(), last_snapshot->get_last_config()->get_servers());
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
