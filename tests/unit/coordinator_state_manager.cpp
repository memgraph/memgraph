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

#include "coordination/coordinator_state_manager.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "coordination/logger.hpp"

#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <nlohmann/json.hpp>
#include <range/v3/view.hpp>

using memgraph::coordination::CoordinatorInstanceAux;
using memgraph::coordination::CoordinatorStateManager;
using memgraph::coordination::CoordinatorStateManagerConfig;
using memgraph::coordination::LogStoreDurability;
using memgraph::coordination::LogStoreVersion;
using memgraph::kvstore::KVStore;
using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::ptr;
using nuraft::srv_config;

namespace {
void CompareServers(ptr<srv_config> const &temp_server, ptr<srv_config> const &loaded_server) {
  ASSERT_EQ(temp_server->get_id(), loaded_server->get_id());
  ASSERT_EQ(temp_server->get_endpoint(), loaded_server->get_endpoint());
  ASSERT_EQ(temp_server->get_aux(), loaded_server->get_aux());
}
}  // namespace

// No networking is used in this test
class CoordinatorStateManagerTest : public ::testing::Test {
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
                                     "MG_tests_unit_coordinator_state_manager"};
};

TEST_F(CoordinatorStateManagerTest, SingleCoord) {
  CoordinatorStateManagerConfig config{
      .coordinator_id_ = 1,
      .coordinator_port_ = 12345,
      .bolt_port_ = 9090,
      .management_port_ = 20456,
      .coordinator_hostname = "localhost",
      .state_manager_durability_dir_ = test_folder_ / "high_availability" / "coordination",
      .log_store_durability_ = LogStoreDurability{
          .durability_store_ = std::make_shared<KVStore>(test_folder_ / "high_availability" / "logs"),
          .stored_log_store_version_ = LogStoreVersion::kV2}};
  using memgraph::coordination::Logger;
  using memgraph::coordination::LoggerWrapper;

  Logger logger("");
  LoggerWrapper my_logger(&logger);
  ptr<cluster_config> old_config;
  {
    ptr<CoordinatorStateManager> state_manager_ = cs_new<CoordinatorStateManager>(config, my_logger);
    old_config = state_manager_->load_config();
    state_manager_->save_config(*old_config);
  }

  ptr<CoordinatorStateManager> state_manager_copy = cs_new<CoordinatorStateManager>(config, my_logger);
  auto loaded_config = state_manager_copy->load_config();

  ASSERT_EQ(old_config->get_servers().size(), loaded_config->get_servers().size());
  auto zipped_view = ranges::views::zip(old_config->get_servers(), loaded_config->get_servers());
  std::ranges::for_each(zipped_view, [](auto const &pair) {
    auto &[temp_server, loaded_server] = pair;
    CompareServers(temp_server, loaded_server);
  });
}

TEST_F(CoordinatorStateManagerTest, MultipleCoords) {
  // 1st coord stored here
  ptr<cluster_config> old_config;
  CoordinatorStateManagerConfig config{
      .coordinator_id_ = 0,
      .coordinator_port_ = 12345,
      .bolt_port_ = 9090,
      .management_port_ = 20345,
      .coordinator_hostname = "localhost",
      .state_manager_durability_dir_ = test_folder_ / "high_availability" / "coordination",
      .log_store_durability_ = LogStoreDurability{
          .durability_store_ = std::make_shared<KVStore>(test_folder_ / "high_availability" / "logs"),
          .stored_log_store_version_ = LogStoreVersion::kV2}};
  using memgraph::coordination::Logger;
  using memgraph::coordination::LoggerWrapper;

  Logger logger("");
  LoggerWrapper my_logger(&logger);
  {
    ptr<CoordinatorStateManager> state_manager_ = cs_new<CoordinatorStateManager>(config, my_logger);
    old_config = state_manager_->load_config();

    auto const coord_instance_aux = CoordinatorInstanceAux{
        .id = config.coordinator_id_, .coordinator_server = "0.0.0.0:12346", .management_server = "0.0.0.0:2320"};

    auto temp_srv_config = cs_new<srv_config>(1, 0, coord_instance_aux.coordinator_server,
                                              nlohmann::json(coord_instance_aux).dump(), false);
    // second coord stored here
    old_config->get_servers().push_back(temp_srv_config);
    state_manager_->save_config(*old_config);
    ASSERT_EQ(old_config->get_servers().size(), 2);
  }

  ptr<CoordinatorStateManager> state_manager_copy = cs_new<CoordinatorStateManager>(config, my_logger);
  auto loaded_config = state_manager_copy->load_config();

  ASSERT_EQ(old_config->get_servers().size(), loaded_config->get_servers().size());
  auto zipped_view = ranges::views::zip(old_config->get_servers(), loaded_config->get_servers());

  std::ranges::for_each(zipped_view, [](auto const &pair) {
    auto &[temp_server, loaded_server] = pair;
    CompareServers(temp_server, loaded_server);
  });
}
