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

#include "nuraft/coordinator_state_manager.hpp"
#include <gtest/gtest.h>
#include "coordination/coordinator_communication_config.hpp"
#include "libnuraft/nuraft.hxx"

#include <range/v3/view.hpp>

using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorStateManager;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::ptr;
using nuraft::srv_config;

class CoordinatorStateManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_state_manager"};
};

TEST_F(CoordinatorStateManagerTest, BasicTest) {
  // TODO(antoniofilipovic) double check ports
  CoordinatorInstanceInitConfig config{1, 12345, 9090, test_folder_ / "high_availability" / "coordination"};
  ptr<CoordinatorStateManager> state_manager_ = cs_new<CoordinatorStateManager>(config);

  auto temp_cluster_config = cs_new<nuraft::cluster_config>();
  auto const c2c = CoordinatorToCoordinatorConfig{
      config.coordinator_id, memgraph::io::network::Endpoint("0.0.0.0", config.bolt_port),
      memgraph::io::network::Endpoint{"0.0.0.0", static_cast<uint16_t>(config.coordinator_port)}};
  auto temp_srv_config = cs_new<srv_config>(config.coordinator_id, 0, c2c.coordinator_server.SocketAddress(),
                                            nlohmann::json(c2c).dump(), false);

  state_manager_->save_config(*temp_cluster_config.get());

  auto loaded_config = state_manager_->load_config();

  ASSERT_EQ(temp_cluster_config->get_servers().size(), loaded_config->get_servers().size());
  auto zipped_view = ranges::views::zip(temp_cluster_config->get_servers(), loaded_config->get_servers());

  std::ranges::for_each(zipped_view, [](auto const &pair) {
    auto const &[temp_server, loaded_server] = pair;
    ASSERT_EQ(temp_server, loaded_server);
  });
}

TEST_F(CoordinatorStateManagerTest, MultipleCoords) {
  // TODO(antoniofilipovic) double check ports
  CoordinatorInstanceInitConfig config{1, 12345, 9090, test_folder_ / "high_availability" / "coordination"};
  ptr<CoordinatorStateManager> state_manager_ = cs_new<CoordinatorStateManager>(config);

  auto temp_cluster_config = cs_new<nuraft::cluster_config>();
  auto const c2c = CoordinatorToCoordinatorConfig{
      config.coordinator_id, memgraph::io::network::Endpoint("0.0.0.0", config.bolt_port),
      memgraph::io::network::Endpoint{"0.0.0.0", static_cast<uint16_t>(config.coordinator_port)}};
  auto temp_srv_config = cs_new<srv_config>(config.coordinator_id, 0, c2c.coordinator_server.SocketAddress(),
                                            nlohmann::json(c2c).dump(), false);

  state_manager_->save_config(*temp_cluster_config.get());

  auto loaded_config = state_manager_->load_config();

  ASSERT_EQ(temp_cluster_config->get_servers().size(), loaded_config->get_servers().size());
  auto zipped_view = ranges::views::zip(temp_cluster_config->get_servers(), loaded_config->get_servers());

  std::ranges::for_each(zipped_view, [](auto const &pair) {
    auto const &[temp_server, loaded_server] = pair;
    ASSERT_EQ(temp_server, loaded_server);
  });
}
