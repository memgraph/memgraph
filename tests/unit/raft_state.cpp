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

#include "coordination/raft_state.hpp"
#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

#include "libnuraft/nuraft.hxx"

using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::RaftState;
using memgraph::io::network::Endpoint;
using nuraft::ptr;

class RaftStateTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_raft_state"};
};

TEST_F(RaftStateTest, RaftStateEmptyMetadata) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};

  auto const config = CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 1234, .bolt_port = 7688};

  auto raft_state = RaftState::MakeRaftState(config, std::move(become_leader_cb), std::move(become_follower_cb));

  ASSERT_EQ(raft_state.InstanceName(), "coordinator_1");
  ASSERT_EQ(raft_state.RaftSocketAddress(), "127.0.0.1:1234");
  ASSERT_TRUE(raft_state.IsLeader());
  ASSERT_TRUE(raft_state.GetReplicationInstances().empty());

  auto const coords = raft_state.GetCoordinatorInstances();
  ASSERT_EQ(coords.size(), 1);
  auto const &coord_instance = coords[0];
  auto const &coord_config = CoordinatorToCoordinatorConfig{.coordinator_id = 1,
                                                            .bolt_server = Endpoint{"127.0.0.1", 7688},
                                                            .coordinator_server = Endpoint{"127.0.0.1", 1234}};
  ASSERT_EQ(coord_instance.config, coord_config);
}
