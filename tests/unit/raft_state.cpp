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

using memgraph::coordination::RaftState;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;

class RaftStateTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_raft_state"};
};

TEST_F(RaftStateTest, CreationOfRaftState) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};

  auto raft_state = RaftState::MakeRaftState(std::move(become_leader_cb), std::move(become_follower_cb));
  spdlog::info("Raft state instance name: {}", raft_state.InstanceName());
  spdlog::info("Raft state socket address: {}", raft_state.RaftSocketAddress());
}
