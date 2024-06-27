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

#include "flags/coord_flag_env_handler.hpp"

#include <gtest/gtest.h>

#include <stdlib.h>

class CoordinationSetupTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  // NOTE: Update this function if new environment variables are added
  void TearDown() override {
    unsetenv(memgraph::flags::kMgManagementPort);
    unsetenv(memgraph::flags::kMgCoordinatorPort);
    unsetenv(memgraph::flags::kMgCoordinatorId);
    unsetenv(memgraph::flags::kMgNuRaftLogFile);
    unsetenv(memgraph::flags::kMgCoordinatorHostname);
  }
};

TEST_F(CoordinationSetupTest, CoordinationSetupSimple) {
  memgraph::flags::CoordinationSetup coord_setup(1, 2, 3, "nuraft_log_file", false, "localhost");
  EXPECT_EQ(coord_setup.management_port, 1);
  EXPECT_EQ(coord_setup.coordinator_port, 2);
  EXPECT_EQ(coord_setup.coordinator_id, 3);
  EXPECT_EQ(coord_setup.nuraft_log_file, "nuraft_log_file");
  EXPECT_EQ(coord_setup.coordinator_hostname, "localhost");
}

TEST_F(CoordinationSetupTest, CoordinationSetupAll) {
  using namespace memgraph::flags;
  // If the environment variable named by envname already exists and the value of overwrite is non-zero, the function
  // shall return success and the environment shall be updated.
  if (setenv(kMgManagementPort, "10011", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_MANAGEMENT_PORT environment variable";
  }

  if (setenv(kMgCoordinatorPort, "10111", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_COORDINATOR_PORT environment variable";
  }

  if (setenv(kMgCoordinatorId, "1", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_COORDINATOR_ID environment variable";
  }

  if (setenv(kMgNuRaftLogFile, "nuraft_log_file", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_NURAFT_LOG_FILE environment variable";
  }

  if (setenv(kMgCoordinatorHostname, "memgraph-svc-cluster-1.dns", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_COORDINATOR_HOSTNAME environment variable";
  }

  memgraph::flags::SetFinalCoordinationSetup();
  auto const &coordination_setup = memgraph::flags::CoordinationSetupInstance();

  EXPECT_EQ(coordination_setup.management_port, 10011);
  EXPECT_EQ(coordination_setup.coordinator_port, 10111);
  EXPECT_EQ(coordination_setup.coordinator_id, 1);
  EXPECT_EQ(coordination_setup.nuraft_log_file, "nuraft_log_file");
  EXPECT_EQ(coordination_setup.coordinator_hostname, "memgraph-svc-cluster-1.dns");
}

TEST_F(CoordinationSetupTest, CoordinatorSetupPartial) {
  using namespace memgraph::flags;
  if (setenv(kMgCoordinatorPort, "10111", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_COORDINATOR_PORT environment variable";
  }

  if (setenv(kMgCoordinatorId, "1", 1) != 0) {
    FAIL() << "Failed to set MEMGRAPH_COORDINATOR_ID environment variable";
  }

  memgraph::flags::SetFinalCoordinationSetup();
  auto const &coordination_setup = memgraph::flags::CoordinationSetupInstance();
  EXPECT_EQ(coordination_setup.management_port, 0);
  EXPECT_EQ(coordination_setup.coordinator_port, 10111);
  EXPECT_EQ(coordination_setup.coordinator_id, 1);
  EXPECT_EQ(coordination_setup.nuraft_log_file, "");
  EXPECT_EQ(coordination_setup.coordinator_hostname, "");
}
