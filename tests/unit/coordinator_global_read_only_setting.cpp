// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "coordination/constants.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_ops_status.hpp"
#include "utils/file.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using memgraph::coordination::CoordinatorInstance;
using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::SetCoordinatorSettingStatus;

// Boots a single-node coordinator which elects itself as leader, so the SET path can append to the Raft log and the
// SHOW path can read it back. No data instances or networking to other nodes are involved.
class CoordinatorGlobalReadOnlySettingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (std::filesystem::exists(test_folder_)) {
      std::filesystem::remove_all(test_folder_);
    }
  }

  void TearDown() override {
    if (std::filesystem::exists(test_folder_)) {
      std::filesystem::remove_all(test_folder_);
    }
  }

  static auto WaitUntilLeaderReady(CoordinatorInstance const &instance) -> bool {
    // ShowInstancesAsLeader returns nullopt until the coordinator is a ready leader.
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (std::chrono::steady_clock::now() < deadline) {
      if (instance.ShowInstancesAsLeader().has_value()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    return false;
  }

  static auto GetShownSetting(CoordinatorInstance const &instance, std::string_view name)
      -> std::optional<std::string> {
    auto const settings = instance.ShowCoordinatorSettings();
    auto const it = std::ranges::find(settings, name, &std::pair<std::string, std::string>::first);
    if (it == settings.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_global_read_only_setting"};

  int32_t const coordinator_id = 1;
  uint16_t const bolt_port = 6690;
  uint16_t const coordinator_port = 40'115;
  uint16_t const management_port = 21'348;
};

TEST_F(CoordinatorGlobalReadOnlySettingTest, SetShowAcceptRejectGlobalReadOnly) {
  auto const instance_config =
      CoordinatorInstanceInitConfig{.coordinator_id = coordinator_id,
                                    .coordinator_port = coordinator_port,
                                    .bolt_port = bolt_port,
                                    .management_port = management_port,
                                    .durability_dir = test_folder_ / "global_read_only_setting",
                                    .coordinator_hostname = "localhost"};

  CoordinatorInstance instance{instance_config};
  ASSERT_TRUE(WaitUntilLeaderReady(instance)) << "Coordinator did not become a ready leader in time.";

  using memgraph::coordination::kGlobalReadOnly;

  // Default value is shown as false.
  ASSERT_EQ(GetShownSetting(instance, kGlobalReadOnly), "false");

  // Accept "true" and reflect it in SHOW.
  ASSERT_EQ(instance.SetCoordinatorSetting(kGlobalReadOnly, "true"), SetCoordinatorSettingStatus::SUCCESS);
  ASSERT_EQ(GetShownSetting(instance, kGlobalReadOnly), "true");

  // Case-insensitive parsing.
  ASSERT_EQ(instance.SetCoordinatorSetting(kGlobalReadOnly, "FALSE"), SetCoordinatorSettingStatus::SUCCESS);
  ASSERT_EQ(GetShownSetting(instance, kGlobalReadOnly), "false");

  // Reject an invalid value; the current value stays unchanged.
  ASSERT_EQ(instance.SetCoordinatorSetting(kGlobalReadOnly, "maybe"), SetCoordinatorSettingStatus::INVALID_ARGUMENT);
  ASSERT_EQ(GetShownSetting(instance, kGlobalReadOnly), "false");

  // Unknown setting name is rejected.
  ASSERT_EQ(instance.SetCoordinatorSetting("not_a_setting", "true"), SetCoordinatorSettingStatus::UNKNOWN_SETTING);
}
