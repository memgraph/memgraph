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

#include "nuraft/coordinator_log_store.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "io/network/endpoint.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/raft_log_action.hpp"
#include "utils/file.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::CoordinatorLogStore;
using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftLogAction;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::utils::UUID;
;

using nuraft::log_entry;

// No networking communication in this test.
class CoordinatorLogStoreTests : public ::testing::Test {
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
                                     "MG_tests_unit_coordinator_log_store_tests"};
};

TEST_F(CoordinatorLogStoreTests, TestBasicSerialization) {
  CoordinatorToReplicaConfig config{.instance_name = "instance3",
                                    .mgt_server = Endpoint{"127.0.0.1", 10112},
                                    .replication_client_info = {.instance_name = "instance_name",
                                                                .replication_mode = ReplicationMode::ASYNC,
                                                                .replication_server = Endpoint{"127.0.0.1", 10001}},
                                    .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                    .instance_down_timeout_sec = std::chrono::seconds{5},
                                    .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                    .ssl = std::nullopt};

  auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);

  {
    CoordinatorLogStore log_store{test_folder_ / "TestBasicSerialization"};
    ASSERT_EQ(log_store.next_slot(), 1);
    ASSERT_EQ(log_store.start_index(), 1);

    auto register_instance_log_entry = cs_new<log_entry>(1, buffer, nuraft::log_val_type::app_log);
    log_store.append(register_instance_log_entry);

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
  }

  {
    CoordinatorLogStore log_store{test_folder_ / "TestBasicSerialization"};
    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);

    auto entry = log_store.entry_at(1);

    auto [payload, action] = CoordinatorStateMachine::DecodeLog(entry->get_buf());

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
    ASSERT_EQ(action, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
    ASSERT_EQ(config, std::get<CoordinatorToReplicaConfig>(payload));
  }
}
