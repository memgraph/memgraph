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

#include <gtest/gtest.h>
#include <chrono>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include "interpreter_faker.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"
#include "utils/exceptions.hpp"

class StorageModeTest : public ::testing::TestWithParam<memgraph::storage::StorageMode> {
 public:
  struct PrintStringParamToName {
    std::string operator()(const testing::TestParamInfo<memgraph::storage::StorageMode> &info) {
      return std::string(StorageModeToString(static_cast<memgraph::storage::StorageMode>(info.param)));
    }
  };
};

// you should be able to see nodes if there is analytics mode
TEST_P(StorageModeTest, Mode) {
  const memgraph::storage::StorageMode storage_mode = GetParam();

  std::unique_ptr<memgraph::storage::Storage> storage =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .transaction{.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}});

  static_cast<memgraph::storage::InMemoryStorage *>(storage.get())->SetStorageMode(storage_mode);
  auto creator = storage->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN);
  auto other_analytics_mode_reader = storage->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN);

  ASSERT_EQ(CountVertices(*creator, memgraph::storage::View::OLD), 0);
  ASSERT_EQ(CountVertices(*other_analytics_mode_reader, memgraph::storage::View::OLD), 0);

  static constexpr int vertex_creation_count = 10;
  {
    for (size_t i = 1; i <= vertex_creation_count; i++) {
      creator->CreateVertex();

      int64_t expected_vertices_count = storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL ? i : 0;
      ASSERT_EQ(CountVertices(*creator, memgraph::storage::View::OLD), expected_vertices_count);
      ASSERT_EQ(CountVertices(*other_analytics_mode_reader, memgraph::storage::View::OLD), expected_vertices_count);
    }
  }

  ASSERT_FALSE(creator->Commit().HasError());
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageModeTests, StorageModeTest, ::testing::ValuesIn(storage_modes),
                        StorageModeTest::PrintStringParamToName());

class StorageModeMultiTxTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = []() {
    const auto tmp = std::filesystem::temp_directory_path() / "MG_tests_unit_storage_mode";
    std::filesystem::remove_all(tmp);
    return tmp;
  }();  // iile

  void TearDown() override { std::filesystem::remove_all(data_directory); }

  memgraph::storage::Config config{.durability.storage_directory = data_directory,
                                   .disk.main_storage_directory = data_directory / "disk"};

  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc, "Failed to access db");
        return db_acc;
      }()  // iile
  };
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{}, nullptr, &repl_state, system_state};
  InterpreterFaker running_interpreter{&interpreter_context, db}, main_interpreter{&interpreter_context, db};
};

TEST_F(StorageModeMultiTxTest, ModeSwitchInactiveTransaction) {
  bool started = false;
  std::jthread running_thread = std::jthread(
      [this, &started](std::stop_token st, int thread_index) {
        running_interpreter.Interpret("CREATE ();");
        started = true;
      },
      0);

  {
    while (!started) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
    main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

    // should change state
    ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

    // finish thread
    running_thread.request_stop();
  }
}

TEST_F(StorageModeMultiTxTest, ModeSwitchActiveTransaction) {
  // transactional state
  ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  main_interpreter.Interpret("BEGIN");

  bool started = false;
  bool finished = false;
  std::jthread running_thread = std::jthread(
      [this, &started, &finished](std::stop_token st, int thread_index) {
        started = true;
        // running interpreter try to change
        running_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
        finished = true;
      },
      0);

  {
    while (!started) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    // should not change still
    ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

    main_interpreter.Interpret("COMMIT");

    while (!finished) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    // should change state
    ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

    // finish thread
    running_thread.request_stop();
  }
}

TEST_F(StorageModeMultiTxTest, ErrorChangeIsolationLevel) {
  ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

  // should change state
  ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

  ASSERT_THROW(running_interpreter.Interpret("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED;"),
               memgraph::query::IsolationLevelModificationInAnalyticsException);
}
