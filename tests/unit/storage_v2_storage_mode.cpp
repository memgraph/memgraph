

#include <gtest/gtest.h>
#include <chrono>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include "interpreter_faker.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"

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

  memgraph::storage::Storage storage{
      {.transaction{.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}}};
  storage.SetStorageMode(storage_mode);
  auto creator = storage.Access();
  auto other_analytics_mode_reader = storage.Access();

  ASSERT_EQ(CountVertices(creator, memgraph::storage::View::OLD), 0);
  ASSERT_EQ(CountVertices(other_analytics_mode_reader, memgraph::storage::View::OLD), 0);

  static constexpr int vertex_creation_count = 10;
  {
    for (size_t i = 1; i <= vertex_creation_count; i++) {
      creator.CreateVertex();

      int64_t expected_vertices_count = storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL ? i : 0;
      ASSERT_EQ(CountVertices(creator, memgraph::storage::View::OLD), expected_vertices_count);
      ASSERT_EQ(CountVertices(other_analytics_mode_reader, memgraph::storage::View::OLD), expected_vertices_count);
    }
  }

  ASSERT_FALSE(creator.Commit().HasError());
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageModeTests, StorageModeTest, ::testing::ValuesIn(storage_modes),
                        StorageModeTest::PrintStringParamToName());

class StorageModeMulitTxTest : public ::testing::Test {
 protected:
  memgraph::storage::Storage db_;
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_storage_mode"};
  memgraph::query::InterpreterContext interpreter_context{&db_, {}, data_directory};
  InterpreterFaker running_interpreter{&interpreter_context}, main_interpreter{&interpreter_context};
};

TEST_F(StorageModeMulitTxTest, ActiveTransaction) {
  bool started = false;
  std::jthread running_thread = std::jthread(
      [this, &started](std::stop_token st, int thread_index) {
        running_interpreter.Interpret("BEGIN");
        started = true;
      },
      0);

  {
    while (!started) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
    main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

    // should not change still
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

    running_interpreter.Interpret("COMMIT");

    // should change state
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

    // finish thread
    running_thread.request_stop();
  }
}

TEST_F(StorageModeMulitTxTest, InActiveTransaction) {
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
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
    main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

    // should change state
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

    // finish thread
    running_thread.request_stop();
  }
}

TEST_F(StorageModeMulitTxTest, ErrorChangeIsolationLevel) {
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
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
    main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

    // should change state
    ASSERT_EQ(db_.GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

    ASSERT_THROW(running_interpreter.Interpret("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITED;"),
                 memgraph::query::IsolationLevelModificationInAnalyticsException);

    // finish thread
    running_thread.request_stop();
  }
}
