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

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "interpreter_faker.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/exceptions.hpp"
#include "utils/scheduler.hpp"

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
  auto creator = storage->Access(memgraph::storage::WRITE);
  auto other_analytics_mode_reader = storage->Access(memgraph::storage::WRITE);

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

  ASSERT_TRUE(creator->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

INSTANTIATE_TEST_SUITE_P(ParameterizedStorageModeTests, StorageModeTest, ::testing::ValuesIn(storage_modes),
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
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{

      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc, "Failed to access db");
        return db_acc;
      }()  // iile
  };
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker running_interpreter{&interpreter_context, db}, main_interpreter{&interpreter_context, db};
};

TEST_F(StorageModeMultiTxTest, ModeSwitchInactiveTransaction) {
  std::atomic<bool> started{false};
  std::jthread running_thread = std::jthread(
      [this, &started](std::stop_token st, int thread_index) {
        running_interpreter.Interpret("CREATE ();");
        started.store(true, std::memory_order_release);
      },
      0);

  {
    while (!started.load(std::memory_order_acquire)) {
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

  std::atomic<bool> started{false};
  std::atomic<bool> finished{false};
  std::jthread running_thread = std::jthread(
      [this, &started, &finished](std::stop_token st, int thread_index) {
        started.store(true, std::memory_order_release);
        // running interpreter try to change
        running_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
        finished.store(true, std::memory_order_release);
      },
      0);

  {
    while (!started.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    // should not change still
    ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

    main_interpreter.Interpret("COMMIT");

    while (!finished.load(std::memory_order_acquire)) {
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

// Analytical index creation asks for read-only access, so it waits for writers but not for readers.
TEST_F(StorageModeMultiTxTest, AnalyticalIndexCreationAccess) {
  main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

  {
    auto reader = db->storage()->Access(memgraph::storage::READ);
    running_interpreter.Interpret("CREATE INDEX ON :Label1");
    reader->Abort();
  }
  {
    auto acc = db->storage()->Access(memgraph::storage::READ);
    ASSERT_EQ(acc->ListAllIndices().label.size(), 1);
    acc->Abort();
  }

  {
    auto writer = db->storage()->Access(memgraph::storage::WRITE);
    ASSERT_THROW(running_interpreter.Interpret("CREATE INDEX ON :Label2"), memgraph::storage::ReadOnlyAccessTimeout);
    writer->Abort();
  }
}

// Dropping asks for read-only access too: an aborted drop restores the index as it was captured, so
// no writer may run between the drop and the commit.
TEST_F(StorageModeMultiTxTest, AnalyticalIndexDropAccess) {
  main_interpreter.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  ASSERT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
  main_interpreter.Interpret("CREATE INDEX ON :Label1");

  {
    auto writer = db->storage()->Access(memgraph::storage::WRITE);
    ASSERT_THROW(running_interpreter.Interpret("DROP INDEX ON :Label1"), memgraph::storage::ReadOnlyAccessTimeout);
    writer->Abort();
  }

  {
    auto reader = db->storage()->Access(memgraph::storage::READ);
    running_interpreter.Interpret("DROP INDEX ON :Label1");
    reader->Abort();
  }

  auto acc = db->storage()->Access(memgraph::storage::READ);
  ASSERT_EQ(acc->ListAllIndices().label.size(), 0);
  acc->Abort();
}

// The analytical -> transactional direction writes its exit snapshot under READ_ONLY and finishes
// the exclusive flip asynchronously when readers are in flight; these tests pin that contract.
class StorageModeSwitchTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = []() {
    const auto tmp = std::filesystem::temp_directory_path() / "MG_tests_unit_storage_mode_switch";
    std::filesystem::remove_all(tmp);
    return tmp;
  }();  // iile

  void TearDown() override {
    storage.reset();
    std::filesystem::remove_all(data_directory);
  }

  std::unique_ptr<memgraph::storage::InMemoryStorage> storage =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .durability = {
              .storage_directory = data_directory,
              .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
              .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::minutes(20)}}});
};

TEST_F(StorageModeSwitchTest, ReaderDefersSwitchBackUntilRelease) {
  using memgraph::storage::StorageMode;
  storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto reader = storage->Access(memgraph::storage::READ);
  // Returns with the exit snapshot written but the exclusive flip still pending behind the reader.
  storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_ANALYTICAL);

  reader.reset();
  // Accessors requested after the switch wait for the flip, so this doubles as a barrier.
  auto barrier = storage->Access(memgraph::storage::WRITE);
  EXPECT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_TRANSACTIONAL);
}

TEST_F(StorageModeSwitchTest, AccessorsRequestedAfterSwitchStartTransactional) {
  using memgraph::storage::StorageMode;
  storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);

  auto reader = storage->Access(memgraph::storage::READ);
  storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_ANALYTICAL);

  std::atomic<bool> writer_admitted{false};
  std::atomic<StorageMode> mode_at_admission{StorageMode::IN_MEMORY_ANALYTICAL};
  std::jthread writer([&] {
    auto acc = storage->Access(memgraph::storage::WRITE);
    mode_at_admission.store(storage->GetStorageMode(), std::memory_order_release);
    writer_admitted.store(true, std::memory_order_release);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // The pending flip gates every new accessor: nothing is admitted into the half-switched state.
  EXPECT_FALSE(writer_admitted.load(std::memory_order_acquire));

  reader.reset();
  writer.join();
  EXPECT_EQ(mode_at_admission.load(std::memory_order_acquire), StorageMode::IN_MEMORY_TRANSACTIONAL);
}

TEST_F(StorageModeSwitchTest, BackToBackTransitionsSerialize) {
  using memgraph::storage::StorageMode;
  storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);

  auto reader = storage->Access(memgraph::storage::READ);
  storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_ANALYTICAL);

  // Must wait for the pending flip and only then switch, never jump ahead of it.
  std::jthread back_to_analytical([&] { storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL); });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  reader.reset();
  back_to_analytical.join();
  EXPECT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_ANALYTICAL);

  // The intermediate flip to transactional really happened: it left its exit snapshot behind.
  size_t snapshot_count = 0;
  for (auto const &entry : std::filesystem::directory_iterator(data_directory / "snapshots")) {
    snapshot_count += static_cast<size_t>(entry.is_regular_file());
  }
  EXPECT_GE(snapshot_count, 1);
}

TEST_F(StorageModeSwitchTest, CompetingUniqueAccessAbortsSwitch) {
  using memgraph::storage::StorageMode;
  // A unique access can win the lock between the prepared snapshot and the flip; the flip then
  // detects it and aborts, leaving the storage analytical for the user to repeat the switch. The
  // race is genuine, so iterate to land on both orders and assert the invariant that holds for
  // either: the mode is never half-switched, and repeating the switch always converges.
  for (int i = 0; i < 20; ++i) {
    storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
    auto reader = storage->Access(memgraph::storage::READ);
    storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);

    std::jthread interloper([&] { auto unique = storage->UniqueAccess(); });
    reader.reset();
    interloper.join();

    // Admitted only once the flip's exclusive cycle is over, so the outcome is settled after this.
    {
      auto barrier = storage->Access(memgraph::storage::WRITE);
    }
    if (storage->GetStorageMode() == StorageMode::IN_MEMORY_ANALYTICAL) {
      // The interloper won and the switch aborted; repeating it succeeds.
      storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
    }
    ASSERT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_TRANSACTIONAL);
  }
}

TEST_F(StorageModeSwitchTest, ConcurrentReadOnlyIndexDdlAbortsSwitch) {
  using memgraph::storage::StorageMode;
  storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);

  // Analytical index DDL runs under READ_ONLY access: admitted alongside the read-only exit
  // snapshot, it can commit an index the snapshot does not record, invisibly to the
  // transaction-id check (its transaction predates the switch).
  auto ddl = storage->ReadOnlyAccess();
  storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_ANALYTICAL);

  // The exit snapshot is already written; this index is missing from it.
  ASSERT_TRUE(ddl->CreateIndex(storage->NameToLabel("ConcurrentlyIndexed")).has_value());
  ASSERT_TRUE(ddl->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  ddl.reset();

  // The flip compares index definitions against the snapshot's, detects the divergence and aborts.
  {
    auto barrier = storage->Access(memgraph::storage::WRITE);
  }
  ASSERT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_ANALYTICAL);

  // Repeating the switch snapshots the index too and succeeds.
  storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(storage->GetStorageMode(), StorageMode::IN_MEMORY_TRANSACTIONAL);
  auto acc = storage->Access(memgraph::storage::READ);
  ASSERT_EQ(acc->ListAllIndices().label.size(), 1);
}

// nlohmann ADL hooks for StorageMode (storage_mode.hpp): integer wire encoding + range-checked read.
// These back the durable hot/cold cold_stats JSON and SalientConfig, which both rely on plain
// integer encoding — switching to a string form would break read-back of existing entries.
TEST(StorageModeJson, EncodesAsUnderlyingInteger) {
  using memgraph::storage::StorageMode;
  // to_json must emit the underlying integer, not a string (the on-disk format contract).
  for (auto mode :
       {StorageMode::IN_MEMORY_ANALYTICAL, StorageMode::IN_MEMORY_TRANSACTIONAL, StorageMode::ON_DISK_TRANSACTIONAL}) {
    nlohmann::json j = mode;
    ASSERT_TRUE(j.is_number_integer()) << "StorageMode must serialize as an integer";
    EXPECT_EQ(j.get<int>(), std::to_underlying(mode));
  }
}

TEST(StorageModeJson, RoundTrips) {
  using memgraph::storage::StorageMode;
  for (auto mode :
       {StorageMode::IN_MEMORY_ANALYTICAL, StorageMode::IN_MEMORY_TRANSACTIONAL, StorageMode::ON_DISK_TRANSACTIONAL}) {
    nlohmann::json j = mode;
    EXPECT_EQ(j.get<StorageMode>(), mode);
  }
}

TEST(StorageModeJson, ReadsRawIntegerFromOlderEntries) {
  using memgraph::storage::StorageMode;
  // A pre-hook durable entry / SalientConfig stores the bare integer (nlohmann's default enum form).
  // The hook must read it back identically — proving the format is unchanged.
  EXPECT_EQ(nlohmann::json(0).get<StorageMode>(), StorageMode::IN_MEMORY_ANALYTICAL);
  EXPECT_EQ(nlohmann::json(1).get<StorageMode>(), StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_EQ(nlohmann::json(2).get<StorageMode>(), StorageMode::ON_DISK_TRANSACTIONAL);
}

TEST(StorageModeJson, OutOfRangeFallsBackWithoutThrowing) {
  using memgraph::storage::StorageMode;
  // A corrupt/out-of-range value must NOT blind-cast to a garbage enum (the latent gap in the
  // built-in from_json) — from_json range-checks via NumToEnum and falls back to a safe default.
  StorageMode out{};
  nlohmann::json corrupt = 99;
  ASSERT_NO_THROW(out = corrupt.get<StorageMode>());
  EXPECT_EQ(out, StorageMode::IN_MEMORY_TRANSACTIONAL);
}
