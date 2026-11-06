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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <concepts>
#include <expected>
#include <filesystem>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "dbms/database_handler.hpp"
#include "dbms/dbms_name.hpp"
#include "dbms/global.hpp"
#include "memory/db_arena.hpp"
#include "metrics/prometheus_metrics.hpp"

#include "license/license.hpp"
#include "query_plan_common.hpp"
#include "replication/state.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_database"};

memgraph::storage::Config default_conf(std::string name = "") {
  return {.durability = {.storage_directory = storage_directory / name,
                         .snapshot_wal_mode =
                             memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
          .disk = {.main_storage_directory = storage_directory / name / "disk"},
          .salient.name = name.empty() ? std::string{"memgraph"} : name};
}

class DBMS_Database : public ::testing::Test {
 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

 private:
  void Clear() {
    if (std::filesystem::exists(storage_directory)) {
      std::filesystem::remove_all(storage_directory);
    }
  }
};

#ifdef MG_ENTERPRISE
TEST_F(DBMS_Database, New) {
  memgraph::dbms::DatabaseHandler db_handler;
  {
    ASSERT_FALSE(db_handler.GetConfig("db1"));
  }
  {  // With custom config
    memgraph::storage::Config db_config{
        .durability = {.storage_directory = storage_directory / "db2",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "disk"},
        .salient.name = "db2"};
    auto db2 = db_handler.New(db_config);
    ASSERT_TRUE(db2.has_value() && db2.value());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db2"));
  }
  {
    // With default config
    auto db3 = db_handler.New(default_conf("db3"));
    ASSERT_TRUE(db3.has_value() && db3.value());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db3"));
    auto db4 = db_handler.New(default_conf("four"));
    ASSERT_TRUE(db4.has_value() && db4.value());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "four"));
    auto db5 = db_handler.New(default_conf("db3"));
    ASSERT_EQ(db5, std::unexpected{memgraph::dbms::NewError::EXISTS});
  }

  auto all = db_handler.All();
  std::sort(all.begin(), all.end());
  ASSERT_EQ(all.size(), 3);
  ASSERT_EQ(all[0], "db2");
  ASSERT_EQ(all[1], "db3");
  ASSERT_EQ(all[2], "four");
}

TEST_F(DBMS_Database, Get) {
  memgraph::dbms::DatabaseHandler db_handler;

  auto db1 = db_handler.New(default_conf("db1"));
  auto db2 = db_handler.New(default_conf("db2"));
  auto db3 = db_handler.New(default_conf("db3"));

  ASSERT_TRUE(db1.has_value());
  ASSERT_TRUE(db2.has_value());
  ASSERT_TRUE(db3.has_value());

  auto get_db1 = db_handler.Get("db1");
  auto get_db2 = db_handler.Get("db2");
  auto get_db3 = db_handler.Get("db3");

  ASSERT_TRUE(get_db1 && *get_db1 == db1.value());
  ASSERT_TRUE(get_db2 && *get_db2 == db2.value());
  ASSERT_TRUE(get_db3 && *get_db3 == db3.value());

  ASSERT_FALSE(db_handler.Get("db123"));
  ASSERT_FALSE(db_handler.Get("db2 "));
  ASSERT_FALSE(db_handler.Get(" db3"));
}

TEST_F(DBMS_Database, Delete) {
  memgraph::dbms::DatabaseHandler db_handler;

  auto db1 = db_handler.New(default_conf("db1"));
  auto db2 = db_handler.New(default_conf("db2"));
  auto db3 = db_handler.New(default_conf("db3"));

  ASSERT_TRUE(db1.has_value());
  ASSERT_TRUE(db2.has_value());
  ASSERT_TRUE(db3.has_value());

  {
    // Release accessor to storage
    db1.value().reset();
    // Delete from handler
    ASSERT_TRUE(db_handler.TryDelete("db1"));
    ASSERT_FALSE(db_handler.Get("db1"));
    auto all = db_handler.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }

  {
    ASSERT_THROW(db_handler.TryDelete("db0"), memgraph::utils::BasicException);
    ASSERT_THROW(db_handler.TryDelete("db1"), memgraph::utils::BasicException);
    auto all = db_handler.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }
}

TEST_F(DBMS_Database, DeleteAndRecover) {
  memgraph::license::global_license_checker.EnableTesting();
  memgraph::dbms::DatabaseHandler db_handler;

  {
    auto db1 = db_handler.New(default_conf("db1"));
    auto db2 = db_handler.New(default_conf("db2"));

    memgraph::storage::Config conf_w_snap{
        .durability = {.storage_directory = storage_directory / "db3",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_on_exit = true},
        .disk = {.main_storage_directory = storage_directory / "db3" / "disk"},
        .salient.name = "db3"};

    auto db3 = db_handler.New(conf_w_snap);

    ASSERT_TRUE(db1.has_value());
    ASSERT_TRUE(db2.has_value());
    ASSERT_TRUE(db3.has_value());

    // Add data to graphs
    {
      const memgraph::memory::DbArenaScope arena_scope{&db1.value()->Arena()};
      auto storage_dba = db1.value()->Access(memgraph::storage::WRITE);
      memgraph::query::DbAccessor dba{storage_dba.get()};
      memgraph::query::VertexAccessor v1{dba.InsertVertex()};
      memgraph::query::VertexAccessor v2{dba.InsertVertex()};
      ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l11")).has_value());
      ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l12")).has_value());
      ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      const memgraph::memory::DbArenaScope arena_scope{&db3.value()->Arena()};
      auto storage_dba = db3.value()->Access(memgraph::storage::WRITE);
      memgraph::query::DbAccessor dba{storage_dba.get()};
      memgraph::query::VertexAccessor v1{dba.InsertVertex()};
      memgraph::query::VertexAccessor v2{dba.InsertVertex()};
      memgraph::query::VertexAccessor v3{dba.InsertVertex()};
      ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l31")).has_value());
      ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l32")).has_value());
      ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l33")).has_value());
      ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  // Delete from handler
  ASSERT_TRUE(db_handler.TryDelete("db1"));
  ASSERT_TRUE(db_handler.TryDelete("db2"));
  ASSERT_TRUE(db_handler.TryDelete("db3"));

  {
    // Recover graphs (only db3)
    auto db1 = db_handler.New(default_conf("db1"));
    auto db2 = db_handler.New(default_conf("db2"));

    memgraph::storage::Config conf_w_rec{
        .durability = {.storage_directory = storage_directory / "db3",
                       .recover_on_startup = true,
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "db3" / "disk"},
        .salient.name = "db3"};

    auto db3 = db_handler.New(conf_w_rec);

    // Check content
    {
      // Empty
      const memgraph::memory::DbArenaScope arena_scope{&db1.value()->Arena()};
      auto storage_dba = db1.value()->Access(memgraph::storage::WRITE);
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Empty
      const memgraph::memory::DbArenaScope arena_scope{&db2.value()->Arena()};
      auto storage_dba = db2.value()->Access(memgraph::storage::WRITE);
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Full
      const memgraph::memory::DbArenaScope arena_scope{&db3.value()->Arena()};
      auto storage_dba = db3.value()->Access(memgraph::storage::WRITE);
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 3);
    }
  }
}

// The metrics registration is made before recovery runs, so it must follow the uuid recovery adopts.
TEST_F(DBMS_Database, MetricsFollowUuidAdoptedDuringRecovery) {
  memgraph::license::global_license_checker.EnableTesting();
  memgraph::dbms::DatabaseHandler db_handler;

  auto make_conf = [](memgraph::utils::UUID uuid, bool recover) {
    memgraph::storage::Config conf{
        .durability = {.storage_directory = storage_directory / "recov",
                       .recover_on_startup = recover,
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_on_exit = true},
        .disk = {.main_storage_directory = storage_directory / "recov" / "disk"},
        .salient.name = "recov"};
    conf.salient.uuid = uuid;
    return conf;
  };

  memgraph::utils::UUID const snapshot_uuid{};
  {
    auto db = db_handler.New(make_conf(snapshot_uuid, false));
    ASSERT_TRUE(db.has_value());
    const memgraph::memory::DbArenaScope arena_scope{&db.value()->Arena()};
    auto storage_dba = db.value()->Access(memgraph::storage::WRITE);
    memgraph::query::DbAccessor dba{storage_dba.get()};
    dba.InsertVertex();
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_TRUE(db_handler.TryDelete("recov"));

  // Rebuild from the snapshot, with a config carrying a different uuid.
  memgraph::utils::UUID const config_uuid{};
  ASSERT_NE(std::string(config_uuid), std::string(snapshot_uuid));
  auto db = db_handler.New(make_conf(config_uuid, true));
  ASSERT_TRUE(db.has_value());

  ASSERT_EQ(std::string(db.value()->uuid()), std::string(snapshot_uuid));

  EXPECT_TRUE(memgraph::metrics::Metrics().GetDbMetricsInfo(db.value()->uuid()).has_value());
}

// Rejected: empty, >63 bytes, malformed UTF-8, Cc control code points, leading/trailing Unicode whitespace.
TEST_F(DBMS_Database, DatabaseNameIsWellFormedBoundaries) {
  using namespace std::string_literals;

  struct Case {
    std::string name;
    bool well_formed;
    const char *what;
  };

  const Case cases[] = {
      {"", false, "empty"},
      {" ", false, "space only"},
      {" a", false, "leading space"},
      {"a ", false, "trailing space"},
      {"a\xC2\xA0", false, "trailing NBSP"},
      {"a\nb", false, "embedded LF"},
      {"a\rb", false, "embedded CR"},
      {"a\tb", false, "embedded TAB"},
      {"a\0b"s, false, "embedded NUL"},
      {"a\xC2\x85", false, "U+0085 control"},
      {"\x80", false, "lone continuation byte"},
      {"\xC3", false, "truncated sequence"},
      {"\xC0\xAF", false, "overlong encoding"},
      {"\xED\xA0\x80", false, "UTF-16 surrogate"},
      {std::string(64, 'x'), false, "64 ASCII bytes"},
      {[] {
         std::string s;
         for (int i = 0; i < 32; ++i) s += "\xD1\x8F";
         return s;
       }(),
       false,
       "32 two-byte letters (64 bytes)"},
      {std::string(63, 'x'), true, "63 ASCII bytes"},
      {"a b", true, "inner space"},
      {".", true, "dot"},
      {"..", true, "dot dot"},
      {"a/b", true, "slash"},
      {"a@b", true, "at"},
      {"a:b", true, "colon"},
      {"a`b", true, "backtick"},
      {"db_1+2-3.x", true, "punctuation mix"},
      {"\xD0\xB1\xD0\xB0\xD0\xB7\xD0\xB0", true, "Cyrillic"},
      {"\xF0\x9F\x98\x80", true, "4-byte emoji"},
  };
  for (const auto &[name, well_formed, what] : cases) {
    EXPECT_EQ(memgraph::dbms::DatabaseNameIsWellFormed(name), well_formed) << what;
  }
}

namespace {

template <typename T>
concept UnqualifiedRawNewReachable = requires(T &handler, std::string_view name, memgraph::storage::Config config) {
  handler.New(std::piecewise_construct, name, config);
};

template <typename T>
concept RawEmplaceColdShellReachable = requires(T &handler, std::string_view name) { handler.EmplaceColdShell(name); };

}  // namespace

TEST_F(DBMS_Database, AdmissionEntriesStrictVsAdopt) {
  using DH = memgraph::dbms::DatabaseHandler;
  using RenameResult = std::expected<void, memgraph::dbms::RenameError>;

  static_assert(std::is_same_v<decltype(&DH::New), DH::HandlerT::NewResult (DH::*)(memgraph::storage::Config)>);
  static_assert(std::is_same_v<decltype(&DH::Rename), RenameResult (DH::*)(std::string_view, std::string_view)>);
  static_assert(std::is_same_v<decltype(&DH::AdoptLegacy), DH::HandlerT::NewResult (DH::*)(memgraph::storage::Config)>);
  static_assert(std::is_same_v<decltype(&DH::AdoptColdShell),
                               memgraph::utils::Gatekeeper<memgraph::dbms::Database> *(DH::*)(std::string_view)>);
  static_assert(
      std::is_same_v<decltype(&DH::AdoptReplicatedRename), RenameResult (DH::*)(std::string_view, std::string_view)>);
  static_assert(
      std::is_same_v<decltype(&DH::AdoptRenameRollback), RenameResult (DH::*)(std::string_view, std::string_view)>);
  static_assert(!UnqualifiedRawNewReachable<DH>,
                "DatabaseHandler must shadow Handler::New with the strict entry; a raw insert is not an admission "
                "path and must not be reachable without passing a named strict/adopt entry");

  static_assert(!RawEmplaceColdShellReachable<DH>,
                "the unchecked Handler::EmplaceColdShell must not be callable on a DatabaseHandler from outside");
  static_assert(!std::is_convertible_v<DH *, DH::HandlerT *>,
                "the base must stay private so the unchecked Handler::Rename is unreachable from outside");

  DH db_handler;

  auto rejected_new = db_handler.New(default_conf("a\nb"));
  ASSERT_FALSE(rejected_new.has_value()) << "New is the items_ mutator every CREATE reaches; it must be strict";
  EXPECT_EQ(rejected_new.error(), memgraph::dbms::NewError::INVALID_NAME);
  ASSERT_TRUE(db_handler.New(default_conf("census_src")).has_value());
  auto rejected_rename = db_handler.Rename("census_src", " lead");
  ASSERT_FALSE(rejected_rename.has_value()) << "Rename is the items_ mutator RENAME reaches; it must be strict";
  EXPECT_EQ(rejected_rename.error(), memgraph::dbms::RenameError::INVALID_NAME);
  EXPECT_TRUE(db_handler.GetConfig("census_src").has_value());

  const std::string legacy_name = " ad opt";
  const std::string cold_name = "cold sh ";
  const std::string renamed_name = "repl\name";
  const std::string rollback_name(64, 'r');
  for (const auto *invalid : {&legacy_name, &cold_name, &renamed_name, &rollback_name}) {
    ASSERT_FALSE(memgraph::dbms::DatabaseNameIsWellFormed(*invalid))
        << "precondition: adopt entries must be shown to admit a name the strict rule rejects";
  }

  ASSERT_TRUE(db_handler.AdoptLegacy(default_conf(legacy_name)).has_value());
  EXPECT_TRUE(db_handler.Has(legacy_name));
  ASSERT_NE(db_handler.AdoptColdShell(cold_name), nullptr);
  EXPECT_TRUE(db_handler.Has(cold_name));
  ASSERT_TRUE(db_handler.New(default_conf("repl_src")).has_value());
  ASSERT_TRUE(db_handler.AdoptReplicatedRename("repl_src", renamed_name).has_value());
  EXPECT_TRUE(db_handler.Has(renamed_name));
  ASSERT_TRUE(db_handler.New(default_conf("roll_src")).has_value());
  ASSERT_TRUE(db_handler.AdoptRenameRollback("roll_src", rollback_name).has_value());
  EXPECT_TRUE(db_handler.Has(rollback_name));
}

#endif
