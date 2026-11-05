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

// DUMP DATABASE on a checked-out branch: the data section is the branch's reconstructed view, the schema section is
// main's (not the diff engine's synthetic overlay indexes), and vector-indexed values must not touch the diff engine.

#include <algorithm>
#include <filesystem>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include "gtest/gtest.h"

#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/config.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

namespace {
constexpr auto kNoHandler = nullptr;
}  // namespace

class VersioningDumpTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_dump";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.experimental_versioning = true;
    config.durability.recover_on_startup = true;
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&] {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
              "Wrong storage mode!");
    return db_acc;
  }()};

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };

  InterpreterFaker faker{&interpreter_context, db};

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void TearDown() override {
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  std::vector<std::string> Dump() {
    auto stream = faker.Interpret("DUMP DATABASE");
    std::vector<std::string> lines;
    for (const auto &row : stream.GetResults()) lines.push_back(row[0].ValueString());
    return lines;
  }

  static std::string Join(const std::vector<std::string> &lines) {
    std::string all;
    for (const auto &l : lines) all += l + "\n";
    return all;
  }

  static bool HasLineWithAll(const std::vector<std::string> &lines, std::initializer_list<std::string_view> parts) {
    return std::ranges::any_of(lines, [&](const auto &l) {
      return std::ranges::all_of(parts, [&](auto p) { return l.find(p) != std::string::npos; });
    });
  }

  static bool HasLine(const std::vector<std::string> &lines, const std::string &line) {
    return std::ranges::find(lines, line) != lines.end();
  }
};

TEST_F(VersioningDumpTest, DumpOnBranchReflectsBranchDataAndMainSchema) {
  faker.Interpret("CREATE INDEX ON :Person(name)");
  faker.Interpret("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)");
  faker.Interpret("CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE");
  faker.Interpret(
      "CREATE (:Person {name: 'a', id: 1, age: 10}), (:Person {name: 'b', id: 2}), (:Person {name: 'c', id: 3})");
  faker.Interpret("MATCH (a {name: 'a'}), (b {name: 'b'}) CREATE (a)-[:KNOWS {w: 1}]->(b)");

  faker.Interpret("CREATE BRANCH 'feature'");
  faker.Interpret("USE BRANCH 'feature'");
  faker.Interpret("MATCH (a {name: 'a'}) SET a.age = 99, a:Extra");
  faker.Interpret("MATCH (b {name: 'b'}) REMOVE b.id, b:Person SET b:Other");
  faker.Interpret("MATCH (c {name: 'c'}) DETACH DELETE c");
  faker.Interpret("MATCH (a:Extra), (b {name: 'b'}) CREATE (a)-[:NEW {x: 5}]->(b)");
  faker.Interpret("CREATE (:BranchOnly {name: 'native'})");

  std::vector<std::string> lines;
  ASSERT_NO_THROW(lines = Dump());
  const auto all = Join(lines);

  EXPECT_THAT(all, ::testing::HasSubstr("`age`: 99"));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("`age`: 10")));
  EXPECT_THAT(all, ::testing::HasSubstr(":`Extra`"));
  EXPECT_TRUE(HasLineWithAll(lines, {"CREATE (:__mg_vertex__", ":`Person`", ":`Extra`", "`age`: 99"}));
  EXPECT_THAT(all, ::testing::HasSubstr(":`Other`"));
  EXPECT_THAT(all, ::testing::HasSubstr(":`BranchOnly`"));
  EXPECT_THAT(all, ::testing::HasSubstr(":`NEW`"));
  EXPECT_THAT(all, ::testing::HasSubstr("`x`: 5"));
  EXPECT_THAT(all, ::testing::HasSubstr(":`KNOWS`"));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("`id`: 2")));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("`id`: 3")));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("`name`: \"c\"")));

  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("__mg_removed")));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("__mg_deleted")));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("__mg_rm:")));

  // Main has no label-only or global property index, so such a statement would be a diff-engine overlay artifact.
  EXPECT_TRUE(HasLine(lines, "CREATE INDEX ON :`Person`(`name`);"));
  EXPECT_TRUE(HasLine(lines, "CREATE CONSTRAINT ON (u:`Person`) ASSERT EXISTS (u.`name`);"));
  EXPECT_THAT(all, ::testing::HasSubstr("ASSERT u.`id` IS UNIQUE;"));
  EXPECT_FALSE(HasLine(lines, "CREATE INDEX ON :`Person`;"));
  EXPECT_THAT(all, ::testing::Not(::testing::HasSubstr("CREATE GLOBAL INDEX")));
}

TEST_F(VersioningDumpTest, DumpOnBranchWithVectorIndexedVertex) {
  faker.Interpret("CREATE VECTOR INDEX vi ON :Emb(vec) WITH CONFIG {\"dimension\": 2, \"capacity\": 10}");
  faker.Interpret("CREATE (:Emb {vec: [1.0, 2.0], name: 'fork'})");

  faker.Interpret("CREATE BRANCH 'feature'");
  faker.Interpret("USE BRANCH 'feature'");
  // Touching the fork vertex turns it into a stub.
  faker.Interpret("MATCH (e:Emb) SET e.tag = 1");
  faker.Interpret("CREATE (:Emb {vec: [3.0, 4.0], name: 'native'})");

  std::vector<std::string> lines;
  ASSERT_NO_THROW(lines = Dump());
  const auto all = Join(lines);
  EXPECT_TRUE(
      HasLineWithAll(lines, {"CREATE (:__mg_vertex__", ":`Emb`", "`name`: \"fork\"", "`vec`: [1, 2]", "`tag`: 1"}));
  EXPECT_TRUE(HasLineWithAll(lines, {"CREATE (:__mg_vertex__", ":`Emb`", "`name`: \"native\"", "`vec`: [3, 4]"}));
  EXPECT_THAT(all, ::testing::HasSubstr("`tag`: 1"));
  EXPECT_THAT(all, ::testing::HasSubstr("CREATE VECTOR INDEX `vi` ON :`Emb`(`vec`)"));
}
