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

// SHOW INDEX/CONSTRAINT INFO, SHOW NODE_LABELS/EDGE_TYPES, SHOW STORAGE INFO and SHOW SCHEMA INFO on a checked-out
// versioning branch: they must describe main plus the branch's own changes, never the diff engine's internals.

#include <algorithm>
#include <filesystem>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include "gtest/gtest.h"

#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/synchronized.hpp"
#include "versioning/version_store.hpp"

namespace {
constexpr auto kNoHandler = nullptr;
}  // namespace

class VersioningInfoTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_info";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    config.salient.items.enable_schema_metadata = true;
    config.salient.items.enable_schema_info = true;
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&] {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    return *db_acc_opt;
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

  void SetUp() override {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }

  void TearDown() override {
    FLAGS_versioning_enabled = false;
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  void BuildMain() {
    faker.Interpret("CREATE INDEX ON :A");
    faker.Interpret("CREATE INDEX ON :A(p)");
    faker.Interpret("CREATE CONSTRAINT ON (n:E) ASSERT EXISTS (n.p)");
    faker.Interpret("CREATE CONSTRAINT ON (n:T) ASSERT n.q IS TYPED INTEGER");
    faker.Interpret("CREATE CONSTRAINT ON (n:U) ASSERT n.k IS UNIQUE");
    faker.Interpret("CREATE (:A {p: 1}), (:A {p: 2})-[:R]->(:B)");
    faker.Interpret("CREATE (:U {k: 1})");
  }

  // Touches every marker kind: native vertex/edge/labels, added label, removed label, deleted fork vertex.
  void ChangeBranch() {
    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");
    faker.Interpret("CREATE (:BranchOnly)-[:BR]->(:BranchOnly2)");
    faker.Interpret("MATCH (a:A {p: 1}) SET a:Added REMOVE a:A");
    faker.Interpret("MATCH (a:A {p: 2}) DETACH DELETE a");
  }

  using Rows = std::vector<std::vector<std::string>>;

  // Renders each result row as strings (lists joined, null as "<null>") so branch/main outputs compare directly.
  Rows Show(std::string const &query) {
    auto stream = faker.Interpret(query);
    Rows rows;
    for (auto const &row : stream.GetResults()) {
      std::vector<std::string> out;
      for (auto const &value : row) {
        if (value.type() == memgraph::communication::bolt::Value::Type::Null) {
          out.emplace_back("<null>");
        } else if (value.IsString()) {
          out.emplace_back(value.ValueString());
        } else if (value.IsInt()) {
          out.emplace_back(std::to_string(value.ValueInt()));
        } else if (value.IsList()) {
          std::string joined;
          for (auto const &item : value.ValueList()) joined += item.IsString() ? item.ValueString() : "?";
          out.emplace_back(std::move(joined));
        } else {
          out.emplace_back("?");
        }
      }
      rows.push_back(std::move(out));
    }
    return rows;
  }

  std::set<std::string> Column(std::string const &query, size_t col = 0) {
    std::set<std::string> out;
    for (auto const &row : Show(query)) out.insert(row.at(col));
    return out;
  }

  // Index rows minus the trailing approximate count.
  std::set<std::vector<std::string>> IndexDefinitions() {
    std::set<std::vector<std::string>> out;
    for (auto row : Show("SHOW INDEX INFO")) {
      row.pop_back();
      out.insert(std::move(row));
    }
    return out;
  }

  std::string StorageInfoValue(std::string const &key) {
    for (auto const &row : Show("SHOW STORAGE INFO ON CURRENT DATABASE")) {
      if (row[0] == key) return row[1];
    }
    return "<missing>";
  }
};

TEST_F(VersioningInfoTest, IndexInfoOnBranchListsMainDefinitionsOnly) {
  BuildMain();
  auto const main_definitions = IndexDefinitions();
  ASSERT_EQ(main_definitions.size(), 2U);

  ChangeBranch();
  // No synthetic overlay indexes (global property / extra label indexes), and no branch-only labels.
  EXPECT_EQ(IndexDefinitions(), main_definitions);

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IndexDefinitions(), main_definitions);
}

TEST_F(VersioningInfoTest, ConstraintInfoMarksUniqueAsNotEnforcedOnBranch) {
  BuildMain();
  auto const main_rows = Show("SHOW CONSTRAINT INFO");
  ASSERT_EQ(main_rows.size(), 3U);
  EXPECT_THAT(Column("SHOW CONSTRAINT INFO"), ::testing::UnorderedElementsAre("exists", "data_type", "unique"));

  ChangeBranch();
  auto branch_rows = Show("SHOW CONSTRAINT INFO");
  ASSERT_EQ(branch_rows.size(), 3U);
  EXPECT_THAT(Column("SHOW CONSTRAINT INFO"),
              ::testing::UnorderedElementsAre("exists", "data_type", "unique (not enforced on branch)"));
  for (auto &row : branch_rows) {
    if (row[0] == "unique (not enforced on branch)") row[0] = "unique";
  }
  auto sorted_main = main_rows;
  std::ranges::sort(sorted_main);
  std::ranges::sort(branch_rows);
  EXPECT_EQ(branch_rows, sorted_main);

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_THAT(Column("SHOW CONSTRAINT INFO"), ::testing::UnorderedElementsAre("exists", "data_type", "unique"));
}

TEST_F(VersioningInfoTest, NodeLabelsAndEdgeTypesAreMainPlusBranchWithoutMarkers) {
  BuildMain();
  ChangeBranch();

  // Each name once (main/branch overlap deduped), no `__mg_*` markers.
  EXPECT_THAT(Column("SHOW NODE_LABELS INFO"),
              ::testing::UnorderedElementsAre("A", "B", "U", "BranchOnly", "BranchOnly2", "Added"));
  EXPECT_EQ(Show("SHOW NODE_LABELS INFO").size(), 6U);
  EXPECT_THAT(Column("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R", "BR"));
  EXPECT_EQ(Show("SHOW EDGE_TYPES INFO").size(), 2U);

  faker.Interpret("CHECKOUT BRANCH 'main'");
  auto const main_labels = Column("SHOW NODE_LABELS INFO");
  EXPECT_FALSE(main_labels.contains("BranchOnly"));
  EXPECT_FALSE(main_labels.contains("Added"));
  EXPECT_THAT(main_labels, ::testing::UnorderedElementsAre("A", "B", "U"));
  EXPECT_THAT(Column("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R"));
}

TEST_F(VersioningInfoTest, StorageInfoHidesCountsOnBranch) {
  BuildMain();
  EXPECT_EQ(StorageInfoValue("vertex_count"), "4");
  EXPECT_EQ(StorageInfoValue("edge_count"), "1");
  EXPECT_EQ(StorageInfoValue("branch"), "<missing>");

  ChangeBranch();
  EXPECT_EQ(StorageInfoValue("vertex_count"), "<null>");
  EXPECT_EQ(StorageInfoValue("edge_count"), "<null>");
  EXPECT_EQ(StorageInfoValue("average_degree"), "<null>");
  EXPECT_EQ(StorageInfoValue("branch"), "b");
  EXPECT_THAT(
      StorageInfoValue("branch_note"),
      ::testing::HasSubstr("are not available on a versioning branch; CHECKOUT BRANCH 'main' for main's counts"));
  EXPECT_NE(StorageInfoValue("graph_memory_tracked"), "<missing>");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(StorageInfoValue("vertex_count"), "4");
  EXPECT_EQ(StorageInfoValue("branch"), "<missing>");

  // Instance-level SHOW STORAGE INFO carries no per-database counts and is unaffected.
  faker.Interpret("CHECKOUT BRANCH 'b'");
  auto const instance_rows = Show("SHOW STORAGE INFO");
  EXPECT_FALSE(instance_rows.empty());
  for (auto const &row : instance_rows) {
    EXPECT_NE(row[0], "vertex_count");
    EXPECT_NE(row[0], "branch");
    EXPECT_NE(row[0], "branch_note");
  }
}

TEST_F(VersioningInfoTest, SchemaInfoRejectedOnBranch) {
  BuildMain();
  EXPECT_NO_THROW(Show("SHOW SCHEMA INFO"));

  ChangeBranch();
  try {
    Show("SHOW SCHEMA INFO");
    FAIL() << "SHOW SCHEMA INFO must be rejected on a branch";
  } catch (memgraph::query::QueryRuntimeException const &e) {
    EXPECT_THAT(std::string{e.what()},
                ::testing::HasSubstr("SHOW SCHEMA INFO is not supported on a versioning branch"));
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_NO_THROW(Show("SHOW SCHEMA INFO"));
}
