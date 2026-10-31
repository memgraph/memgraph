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

// Graph Versioning: schema and storage-global operations refused on a branch, and the operations refused on main while
// the database has branches. Branches are created and attached through the C++ API.

#include <algorithm>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/gate.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;

namespace {

constexpr auto kOnBranchError =
    "Schema and storage-global operations are not allowed on a branch. Run USE BRANCH main first.";

using QueryAndMessage = std::pair<const char *, const char *>;

constexpr QueryAndMessage kRefusedWhileBranchesExist[] = {
    {"STORAGE MODE IN_MEMORY_ANALYTICAL", "Cannot switch storage mode while branches exist"},
    {"STORAGE MODE ON_DISK_TRANSACTIONAL", "Cannot switch storage mode while branches exist"},
    {"DROP GRAPH", "Cannot drop the graph while branches exist"},
    {"RECOVER SNAPSHOT \"/nonexistent/snapshot\"", "Cannot recover a snapshot while branches exist"},
    {"CREATE ENUM Mood VALUES { Happy, Sad }", "Cannot create an enum while branches exist"},
    {"ALTER ENUM Status ADD VALUE Unknown", "Cannot alter an enum while branches exist"},
    {"ALTER ENUM Status UPDATE VALUE Bad TO Poor", "Cannot alter an enum while branches exist"},
};

void ExpectRefusedWith(InterpreterFaker &session, const char *query, const char *message) {
  try {
    session.Interpret(query);
    ADD_FAILURE() << "must be refused: " << query;
  } catch (const memgraph::query::QueryRuntimeException &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr(message)) << query;
    EXPECT_THAT(e.what(), ::testing::HasSubstr("Drop all branches first.")) << query;
  }
}

memgraph::storage::Config StandaloneConfig(const std::filesystem::path &directory, bool versioning) {
  memgraph::storage::Config config{};
  config.durability.storage_directory = directory;
  config.durability.recover_on_startup = true;
  config.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.disk.main_storage_directory = directory / "disk";
  config.experimental_versioning = versioning;
  return config;
}

class VersioningRefusalsTest : public VersioningGatedTest {
 protected:
  void UseMain() { faker.interpreter.current_db_.LeaveBranch(); }
};

}  // namespace

TEST_F(VersioningRefusalsTest, StatementsRejectedOnBranch) {
  faker.Interpret("CREATE INDEX ON :Person(age)");
  faker.Interpret("CREATE INDEX ON :Person");
  faker.Interpret("CREATE EDGE INDEX ON :KNOWS");
  faker.Interpret("CREATE (:Person {age:10})");
  faker.Interpret("CREATE (:L {p:1})-[:R {p:1}]->(:M)");

  auto index_rows = [&]() {
    auto s = faker.Interpret("SHOW INDEX INFO");
    std::vector<std::string> rows;
    for (const auto &row : s.GetResults()) {
      std::string r;
      for (size_t i = 0; i + 1 < row.size(); ++i) {  // skip the trailing count column
        r += row[i].IsString() ? std::string(row[i].ValueString()) : std::string("-");
        r += '|';
      }
      rows.push_back(std::move(r));
    }
    std::ranges::sort(rows);
    return rows;
  };
  const auto main_rows = index_rows();
  ASSERT_GE(main_rows.size(), 3U);

  Fork();

  static constexpr const char *rejected[] = {
      "CREATE INDEX ON :L(p)",
      "CREATE EDGE INDEX ON :R(p)",
      "CREATE POINT INDEX ON :L(p)",
      "CREATE VECTOR INDEX vi ON :L(p) WITH CONFIG {\"dimension\": 2, \"capacity\": 10}",
      "CREATE TEXT INDEX ti ON :L",
      "DROP INDEX ON :Person(age)",
      "DROP INDEX ON :Person",
      "DROP EDGE INDEX ON :KNOWS",
      "DROP ALL INDEXES",
      "CREATE CONSTRAINT ON (n:L) ASSERT EXISTS (n.p)",
      "DROP ALL CONSTRAINTS",
      "CREATE ENUM Status VALUES { Good, Bad }",
      "ANALYZE GRAPH",
      "DROP GRAPH",
      "STORAGE MODE IN_MEMORY_ANALYTICAL",
      "RECOVER SNAPSHOT \"/nonexistent/snapshot\"",
      "EDGE IMPORT MODE ACTIVE",
      "ENABLE TTL",
      "CREATE TRIGGER my_trigger AFTER COMMIT EXECUTE RETURN 1",
      // Must pass semantic validation (TOPICS/TRANSFORM are required) to reach the guard.
      "CREATE KAFKA STREAM my_stream TOPICS my_topic TRANSFORM my_module.my_transform",
      "SET DESCRIPTION ON LABEL :Person \"d\"",
      "DELETE DESCRIPTION ON LABEL :Person",
      "SHOW DESCRIPTIONS",
  };
  for (const auto *query : rejected) {
    SCOPED_TRACE(query);
    try {
      faker.Interpret(query);
      ADD_FAILURE() << "must be rejected on a branch";
    } catch (const memgraph::query::QueryRuntimeException &e) {
      EXPECT_STREQ(e.what(), kOnBranchError);
    }
  }

  try {
    faker.Interpret("SHOW SCHEMA INFO");
    ADD_FAILURE() << "SHOW SCHEMA INFO must be refused on a branch";
  } catch (const memgraph::query::QueryRuntimeException &e) {
    EXPECT_THAT(e.what(), ::testing::StartsWith("SHOW SCHEMA INFO on a branch is not in v1"));
  }

  EXPECT_EQ(IntRows(faker, "MATCH (n:L) RETURN n.p"), (Rows{{1}}));
  faker.Interpret("CREATE (:L {p:2})");
  faker.Interpret("CREATE SNAPSHOT");

  EXPECT_EQ(index_rows(), main_rows);
  EXPECT_TRUE(
      PlanHasOperator(ExplainText(faker, "MATCH (n:Person) WHERE n.age > 0 RETURN n"), "ScanAllByLabelProperties"));

  UseMain();
  EXPECT_EQ(index_rows(), main_rows);
  faker.Interpret("CREATE INDEX ON :L(p)");
  EXPECT_EQ(index_rows().size(), main_rows.size() + 1);
  faker.Interpret("CREATE TRIGGER my_trigger AFTER COMMIT EXECUTE RETURN 1");
  EXPECT_EQ(faker.Interpret("SHOW TRIGGERS").GetResults().size(), 1U);
  EXPECT_NO_THROW(faker.Interpret("SET DESCRIPTION ON LABEL :Person \"d\""));
}

// The schema procedures of the C API refuse on a branch and still work on main.
TEST_F(VersioningRefusalsTest, SchemaProceduresRefusedOnBranch) {
  mgp_memory memory{memgraph::utils::NewDeleteResource()};
  mgp_list *empty_list = nullptr;
  ASSERT_EQ(mgp_list_make_empty(0, &memory, &empty_list), mgp_error::MGP_ERROR_NO_ERROR);

  auto const outcomes = [&](mgp_graph &graph) {
    int result = 0;
    return std::vector<mgp_error>{
        mgp_create_label_index(&graph, "L", &result),
        mgp_drop_label_index(&graph, "L", &result),
        mgp_create_label_property_index(&graph, "L", "p", &result),
        mgp_drop_label_property_index(&graph, "L", "p", &result),
        mgp_create_vertex_property_index(&graph, "p", &result),
        mgp_drop_vertex_property_index(&graph, "p", &result),
        mgp_create_existence_constraint(&graph, "L", "p", &result),
        mgp_drop_existence_constraint(&graph, "L", "p", &result),
        mgp_create_unique_constraint(&graph, "L", empty_list, &result),
        mgp_drop_unique_constraint(&graph, "L", empty_list, &result),
    };
  };

  Fork();
  {
    auto &ctx = *faker.interpreter.current_db_.branch_context();
    auto diff = ctx.diff_engine().UniqueAccess();
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    memgraph::query::DbAccessor dba{ctx.current_diff_txn(), &ctx};
    mgp_graph graph{&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode()};
    for (auto const error : outcomes(graph)) EXPECT_EQ(error, mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
    ctx.EndTransaction();
  }
  {
    auto acc = db->storage()->UniqueAccess();
    memgraph::query::DbAccessor dba{acc.get()};
    mgp_graph graph{&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode()};
    for (auto const error : outcomes(graph)) EXPECT_NE(error, mgp_error::MGP_ERROR_IMMUTABLE_OBJECT);
  }
  mgp_list_destroy(empty_list);
}

// Refused while any branch exists, also from a session on main; allowed again once the last branch is dropped.
TEST_F(VersioningRefusalsTest, BranchExistenceRejectsStorageModeDropGraphAndEnumChanges) {
  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad }");
  faker.Interpret("CREATE (:A {x:1})");
  ASSERT_TRUE(db->version_store()->CreateBranch("b").has_value());
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  for (const auto &[query, message] : kRefusedWhileBranchesExist) ExpectRefusedWith(faker, query, message);
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.x"), (Rows{{1}}));
  EXPECT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

  ASSERT_EQ(db->version_store()->DropBranch("b"), memgraph::versioning::DropResult::kDropped);
  faker.Interpret("ALTER ENUM Status ADD VALUE Unknown");
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  faker.Interpret("DROP GRAPH");
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}}));
}

// With versioning off the ledger's branches are kept hidden, and the refusals stay as with live branches.
TEST_F(VersioningRefusalsTest, HiddenBranchesKeepDatabaseGlobalOperationsRefused) {
  const auto directory = data_directory / "hidden_db";
  {
    memgraph::utils::Gatekeeper<memgraph::dbms::Database> gk{StandaloneConfig(directory, true)};
    auto acc = gk.access();
    ASSERT_TRUE(acc);
    ASSERT_TRUE((*acc)->version_store()->CreateBranch("b").has_value());
  }

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> gk{StandaloneConfig(directory, false)};
  auto acc = gk.access();
  ASSERT_TRUE(acc);
  ASSERT_FALSE((*acc)->version_store()->Empty());
  InterpreterFaker hidden{&interpreter_context, *acc};

  for (const auto &[query, message] : kRefusedWhileBranchesExist) ExpectRefusedWith(hidden, query, message);
  for (const auto *query : {"SHOW BRANCHES", "CREATE BRANCH 'c'", "DROP BRANCH 'b'"}) {
    EXPECT_EQ(ThrownMessage([&] { hidden.Interpret(query); }),
              memgraph::versioning::GateErrorMessage(memgraph::versioning::GateError::kDisabled))
        << query;
  }
}

// The early check of STORAGE MODE runs at Prepare; the switch checks again under its exclusive hold, so a branch
// created in between still refuses it.
TEST_F(VersioningRefusalsTest, StorageModeRefusedForBranchCreatedAfterPrepare) {
  auto [stream, qid] = faker.Prepare("STORAGE MODE IN_MEMORY_ANALYTICAL");
  ASSERT_TRUE(db->version_store()->CreateBranch("late").has_value());

  try {
    faker.Pull(&stream, {}, qid);
    ADD_FAILURE() << "the storage-mode switch must be refused once a branch exists";
  } catch (const memgraph::query::QueryRuntimeException &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr("Cannot switch storage mode while branches exist"));
  }
  EXPECT_EQ(db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_TRUE(db->version_store()->Exists("late"));
}

// RECOVER SNAPSHOT checks for branches at Prepare, like the storage-mode switch: a branch created in between must still
// refuse it and leave the data alone.
TEST_F(VersioningRefusalsTest, RecoverSnapshotRefusedForBranchCreatedAfterPrepare) {
  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE SNAPSHOT");
  std::filesystem::path snapshot;
  for (const auto &entry : std::filesystem::directory_iterator(data_directory / "snapshots")) snapshot = entry.path();
  ASSERT_FALSE(snapshot.empty());
  faker.Interpret("CREATE (:A {x:2})");

  auto [stream, qid] = faker.Prepare(fmt::format("RECOVER SNAPSHOT \"{}\" FORCE", snapshot.string()));
  ASSERT_TRUE(db->version_store()->CreateBranch("late").has_value());

  try {
    faker.Pull(&stream, {}, qid);
    ADD_FAILURE() << "the snapshot recovery must be refused once a branch exists";
  } catch (const std::exception &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr("Cannot recover a snapshot while branches exist"));
  }
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN count(n)"), (Rows{{2}})) << "the data must not have been replaced";
  EXPECT_TRUE(db->version_store()->Exists("late"));
}

// Same for the switch to on-disk, which checks under the Database's exclusive access. The session is the database's
// only holder, as that switch requires.
TEST_F(VersioningRefusalsTest, OnDiskSwitchRefusedForBranchCreatedAfterPrepare) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> gk{StandaloneConfig(data_directory / "on_disk_db", true)};
  InterpreterFaker session{&interpreter_context, *gk.access()};
  auto *const version_store = session.interpreter.current_db_.db_acc_->get()->version_store();

  auto [stream, qid] = session.Prepare("STORAGE MODE ON_DISK_TRANSACTIONAL");
  ASSERT_TRUE(version_store->CreateBranch("late").has_value());

  try {
    session.Pull(&stream, {}, qid);
    ADD_FAILURE() << "the on-disk switch must be refused once a branch exists";
  } catch (const memgraph::query::QueryRuntimeException &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr("Cannot switch storage mode while branches exist"));
  }
  EXPECT_EQ(session.interpreter.current_db_.db_acc_->get()->GetStorageMode(),
            memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_TRUE(version_store->Exists("late"));
}
