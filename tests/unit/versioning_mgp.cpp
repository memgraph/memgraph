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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include <gmock/gmock.h>
#include "gtest/gtest.h"

#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/memory.hpp"
#include "utils/synchronized.hpp"
#include "versioning/version_store.hpp"

// mgp_execute_query needs DbmsHandler/InterpreterContextHolder singletons unavailable here, so its attach
// (CurrentDB::AttachBranchOf) is tested directly via a second session on the first one's branch.

namespace {
constexpr auto kNoHandler = nullptr;
}  // namespace

class VersioningMgpTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_mgp";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
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

  void ExpectForkVertexNotDeletedAfterMainDeletes(bool branch_changes_vertex) {
    faker.Interpret("CREATE (:V {id: 1})");
    faker.Interpret("CREATE BRANCH 'b'");
    faker.Interpret("USE BRANCH 'main'");
    faker.Interpret("MATCH (n:V) DELETE n");
    faker.Interpret("USE BRANCH 'b'");

    faker.Interpret("BEGIN");
    if (branch_changes_vertex) faker.Interpret("MATCH (n:V) SET n.x = 2");
    int64_t gid = 0;
    {
      auto stream = faker.Interpret("MATCH (n:V) RETURN id(n)");
      ASSERT_EQ(stream.GetResults().size(), 1U);
      gid = stream.GetResults()[0][0].ValueInt();
    }
    auto &dba_opt = faker.interpreter.current_db_.execution_db_accessor_;
    ASSERT_TRUE(dba_opt.has_value());
    auto va = dba_opt->FindVertex(memgraph::storage::Gid::FromInt(gid), memgraph::storage::View::NEW);
    ASSERT_TRUE(va.has_value());

    mgp_graph graph{
        &*dba_opt, memgraph::storage::View::NEW, nullptr, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
    mgp_vertex vertex{*va, &graph, mgp_vertex::allocator_type{memgraph::utils::NewDeleteResource()}};

    int deleted = -1;
    ASSERT_EQ(mgp_vertex_is_deleted(&vertex, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
    EXPECT_EQ(deleted, 0);

    faker.Interpret("ROLLBACK");
  }

  void SetUp() override {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }

  void TearDown() override {
    FLAGS_versioning_enabled = false;
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  int64_t CountOf(InterpreterFaker &f, std::string const &query) {
    auto stream = f.Interpret(query);
    EXPECT_EQ(stream.GetResults().size(), 1U);
    return stream.GetResults().at(0).at(0).ValueInt();
  }
};

// A nested session sees and writes the caller's branch; the counted attach is released when it dies.
TEST_F(VersioningMgpTest, NestedSessionAttachedToBranchSeesAndWritesBranch) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'b'");
  faker.Interpret("USE BRANCH 'b'");
  faker.Interpret("CREATE (:OnlyOnBranch)");

  auto *vs = db->version_store();
  ASSERT_NE(vs, nullptr);
  ASSERT_EQ(vs->CheckoutCount("b"), 1U);

  {
    InterpreterFaker nested{&interpreter_context, db};
    nested.interpreter.current_db_.AttachBranchOf(vs, *faker.interpreter.current_db_.branch_context());
    EXPECT_EQ(vs->CheckoutCount("b"), 2U);
    EXPECT_EQ(nested.interpreter.current_db_.CurrentVersion(), "b");

    EXPECT_EQ(CountOf(nested, "MATCH (n:OnlyOnBranch) RETURN count(n)"), 1);
    EXPECT_EQ(CountOf(nested, "MATCH (n:V) RETURN count(n)"), 1);
    nested.Interpret("CREATE (:FromNested)");

    interpreter_context.interpreters.WithLock([&](auto &s) { s.erase(&nested.interpreter); });
  }
  EXPECT_EQ(vs->CheckoutCount("b"), 1U);

  EXPECT_EQ(CountOf(faker, "MATCH (n:FromNested) RETURN count(n)"), 1);
  faker.Interpret("USE BRANCH 'main'");
  EXPECT_EQ(CountOf(faker, "MATCH (n:FromNested) RETURN count(n)"), 0);
  EXPECT_EQ(CountOf(faker, "MATCH (n:OnlyOnBranch) RETURN count(n)"), 0);
}

// DDL entry points are rejected on a branch and leave main untouched; called via the C API because the unit-test
// interpreter loads no query modules (e2e: tests/e2e/query_modules/versioning_mgp_test.py).
TEST_F(VersioningMgpTest, MgpSchemaDdlRejectedOnBranch) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'b'");
  faker.Interpret("USE BRANCH 'b'");

  faker.Interpret("BEGIN");
  auto &dba_opt = faker.interpreter.current_db_.execution_db_accessor_;
  ASSERT_TRUE(dba_opt.has_value());
  auto *dba = &*dba_opt;
  ASSERT_NE(dba->branch_context(), nullptr);

  mgp_graph graph{dba, memgraph::storage::View::NEW, nullptr, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
  mgp_memory memory{memgraph::utils::NewDeleteResource()};
  mgp_list *props = nullptr;
  ASSERT_EQ(mgp_list_make_empty(0, &memory, &props), mgp_error::MGP_ERROR_NO_ERROR);

  constexpr auto kRejected = mgp_error::MGP_ERROR_IMMUTABLE_OBJECT;
  int result = -1;
  EXPECT_EQ(mgp_create_label_index(&graph, "V", &result), kRejected);
  EXPECT_EQ(mgp_drop_label_index(&graph, "V", &result), kRejected);
  EXPECT_EQ(mgp_create_label_property_index(&graph, "V", "id", &result), kRejected);
  EXPECT_EQ(mgp_drop_label_property_index(&graph, "V", "id", &result), kRejected);
  EXPECT_EQ(mgp_create_vertex_property_index(&graph, "id", &result), kRejected);
  EXPECT_EQ(mgp_drop_vertex_property_index(&graph, "id", &result), kRejected);
  EXPECT_EQ(mgp_create_existence_constraint(&graph, "V", "id", &result), kRejected);
  EXPECT_EQ(mgp_drop_existence_constraint(&graph, "V", "id", &result), kRejected);
  EXPECT_EQ(mgp_create_unique_constraint(&graph, "V", props, &result), kRejected);
  EXPECT_EQ(mgp_drop_unique_constraint(&graph, "V", props, &result), kRejected);
  mgp_list_destroy(props);

  faker.Interpret("ROLLBACK");

  {
    auto main_acc = db->Access();
    auto const indices = main_acc->ListAllIndices();
    EXPECT_TRUE(indices.label.empty());
    EXPECT_TRUE(indices.label_properties.empty());
    EXPECT_TRUE(indices.vertex_property.empty());
    auto const constraints = main_acc->ListAllConstraints();
    EXPECT_TRUE(constraints.existence.empty());
    EXPECT_TRUE(constraints.unique.empty());
  }

  EXPECT_EQ(CountOf(faker, "MATCH (n:V) RETURN count(n)"), 1);

  // Control: on main, through an accessor with the READ_ONLY access CreateIndex requires, the same call succeeds.
  faker.Interpret("USE BRANCH 'main'");
  faker.Interpret("BEGIN");
  ASSERT_TRUE(faker.interpreter.current_db_.execution_db_accessor_.has_value());
  EXPECT_EQ(faker.interpreter.current_db_.execution_db_accessor_->branch_context(), nullptr);
  faker.Interpret("ROLLBACK");

  {
    auto main_storage_acc = db->ReadOnlyAccess();
    memgraph::query::DbAccessor main_dba{main_storage_acc.get()};
    mgp_graph main_graph{
        &main_dba, memgraph::storage::View::NEW, nullptr, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
    result = -1;
    EXPECT_EQ(mgp_create_label_index(&main_graph, "V", &result), mgp_error::MGP_ERROR_NO_ERROR);
    EXPECT_EQ(result, 1);
  }
}

TEST_F(VersioningMgpTest, IsDeletedReportsBranchDeletedForkVertex) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'b'");
  faker.Interpret("USE BRANCH 'b'");

  faker.Interpret("BEGIN");
  int64_t gid = 0;
  {
    auto stream = faker.Interpret("MATCH (n:V) RETURN id(n)");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    gid = stream.GetResults()[0][0].ValueInt();
  }
  auto &dba_opt = faker.interpreter.current_db_.execution_db_accessor_;
  ASSERT_TRUE(dba_opt.has_value());
  auto *dba = &*dba_opt;
  auto va = dba->FindVertex(memgraph::storage::Gid::FromInt(gid), memgraph::storage::View::NEW);
  ASSERT_TRUE(va.has_value());

  mgp_graph graph{dba, memgraph::storage::View::NEW, nullptr, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
  mgp_vertex vertex{*va, &graph, mgp_vertex::allocator_type{memgraph::utils::NewDeleteResource()}};

  int deleted = -1;
  ASSERT_EQ(mgp_vertex_is_deleted(&vertex, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
  EXPECT_EQ(deleted, 0);

  faker.Interpret("MATCH (n:V) DELETE n");

  deleted = -1;
  ASSERT_EQ(mgp_vertex_is_deleted(&vertex, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
  EXPECT_EQ(deleted, 1);

  faker.Interpret("ROLLBACK");
}

// Main's live deleted flag must not leak into the branch, whether or not the branch changed the fork vertex.
TEST_F(VersioningMgpTest, IsDeletedIgnoresMainDeletingForkVertexAfterFork) {
  ExpectForkVertexNotDeletedAfterMainDeletes(true);
}

TEST_F(VersioningMgpTest, IsDeletedIgnoresMainDeletingUnchangedForkVertexAfterFork) {
  ExpectForkVertexNotDeletedAfterMainDeletes(false);
}

TEST_F(VersioningMgpTest, IsDeletedReportsBranchDeletedForkEdge) {
  faker.Interpret("CREATE (:A {id: 1})-[:E]->(:B {id: 2})");
  faker.Interpret("CREATE BRANCH 'b'");
  faker.Interpret("USE BRANCH 'b'");

  faker.Interpret("BEGIN");
  int64_t gid = 0;
  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN id(n)");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    gid = stream.GetResults()[0][0].ValueInt();
  }
  auto &dba_opt = faker.interpreter.current_db_.execution_db_accessor_;
  ASSERT_TRUE(dba_opt.has_value());
  auto *dba = &*dba_opt;
  auto va = dba->FindVertex(memgraph::storage::Gid::FromInt(gid), memgraph::storage::View::NEW);
  ASSERT_TRUE(va.has_value());
  auto out = va->OutEdges(memgraph::storage::View::NEW);
  ASSERT_TRUE(out.has_value());
  ASSERT_EQ(out->edges.size(), 1U);

  mgp_graph graph{dba, memgraph::storage::View::NEW, nullptr, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
  mgp_edge edge{out->edges[0], &graph, mgp_edge::allocator_type{memgraph::utils::NewDeleteResource()}};

  int deleted = -1;
  ASSERT_EQ(mgp_edge_is_deleted(&edge, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
  EXPECT_EQ(deleted, 0);

  faker.Interpret("MATCH ()-[r:E]->() DELETE r");

  deleted = -1;
  ASSERT_EQ(mgp_edge_is_deleted(&edge, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
  EXPECT_EQ(deleted, 1);

  faker.Interpret("ROLLBACK");
}

// On a branch the counts come from main@fork, not the diff engine (which holds only touched/created objects).
TEST_F(VersioningMgpTest, ApproximateCountsOnBranchReportMainAtFork) {
  faker.Interpret("CREATE (a:V {id: 1}), (b:V {id: 2}), (c:V {id: 3})");
  faker.Interpret("MATCH (a:V {id: 1}), (b:V {id: 2}), (c:V {id: 3}) CREATE (a)-[:E]->(b), (b)-[:E]->(c)");
  faker.Interpret("CREATE BRANCH 'b'");
  faker.Interpret("USE BRANCH 'b'");

  faker.Interpret("BEGIN");
  faker.Interpret("CREATE (:OnlyOnBranch)");
  auto &dba_opt = faker.interpreter.current_db_.execution_db_accessor_;
  ASSERT_TRUE(dba_opt.has_value());
  auto *dba = &*dba_opt;
  ASSERT_NE(dba->branch_context(), nullptr);

  mgp_graph graph{dba, memgraph::storage::View::NEW, nullptr, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
  size_t vertex_count = 0;
  size_t edge_count = 0;
  ASSERT_EQ(mgp_graph_approximate_vertex_count(&graph, &vertex_count), mgp_error::MGP_ERROR_NO_ERROR);
  ASSERT_EQ(mgp_graph_approximate_edge_count(&graph, &edge_count), mgp_error::MGP_ERROR_NO_ERROR);
  EXPECT_EQ(vertex_count, 3U);
  EXPECT_EQ(edge_count, 2U);

  faker.Interpret("ROLLBACK");
}
