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

// Graph Versioning v2: the mgp (query-module) surface on a checked-out branch.
//
// mgp_execute_query itself needs a DbmsHandler + InterpreterContextHolder singleton (not available in unit tests;
// see the e2e skeleton in tests/e2e/versioning/), so the attach it performs -- CurrentDB::AttachBranchOf -- is tested
// directly here: a second session attached to the first one's branch, which is exactly what the nested Interpreter
// of mgp_execute_query is.

namespace {
constexpr auto kNoHandler = nullptr;
}  // namespace

class VersioningMgpTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_mgp";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    // Chunk-0 gate requires PERIODIC_SNAPSHOT_WITH_WAL (D9) -- see WalGateDisabled below for the
    // default-config (WAL-off) rejection path instead.
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

// (a) A nested session attached to the caller's branch reads the branch's data, writes land on the branch (not main),
// and the counted attach is released when the nested session dies.
TEST_F(VersioningMgpTest, NestedSessionAttachedToBranchSeesAndWritesBranch) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
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

  // The outer session sees the nested write on the branch ...
  EXPECT_EQ(CountOf(faker, "MATCH (n:FromNested) RETURN count(n)"), 1);
  // ... and main never does.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(CountOf(faker, "MATCH (n:FromNested) RETURN count(n)"), 0);
  EXPECT_EQ(CountOf(faker, "MATCH (n:OnlyOnBranch) RETURN count(n)"), 0);
}

// (b) The query-module DDL entry points (schema.assert's building blocks) are rejected on a branch with
// MGP_ERROR_IMMUTABLE_OBJECT and leave main's schema untouched. The calls are made directly through the C API: the
// unit-test interpreter loads no query modules, so `CALL schema.assert` is not reachable here (see the e2e skeleton).
TEST_F(VersioningMgpTest, MgpSchemaDdlRejectedOnBranch) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

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

  // Nothing leaked onto main.
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

  // The session is still usable.
  EXPECT_EQ(CountOf(faker, "MATCH (n:V) RETURN count(n)"), 1);

  // On main the branch rail does not apply (the rail's condition is a non-null branch context). The DDL call itself is
  // not issued: it needs UNIQUE/READ_ONLY storage access, which this write transaction does not hold.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("BEGIN");
  ASSERT_TRUE(faker.interpreter.current_db_.execution_db_accessor_.has_value());
  EXPECT_EQ(faker.interpreter.current_db_.execution_db_accessor_->branch_context(), nullptr);
  faker.Interpret("ROLLBACK");
}

// (c) mgp_vertex_is_deleted reports a branch-deleted fork vertex as deleted (the fork vertex itself is never
// physically deleted: its diff-engine stub carries a delete marker).
TEST_F(VersioningMgpTest, IsDeletedReportsBranchDeletedForkVertex) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

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
