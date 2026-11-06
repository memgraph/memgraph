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

// Graph Versioning: what SHOW INDEX/STORAGE/SCHEMA INFO, the label and edge-type listings and the procedure API's
// schema listings report on a branch.

#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/procedure/mg_procedure_impl.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ThrownMessage;

namespace {

using Rows = std::vector<std::vector<std::string>>;

class VersioningInfoTest : public VersioningGatedTest {
 protected:
  void SetUp() override {
    VersioningGatedTest::SetUp();
    // Read on every write, so turning it on before any data exists is the same as the startup flag.
    db->storage()->config_.salient.items.enable_schema_metadata = true;
  }

  void BuildMain() {
    faker.Interpret("CREATE INDEX ON :A");
    faker.Interpret("CREATE INDEX ON :A(p)");
    faker.Interpret("CREATE (:A {p: 1}), (:A {p: 2})-[:R]->(:B)");
  }

  // A native vertex and edge, a label added to and one removed from a fork vertex, a deleted fork vertex.
  void ChangeBranch() {
    faker.Interpret("CREATE BRANCH b");
    faker.Interpret("USE BRANCH b");
    faker.Interpret("CREATE (:BranchOnly)-[:BR]->(:BranchOnly2)");
    faker.Interpret("MATCH (a:A {p: 1}) SET a:Added REMOVE a:A");
    faker.Interpret("MATCH (a:A {p: 2}) DETACH DELETE a");
  }

  // Each row as strings: null as "<null>", a list as its joined strings.
  Rows Show(const std::string &query) {
    Rows rows;
    for (const auto &row : faker.Interpret(query).GetResults()) {
      auto &out = rows.emplace_back();
      for (const auto &value : row) {
        if (value.type() == memgraph::communication::bolt::Value::Type::Null) {
          out.emplace_back("<null>");
        } else if (value.IsString()) {
          out.push_back(value.ValueString());
        } else if (value.IsInt()) {
          out.push_back(std::to_string(value.ValueInt()));
        } else if (value.IsDouble()) {
          out.push_back(std::to_string(value.ValueDouble()));
        } else if (value.IsList()) {
          std::string joined;
          for (const auto &item : value.ValueList()) joined += item.ValueString();
          out.push_back(std::move(joined));
        } else {
          out.emplace_back("?");
        }
      }
    }
    return rows;
  }

  std::multiset<std::string> FirstColumn(const std::string &query) {
    std::multiset<std::string> out;
    for (const auto &row : Show(query)) out.insert(row.at(0));
    return out;
  }

  // Index rows without the trailing approximate count.
  std::set<std::vector<std::string>> IndexDefinitions() {
    std::set<std::vector<std::string>> out;
    for (auto row : Show("SHOW INDEX INFO")) {
      row.pop_back();
      out.insert(std::move(row));
    }
    return out;
  }

  std::string StorageInfo(const std::string &key) {
    for (const auto &row : Show("SHOW STORAGE INFO ON CURRENT DATABASE")) {
      if (row.at(0) == key) return row.at(1);
    }
    return "<missing>";
  }

  // The procedure API's approximate vertex and edge counts inside the session's open transaction.
  std::pair<std::string, std::string> ProcedureCounts() {
    auto &dba = *faker.interpreter.current_db_.execution_db_accessor_;
    mgp_graph graph{&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode()};
    size_t vertices = 0;
    size_t edges = 0;
    EXPECT_EQ(mgp_graph_approximate_vertex_count(&graph, &vertices), mgp_error::MGP_ERROR_NO_ERROR);
    EXPECT_EQ(mgp_graph_approximate_edge_count(&graph, &edges), mgp_error::MGP_ERROR_NO_ERROR);
    return {std::to_string(vertices), std::to_string(edges)};
  }

  // Sizes of the procedure API's label-index and existence-constraint lists inside the session's open transaction.
  std::pair<size_t, size_t> ProcedureListingSizes() {
    auto &dba = *faker.interpreter.current_db_.execution_db_accessor_;
    mgp_graph graph{&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode()};
    mgp_memory memory{memgraph::utils::NewDeleteResource()};
    auto const size_of = [&](auto list_fn) {
      mgp_list *list = nullptr;
      EXPECT_EQ(list_fn(&graph, &memory, &list), mgp_error::MGP_ERROR_NO_ERROR);
      size_t size = 0;
      EXPECT_EQ(mgp_list_size(list, &size), mgp_error::MGP_ERROR_NO_ERROR);
      mgp_list_destroy(list);
      return size;
    };
    return {size_of(mgp_list_all_label_indices), size_of(mgp_list_all_existence_constraints)};
  }
};

constexpr std::string_view kSchemaInfoOnBranch =
    "SHOW SCHEMA INFO on a branch is not in v1: schema info is tracked per storage engine, so main's does not include "
    "the branch's changes. Run USE BRANCH main first.";

}  // namespace

// Main's definitions, never the indexes the branch keeps for its own scans.
TEST_F(VersioningInfoTest, IndexInfoOnABranchIsMains) {
  BuildMain();
  const auto main_definitions = IndexDefinitions();
  ASSERT_EQ(main_definitions.size(), 2U);

  ChangeBranch();
  EXPECT_EQ(IndexDefinitions(), main_definitions);
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(IndexDefinitions(), main_definitions);
}

// Main's names plus the branch's, each once: labels of branch-created vertices and labels set on fork vertices.
TEST_F(VersioningInfoTest, LabelAndEdgeTypeListingsAreMainsPlusTheBranchs) {
  BuildMain();
  ChangeBranch();
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"),
              ::testing::UnorderedElementsAre("A", "B", "BranchOnly", "BranchOnly2", "Added"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R", "BR"));

  faker.Interpret("USE BRANCH main");
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"), ::testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R"));
}

// Removed labels and properties and a deleted fork vertex are stubs with reserved marker ids on the diff engine, whose
// schema metadata lists them; no listing may show them.
TEST_F(VersioningInfoTest, ListingsOnABranchLeakNoMarkerNames) {
  BuildMain();
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("MATCH (a:A {p: 1}) REMOVE a:A, a.p");
  faker.Interpret("MATCH (a:A {p: 2}) DETACH DELETE a");
  faker.Interpret("MATCH (:B)<-[r:R]-() DELETE r");

  for (const auto *query : {"SHOW NODE_LABELS INFO", "SHOW EDGE_TYPES INFO", "SHOW INDEX INFO"}) {
    for (const auto &row : Show(query)) {
      for (const auto &cell : row) {
        EXPECT_EQ(cell.find("__mg_"), std::string::npos) << query << ": " << cell;
      }
    }
  }
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"), ::testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R"));
  for (const auto &row : Show("SHOW STORAGE INFO ON CURRENT DATABASE")) {
    for (const auto &cell : row) EXPECT_EQ(cell.find("__mg_"), std::string::npos) << cell;
  }
}

// Main at the fork plus the branch's committed changes: native creates, deleted fork vertices, fork edges deleted or
// hidden by a deleted endpoint. Main's later writes and a rolled-back transaction do not count.
TEST_F(VersioningInfoTest, StorageInfoCountsOnABranchAreMainAtTheForkPlusTheBranchs) {
  BuildMain();
  faker.Interpret("CREATE (:C)-[:S]->(:C)");
  EXPECT_EQ(StorageInfo("vertex_count"), "5");
  EXPECT_EQ(StorageInfo("edge_count"), "2");
  EXPECT_EQ(StorageInfo("branch"), "<missing>");

  // 5 + 2 native - 1 deleted vertices; 2 + 1 native - 1 hidden (the deleted vertex's) - 1 deleted edges.
  ChangeBranch();
  faker.Interpret("MATCH ()-[s:S]->() DELETE s");
  const auto expect_branch_counts = [&] {
    EXPECT_EQ(StorageInfo("vertex_count"), "6");
    EXPECT_EQ(StorageInfo("edge_count"), "1");
    EXPECT_EQ(StorageInfo("average_degree"), std::to_string(2.0 / 6));
  };
  expect_branch_counts();
  EXPECT_EQ(StorageInfo("branch"), "b");
  EXPECT_EQ(StorageInfo("storage_mode"), "IN_MEMORY_TRANSACTIONAL");
  faker.Interpret("BEGIN");
  EXPECT_EQ(ProcedureCounts(), (std::pair<std::string, std::string>{"6", "1"}));
  faker.Interpret("ROLLBACK");

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (c:C) DETACH DELETE c");
  faker.Interpret("MATCH (n:BranchOnly) DETACH DELETE n");
  faker.Interpret("ROLLBACK");
  expect_branch_counts();

  faker.Interpret("USE BRANCH main");
  faker.Interpret("CREATE (:M), (:M)-[:R]->(:M)");
  faker.Interpret("MATCH (c:C) DETACH DELETE c");
  faker.Interpret("USE BRANCH b");
  expect_branch_counts();

  // The instance view has no per-database rows.
  for (const auto &row : Show("SHOW STORAGE INFO")) {
    EXPECT_NE(row.at(0), "vertex_count");
    EXPECT_NE(row.at(0), "branch");
  }

  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(StorageInfo("branch"), "<missing>");
}

TEST_F(VersioningInfoTest, SchemaInfoIsRefusedOnABranch) {
  ChangeBranch();
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("SHOW SCHEMA INFO"); }), kSchemaInfoOnBranch);
  faker.Interpret("USE BRANCH main");
  EXPECT_NE(ThrownMessage([&] { faker.Interpret("SHOW SCHEMA INFO"); }), kSchemaInfoOnBranch);
}

// Query modules list main's indexes and constraints on a branch, as SHOW INDEX/CONSTRAINT INFO do.
TEST_F(VersioningInfoTest, ProcedureSchemaListingsOnABranchAreMains) {
  faker.Interpret("CREATE INDEX ON :A");
  faker.Interpret("CREATE CONSTRAINT ON (n:A) ASSERT EXISTS (n.p)");
  faker.Interpret("CREATE (:A {p: 1})");
  faker.Interpret("BEGIN");
  const auto on_main = ProcedureListingSizes();
  faker.Interpret("ROLLBACK");
  ASSERT_EQ(on_main, (std::pair<size_t, size_t>{1, 1}));

  ChangeBranch();
  faker.Interpret("BEGIN");
  EXPECT_EQ(ProcedureListingSizes(), on_main);
  faker.Interpret("ROLLBACK");
}

// The branch's counts are main's at the fork plus the committed branch changes: inside an open branch transaction
// main's later commits never show, the transaction's own deletes count only once it commits, a rolled-back or
// conflicting delete never counts, and a delete that won is counted exactly once. (Like main's, they are not a
// per-transaction snapshot: another branch session's commit shows as soon as it lands.)
TEST_F(VersioningInfoTest, BranchCountsIgnoreMainAfterTheForkAndCountEachCommittedDeleteOnce) {
  faker.Interpret("CREATE (:V {id: 1}), (:V {id: 2}), (:V {id: 3})-[:E]->(:V {id: 4})");
  faker.Interpret("CREATE BRANCH b");
  InterpreterFaker main_session{&interpreter_context, db};
  auto sessions = SessionsOnBranch(2, "b");
  auto &first = *sessions[0];
  auto &second = *sessions[1];
  faker.Interpret("USE BRANCH b");
  const auto counts = [](size_t vertices, size_t edges) {
    return std::pair<std::string, std::string>{std::to_string(vertices), std::to_string(edges)};
  };

  faker.Interpret("BEGIN");
  EXPECT_EQ(ProcedureCounts(), counts(4, 1));
  main_session.Interpret("CREATE (:Later)-[:E]->(:Later), (:Later)");
  main_session.Interpret("MATCH (n:V {id: 4}) DETACH DELETE n");
  EXPECT_EQ(ProcedureCounts(), counts(4, 1)) << "main's commits after the fork are not the branch's";
  faker.Interpret("MATCH (n:V {id: 1}) DELETE n");
  EXPECT_EQ(ProcedureCounts(), counts(4, 1)) << "the transaction's own delete counts when it commits";

  first.Interpret("BEGIN");
  first.Interpret("MATCH (n:V {id: 3}) DETACH DELETE n");
  second.Interpret("BEGIN");
  EXPECT_NE(ThrownMessage([&] { second.Interpret("MATCH (n:V {id: 3}) DETACH DELETE n"); }), "<no exception>");
  second.Interpret("ROLLBACK");
  EXPECT_EQ(ProcedureCounts(), counts(4, 1)) << "an uncommitted or conflicting delete does not count";
  first.Interpret("COMMIT");
  EXPECT_EQ(ProcedureCounts(), counts(3, 0)) << "the winning delete counts once, with the edge it hid";

  faker.Interpret("COMMIT");
  EXPECT_EQ(StorageInfo("vertex_count"), "2");
  EXPECT_EQ(StorageInfo("edge_count"), "0");
  EXPECT_EQ(second.Interpret("MATCH (n:V) RETURN count(n)").GetResults().at(0).at(0).ValueInt(), 2)
      << "the counts are the visible vertices";
}
