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

  void ChangeBranch() {
    faker.Interpret("CREATE BRANCH b");
    faker.Interpret("USE BRANCH b");
    faker.Interpret("CREATE (:BranchOnly)-[:BR]->(:BranchOnly2)");
    faker.Interpret("MATCH (a:A {p: 1}) SET a:Added REMOVE a:A");
    faker.Interpret("MATCH (a:A {p: 2}) DETACH DELETE a");
  }

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

  size_t ProcedureVectorIndexCount() {
    auto &dba = *faker.interpreter.current_db_.execution_db_accessor_;
    mgp_graph graph{&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode()};
    mgp_memory memory{memgraph::utils::NewDeleteResource()};
    mgp_map *info = nullptr;
    EXPECT_EQ(mgp_graph_show_index_info(&graph, &memory, &info), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_value *results = nullptr;
    EXPECT_EQ(mgp_map_at(info, "search_results", &results), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_list *list = nullptr;
    EXPECT_EQ(mgp_value_get_list(results, &list), mgp_error::MGP_ERROR_NO_ERROR);
    size_t size = 0;
    EXPECT_EQ(mgp_list_size(list, &size), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_map_destroy(info);
    return size;
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

  // An index created on main after the fork is listed on the branch too: the listing is main's current one.
  faker.Interpret("CREATE INDEX ON :B");
  const auto main_after = IndexDefinitions();
  ASSERT_EQ(main_after.size(), 3U);
  faker.Interpret("USE BRANCH b");
  EXPECT_EQ(IndexDefinitions(), main_after);
}

TEST_F(VersioningInfoTest, LabelAndEdgeTypeListingsAreMainsPlusTheBranchs) {
  BuildMain();
  ChangeBranch();
  // Names main lists too, so each must appear once.
  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"),
              ::testing::UnorderedElementsAre("A", "B", "BranchOnly", "BranchOnly2", "Added"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R", "BR"));

  faker.Interpret("USE BRANCH main");
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"), ::testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R"));

  // Names first written on main after the fork are listed on the branch too: main's current names.
  faker.Interpret("CREATE (:Later)-[:LATER]->(:Later)");
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"), ::testing::UnorderedElementsAre("A", "B", "Later"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R", "LATER"));
  faker.Interpret("USE BRANCH b");
  EXPECT_THAT(FirstColumn("SHOW NODE_LABELS INFO"),
              ::testing::UnorderedElementsAre("A", "B", "Later", "BranchOnly", "BranchOnly2", "Added"));
  EXPECT_THAT(FirstColumn("SHOW EDGE_TYPES INFO"), ::testing::UnorderedElementsAre("R", "LATER", "BR"));
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

TEST_F(VersioningInfoTest, StorageInfoOnABranchHasNoCountsAndNamesTheBranch) {
  BuildMain();
  EXPECT_EQ(StorageInfo("vertex_count"), "3");
  EXPECT_EQ(StorageInfo("edge_count"), "1");
  EXPECT_EQ(StorageInfo("branch"), "<missing>");

  ChangeBranch();
  EXPECT_EQ(StorageInfo("vertex_count"), "<null>");
  EXPECT_EQ(StorageInfo("edge_count"), "<null>");
  EXPECT_EQ(StorageInfo("average_degree"), "<null>");
  EXPECT_EQ(StorageInfo("branch"), "b");
  EXPECT_EQ(StorageInfo("storage_mode"), "IN_MEMORY_TRANSACTIONAL");

  // The instance view has no per-database rows.
  for (const auto &row : Show("SHOW STORAGE INFO")) {
    EXPECT_NE(row.at(0), "vertex_count");
    EXPECT_NE(row.at(0), "branch");
  }

  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(StorageInfo("vertex_count"), "3");
  EXPECT_EQ(StorageInfo("branch"), "<missing>");
}

TEST_F(VersioningInfoTest, SchemaInfoIsRefusedOnABranch) {
  ChangeBranch();
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("SHOW SCHEMA INFO"); }), kSchemaInfoOnBranch);
  faker.Interpret("USE BRANCH main");
  EXPECT_NE(ThrownMessage([&] { faker.Interpret("SHOW SCHEMA INFO"); }), kSchemaInfoOnBranch);
}

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

TEST_F(VersioningInfoTest, ProcedureVectorIndexListingOnABranchIsMains) {
  faker.Interpret(R"(CREATE VECTOR INDEX vi ON :Doc(emb) WITH CONFIG {"dimension": 2, "capacity": 10})");
  faker.Interpret(R"(CREATE VECTOR EDGE INDEX ve ON :Rel(emb) WITH CONFIG {"dimension": 2, "capacity": 10})");
  faker.Interpret("BEGIN");
  const auto on_main = ProcedureVectorIndexCount();
  faker.Interpret("ROLLBACK");
  ASSERT_EQ(on_main, 2U);

  ChangeBranch();
  ASSERT_EQ(Show("SHOW INDEX INFO").size(), 2U);
  faker.Interpret("BEGIN");
  EXPECT_EQ(ProcedureVectorIndexCount(), on_main);
  faker.Interpret("ROLLBACK");
}
