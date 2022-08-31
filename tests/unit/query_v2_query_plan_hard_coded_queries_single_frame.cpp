// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <iterator>
#include <memory>
#include <vector>

#include "common/types.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "io/simulator/simulator.hpp"
#include "query/v2/context.hpp"
#include "query/v2/exceptions.hpp"
#include "query_v2_query_plan_common.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"

#include <random>

using namespace memgraph::query::v2;
using namespace memgraph::query::v2::plan;
using test_common::ToIntList;
using test_common::ToIntMap;
using testing::UnorderedElementsAre;

namespace memgraph::query::v2::tests {

class QueryPlanHardCodedQueriesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(
        db_v3.CreateSchema(schema_label, {storage::v3::SchemaProperty{schema_property, common::SchemaType::INT}}));
  }

  storage::v3::Storage db_v3;
  const storage::v3::LabelId schema_label{db_v3.NameToLabel("label")};
  const storage::v3::PropertyId schema_property{db_v3.NameToProperty("property")};
};

class QueryPlanHardCodedQueriesTestFixture : public ::testing::TestWithParam<std::pair<size_t, size_t>> {
 protected:
  void SetUp() override {
    ASSERT_TRUE(
        db_v3.CreateSchema(schema_label, {storage::v3::SchemaProperty{schema_property, common::SchemaType::INT}}));
  }

  bool GetRandomBool() {
    const auto lower_bound = 0.0;
    const auto upper_bound = 100.0;
    std::uniform_real_distribution<double> unif(lower_bound, upper_bound);
    std::random_device r;
    std::default_random_engine re(r());
    return unif(re) < 50.0;
  }

  struct Result {
    storage::v3::Gid n_gid_;
    storage::v3::Gid p_gid_;
    storage::v3::Gid i_gid_;

    bool operator<(const Result &other) const {
      if (n_gid_ != other.n_gid_) {
        return n_gid_ < other.n_gid_;
      }
      if (p_gid_ != other.p_gid_) {
        return p_gid_ < other.p_gid_;
      }
      return i_gid_ < other.i_gid_;
    }
  };

  storage::v3::Storage db_v3;
  storage::v3::LabelId schema_label;
  storage::v3::PropertyId schema_property;
};

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWhileBatching) {
  /*
    QUERY:
      MATCH (n)
      RETURN *;

    QUERY PLAN:
      Produce {n}
      ScanAll (n)
      Once
  */
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};
  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_n = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      auto [it, inserted] = gid_of_expected_vertices.insert(vertex_n.Gid());
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n)
  auto scan_all_1 = MakeScanAll(storage, symbol_table, "n");

  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(scan_all_1.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), gid_of_expected_vertices.size());
  for (auto result : results) {
    ASSERT_TRUE(result[0].IsVertex());
    auto gid = result[0].ValueVertex().Gid();
    auto it_found = gid_of_expected_vertices.find(gid);
    ASSERT_TRUE(it_found != gid_of_expected_vertices.end());
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWithLabelFilteringWhileBatching) {
  /*
    INDEXES:
      CREATE INDEX ON :Node;

    QUERY:
      MATCH (n:Node)
      RETURN *;

    QUERY PLAN:
      Produce {n}
      ScanAll (n)
      Once
  */

  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();
  const auto number_of_vertices_with_label = number_of_vertices / 3;  // To have some filtering needed and measureable.
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};

  auto label_node = db_v3.NameToLabel("Node");
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});

      if (property_index <= number_of_vertices_with_label) {
        ASSERT_TRUE(vertex_node.AddLabel(label_node).HasValue());
        auto [it, inserted] = gid_of_expected_vertices.insert(vertex_node.Gid());
        ASSERT_TRUE(inserted);
      }
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  // INDEX CREATION
  db_v3.CreateIndex(label_node);

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n:Node)
  auto scan_all_1 = MakeScanAllByLabel(storage, symbol_table, "n", label_node);

  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(scan_all_1.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), gid_of_expected_vertices.size());
  for (auto result : results) {
    ASSERT_TRUE(result[0].IsVertex());
    auto gid = result[0].ValueVertex().Gid();
    auto it_found = gid_of_expected_vertices.find(gid);
    ASSERT_TRUE(it_found != gid_of_expected_vertices.end());
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWithLabelPropertyValueFilteringWhileBatching) {
  /*
    INDEXES:
      CREATE INDEX ON :Node(someId);

    QUERY:
      MATCH (n:Node {someId:'expectedValue'})

      RETURN *;

    QUERY PLAN:
      Produce {n}
      ScanAll (n)
      Once
  */

  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();

  auto label_node = db_v3.NameToLabel("Node");
  auto property_node = db_v3.NameToProperty("someId");
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});

      auto has_label = GetRandomBool();
      auto has_property = GetRandomBool();
      auto has_expected_property = GetRandomBool();
      if (has_label) {
        ASSERT_TRUE(vertex_node.AddLabel(label_node).HasValue());
      }

      if (has_property) {
        if (has_expected_property) {
          ASSERT_TRUE(vertex_node.SetProperty(property_node, storage::v3::PropertyValue("expectedValue")).HasValue());
        } else {
          ASSERT_TRUE(
              vertex_node.SetProperty(property_node, storage::v3::PropertyValue("spanishInquisition")).HasValue());
        }
      }

      if (has_label && has_property && has_expected_property) {
        auto [it, inserted] = gid_of_expected_vertices.insert(vertex_node.Gid());
        ASSERT_TRUE(inserted);
      }
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  // INDEX CREATION
  db_v3.CreateIndex(label_node, property_node);

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n:Node {someId:'ExpectedValue'})
  auto scan_all_1 = MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label_node, property_node, "someId",
                                                    LITERAL("expectedValue"));
  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(scan_all_1.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), gid_of_expected_vertices.size());
  for (auto result : results) {
    ASSERT_TRUE(result[0].IsVertex());
    auto gid = result[0].ValueVertex().Gid();
    auto it_found = gid_of_expected_vertices.find(gid);
    ASSERT_TRUE(it_found != gid_of_expected_vertices.end());
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWithExpandWhileBatching) {
  /*
    QUERY:
      MATCH ()-[]->(n)
      RETURN *;

    QUERY PLAN:
      Produce {n}
      Expand (n)<-[anon2]-(anon1)
      ScanAll (n)
      Once
  */

  /*  In case of number_of_vertices = 3
      Center          Other
      O[0]--|
            |-------->O[1]
            |-------->O[2]
            |-------->O[3]
  */
  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();

  storage::v3::EdgeTypeId edge_type{db_v3.NameToEdgeType("IS_EDGE")};
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    auto vertex_center = *dba.InsertVertexAndValidate(
        schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});

    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto other_vertex = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});

      ASSERT_TRUE(dba.InsertEdge(&vertex_center, &other_vertex, edge_type).HasValue());
      auto [it, inserted] = gid_of_expected_vertices.insert(other_vertex.Gid());
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto symbol_n = symbol_table.CreateSymbol("n", true);
  auto symbol_anon1 = symbol_table.CreateSymbol("anon1", true);
  auto symbol_anon2 = symbol_table.CreateSymbol("anon2", true);

  // ScanAll (anon1)
  auto scan_all = std::make_shared<ScanAll>(nullptr, symbol_anon1);

  // Expand (n)<-[anon2]-(anon1)
  auto expand_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_type};
  auto expand = std::make_shared<Expand>(scan_all, symbol_anon1, symbol_n, symbol_anon2, EdgeAtom::Direction::OUT,
                                         expand_edge_types, false /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output_n = NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(expand, output_n);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), gid_of_expected_vertices.size());
  for (auto result : results) {
    ASSERT_TRUE(result[0].IsVertex());
    auto gid = result[0].ValueVertex().Gid();
    auto it_found = gid_of_expected_vertices.find(gid);
    ASSERT_TRUE(it_found != gid_of_expected_vertices.end());
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWithExpandWhileBatching2) {  // #NoCommit better naming
  /*
    QUERY:
      MATCH ()-[]->(n)
      RETURN *;

    QUERY PLAN:
      Produce {n}
      Expand (n)<-[anon2]-(anon1)
      ScanAll (n)
      Once
  */

  /*  In case of number_of_vertices = 3

      O[0]---------->O[1]
      O[2]---------->O[3]
      O[4]---------->O[5]
  */
  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();

  storage::v3::EdgeTypeId edge_type{db_v3.NameToEdgeType("IS_EDGE")};
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;

    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_center = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      auto other_vertex = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});

      ASSERT_TRUE(dba.InsertEdge(&vertex_center, &other_vertex, edge_type).HasValue());
      auto [it, inserted] = gid_of_expected_vertices.insert(other_vertex.Gid());
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto symbol_n = symbol_table.CreateSymbol("n", true);
  auto symbol_anon1 = symbol_table.CreateSymbol("anon1", true);
  auto symbol_anon2 = symbol_table.CreateSymbol("anon2", true);

  // ScanAll (anon1)
  auto scan_all = std::make_shared<ScanAll>(nullptr, symbol_anon1);

  // Expand (n)<-[anon2]-(anon1)
  auto expand_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_type};
  auto expand = std::make_shared<Expand>(scan_all, symbol_anon1, symbol_n, symbol_anon2, EdgeAtom::Direction::OUT,
                                         expand_edge_types, false /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output_n = NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));
  auto produce = MakeProduce(expand, output_n);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), gid_of_expected_vertices.size());
  for (auto result : results) {
    ASSERT_TRUE(result[0].IsVertex());
    auto gid = result[0].ValueVertex().Gid();
    auto it_found = gid_of_expected_vertices.find(gid);
    ASSERT_TRUE(it_found != gid_of_expected_vertices.end());
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWithExpandWhileBatching3) {  // #NoCommit better naming
  /*
    CREATE INDEX ON :N;
    CREATE INDEX ON :P;

  QUERY:
    MATCH (n:N)
    MATCH (p:P)-[:IS_EDGE]->(n:N)
    RETURN n, p;

  QUERY PLAN:
    Produce {n, p}
    Expand (p)-[anon2:IS_EDGE]->(n)
    ScanAllByLabel (n :N)
    ScanAllByLabel (p :P)
    Once
  */
  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();

  storage::v3::EdgeTypeId edge_type{db_v3.NameToEdgeType("IS_EDGE")};
  auto label_n = db_v3.NameToLabel("N");
  auto label_p = db_v3.NameToLabel("P");

  auto gid_of_expected_vertices = std::set<std::pair<storage::v3::Gid, storage::v3::Gid>>{};
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;

    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_n = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_n.AddLabel(label_n).HasValue());

      auto vertex_p = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_p.AddLabel(label_p).HasValue());

      ASSERT_TRUE(dba.InsertEdge(&vertex_p, &vertex_n, edge_type).HasValue());
      auto [it, inserted] = gid_of_expected_vertices.insert(std::make_pair(vertex_n.Gid(), vertex_p.Gid()));
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }
  db_v3.CreateIndex(label_n);
  db_v3.CreateIndex(label_p);

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto symbol_n = symbol_table.CreateSymbol("n", true);
  auto symbol_p = symbol_table.CreateSymbol("p", true);
  auto symbol_anon2 = symbol_table.CreateSymbol("anon2", true);

  // ScanAllByLabel (p :P)
  auto scan_all_1 = std::make_shared<ScanAllByLabel>(nullptr, symbol_p, label_p, memgraph::storage::v3::View::OLD);

  // ScanAllByLabel (n :N)
  auto scan_all_2 = std::make_shared<ScanAllByLabel>(scan_all_1, symbol_n, label_n, memgraph::storage::v3::View::OLD);

  // Expand (p)-[e]->(n)
  auto expand_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_type};

  auto expand = std::make_shared<Expand>(scan_all_2, symbol_n, symbol_p, symbol_anon2, EdgeAtom::Direction::IN,
                                         expand_edge_types, true /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output_n = NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));
  auto output_p = NEXPR("p", IDENT("p")->MapTo(symbol_p))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
  auto produce = MakeProduce(expand, output_n, output_p);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  auto transformed_results = std::vector<std::pair<storage::v3::Gid, storage::v3::Gid>>{};
  std::transform(results.begin(), results.end(), std::back_inserter(transformed_results),
                 [](const std::vector<TypedValue> &result) {
                   auto gid_n = result[0].ValueVertex().Gid();
                   auto gid_p = result[1].ValueVertex().Gid();
                   return std::make_pair(gid_n, gid_p);
                 });

  ASSERT_EQ(transformed_results.size(), gid_of_expected_vertices.size());
  for (auto result : transformed_results) {
    ASSERT_TRUE(gid_of_expected_vertices.end() != gid_of_expected_vertices.find(result));
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, ScallAllScanAllWhileBatching) {
  /*
    QUERY:
      MATCH (n)
      MATCH (p)
      RETURN n,p;

    QUERY PLAN:
      Produce {n, p}
      ScanAll (n)
      ScanAll (p)
      Once
  */
  auto pairs_gids_of_expected_vertices = std::set<std::pair<storage::v3::Gid, storage::v3::Gid>>{};
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};

  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      auto [it, inserted] = gid_of_expected_vertices.insert(vertex_node.Gid());
      ASSERT_TRUE(inserted);
    }

    // Generate the expected result from the double ScanAll
    for (auto &outer_gid : gid_of_expected_vertices) {
      for (auto &inner_gid : gid_of_expected_vertices) {
        auto [it, inserted] = pairs_gids_of_expected_vertices.insert(std::make_pair(outer_gid, inner_gid));
        ASSERT_TRUE(inserted);
      }
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (p)
  auto scan_all_1 = MakeScanAll(storage, symbol_table, "p");
  // MATCH (n)
  auto scan_all_2 = MakeScanAll(storage, symbol_table, "n", scan_all_1.op_);

  auto output_p =
      NEXPR("p", IDENT("p")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
  auto output_n =
      NEXPR("n", IDENT("n")->MapTo(scan_all_2.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));

  auto produce = MakeProduce(scan_all_2.op_, output_n, output_p);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  auto transformed_results = std::vector<std::pair<storage::v3::Gid, storage::v3::Gid>>{};
  std::transform(results.begin(), results.end(), std::back_inserter(transformed_results),
                 [](const std::vector<TypedValue> &result) {
                   auto gid_n = result[0].ValueVertex().Gid();
                   auto gid_p = result[1].ValueVertex().Gid();
                   return std::make_pair(gid_n, gid_p);
                 });

  ASSERT_EQ(transformed_results.size(), pairs_gids_of_expected_vertices.size());
  for (auto result : transformed_results) {
    ASSERT_TRUE(pairs_gids_of_expected_vertices.end() != pairs_gids_of_expected_vertices.find(result));
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, ScallAllScanAllScanAllWhileBatching) {
  /*
    QUERY:
      MATCH (n)
      MATCH (p)
      MATCH (q)
      RETURN n,p,q;

    QUERY PLAN:
      Produce {n, p, q}
      ScanAll (n)
      ScanAll (p)
      ScanAll (q)
      Once
  */
  auto tuples_gids_of_expected_vertices = std::set<std::tuple<storage::v3::Gid, storage::v3::Gid, storage::v3::Gid>>{};
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};

  const auto [number_of_vertices, unused_frames_per_batch] = GetParam();
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      auto [it, inserted] = gid_of_expected_vertices.insert(vertex_node.Gid());
      ASSERT_TRUE(inserted);
    }

    // Generate the expected result from the double ScanAll
    for (auto &outer_gid : gid_of_expected_vertices) {
      for (auto &middle_gid : gid_of_expected_vertices) {
        for (auto &inner_gid : gid_of_expected_vertices) {
          auto [it, inserted] =
              tuples_gids_of_expected_vertices.insert(std::make_tuple(outer_gid, middle_gid, inner_gid));
          ASSERT_TRUE(inserted);
        }
      }
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (q)
  auto scan_all_1 = MakeScanAll(storage, symbol_table, "q");
  // MATCH (p)
  auto scan_all_2 = MakeScanAll(storage, symbol_table, "p", scan_all_1.op_);
  // MATCH (n)
  auto scan_all_3 = MakeScanAll(storage, symbol_table, "n", scan_all_2.op_);

  auto output_q =
      NEXPR("q", IDENT("q")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_q", true));
  auto output_p =
      NEXPR("p", IDENT("p")->MapTo(scan_all_2.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
  auto output_n =
      NEXPR("n", IDENT("n")->MapTo(scan_all_3.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));

  auto produce = MakeProduce(scan_all_3.op_, output_n, output_p, output_q);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  auto transformed_results = std::vector<std::tuple<storage::v3::Gid, storage::v3::Gid, storage::v3::Gid>>{};
  std::transform(results.begin(), results.end(), std::back_inserter(transformed_results),
                 [](const std::vector<TypedValue> &result) {
                   auto gid_n = result[0].ValueVertex().Gid();
                   auto gid_p = result[1].ValueVertex().Gid();
                   auto gid_q = result[1].ValueVertex().Gid();
                   return std::make_tuple(gid_n, gid_p, gid_q);
                 });

  ASSERT_EQ(transformed_results.size(), tuples_gids_of_expected_vertices.size());
  for (auto result : transformed_results) {
    ASSERT_TRUE(tuples_gids_of_expected_vertices.end() != tuples_gids_of_expected_vertices.find(result));
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, HardCodedQuery01) {
  /*
  INDEXES:
    CREATE INDEX ON :N(pId);
    CREATE INDEX ON :P;
    CREATE INDEX ON :I(email);

  QUERY:
    MATCH (i:I {email: 'rrr@test.com'})<-[:IS_FOR_I]-(p:P)
    MATCH (p:P)-[:IS_FOR_N]->(n:N)
    MATCH (n:N {pId: 'XXXXXXXXXXXXZZZZZZZZZ'})
    RETURN *;

  QUERY PLAN:
    Produce {i, n, p}
    Expand (p)-[anon1:IS_FOR_I]->(i)
    ScanAllByLabelPropertyValue (i :I {email})
    Expand (n)<-[anon3:IS_FOR_N]-(p)
    ScanAllByLabel (p :P)
    ScanAllByLabelPropertyValue (n :Node {pId})
    Once
  */
  const auto [number_of_clusters, unused_frames_per_batch] = GetParam();
  auto label_n = db_v3.NameToLabel("N");
  auto property_n_pId = db_v3.NameToProperty("pId");

  auto label_p = db_v3.NameToLabel("P");

  auto label_i = db_v3.NameToLabel("I");
  auto property_i_email = db_v3.NameToProperty("email");

  storage::v3::EdgeTypeId edge_is_for_n{db_v3.NameToEdgeType("IS_FOR_N")};
  storage::v3::EdgeTypeId edge_is_for_i{db_v3.NameToEdgeType("IS_FOR_I")};
  auto n_gids = std::set<storage::v3::Gid>{};
  auto p_gids = std::set<storage::v3::Gid>{};
  auto i_gids = std::set<storage::v3::Gid>{};

  auto expected_final_results = std::set<Result>{};

  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_clusters; ++idx) {
      storage::v3::Gid n_gid;
      storage::v3::Gid p_gid;
      storage::v3::Gid i_gid;

      auto vertex_n = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_n.AddLabel(label_n).HasValue());
      ASSERT_TRUE(vertex_n.SetProperty(property_n_pId, storage::v3::PropertyValue("XXXXXXXXXXXXZZZZZZZZZ")).HasValue());
      n_gid = vertex_n.Gid();
      n_gids.insert(n_gid);

      auto vertex_p = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_p.AddLabel(label_p).HasValue());
      p_gid = vertex_p.Gid();
      p_gids.insert(p_gid);

      auto edge_p_to_n = dba.InsertEdge(&vertex_p, &vertex_n, edge_is_for_n);
      ASSERT_TRUE(edge_p_to_n.HasValue());

      auto vertex_i = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_i.AddLabel(label_i).HasValue());
      ASSERT_TRUE(vertex_i.SetProperty(property_i_email, storage::v3::PropertyValue("rrr@test.com")).HasValue());
      i_gid = vertex_i.Gid();
      i_gids.insert(i_gid);

      auto edge_p_to_i = dba.InsertEdge(&vertex_p, &vertex_i, edge_is_for_i);
      ASSERT_TRUE(edge_p_to_i.HasValue());

      auto result = Result{.n_gid_ = n_gid, .p_gid_ = p_gid, .i_gid_ = i_gid};
      auto [it, inserted] = expected_final_results.insert(result);
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  // INDEX CREATION
  db_v3.CreateIndex(label_n, property_n_pId);
  db_v3.CreateIndex(label_p);
  db_v3.CreateIndex(label_i, property_i_email);

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // Create our symbols
  auto symbol_n = symbol_table.CreateSymbol("n", true);
  auto symbol_p = symbol_table.CreateSymbol("p", true);
  auto symbol_i = symbol_table.CreateSymbol("i", true);
  auto symbol_anon1 = symbol_table.CreateSymbol("anon1", true);
  auto symbol_anon3 = symbol_table.CreateSymbol("anon3", true);

  // MATCH (n:N {pId: 'XXXXXXXXXXXXZZZZZZZZZ'})
  auto scan_all_1 =
      std::make_shared<ScanAllByLabelPropertyValue>(nullptr /*input*/, symbol_n, label_n, property_n_pId, "pId",
                                                    LITERAL("XXXXXXXXXXXXZZZZZZZZZ"), memgraph::storage::v3::View::OLD);

  // MATCH (p:P)
  auto scan_all_2 = std::make_shared<ScanAllByLabel>(scan_all_1, symbol_p, label_p, memgraph::storage::v3::View::OLD);

  // (n:N)<-[anon3:IS_FOR_N]-(p:P)
  auto expand_1_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_is_for_n};
  auto expand_1 =
      std::make_shared<Expand>(scan_all_2, symbol_p, symbol_n, symbol_anon3, EdgeAtom::Direction::OUT,
                               expand_1_edge_types, true /*existing_node*/, memgraph::storage::v3::View::OLD);

  // MATCH (i:Iy {email: 'rrr@test.com'})
  auto scan_all_3 =
      std::make_shared<ScanAllByLabelPropertyValue>(expand_1, symbol_i, label_i, property_i_email, "email",
                                                    LITERAL("rrr@test.com"), memgraph::storage::v3::View::OLD);

  // (p:Pn)-[anon1:IS_FOR_I]->(i:I {email: 'rrr@test.com'})
  auto expand_2_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_is_for_i};
  auto expand_2 =
      std::make_shared<Expand>(scan_all_3, symbol_i, symbol_p, symbol_anon1, EdgeAtom::Direction::IN,
                               expand_2_edge_types, true /*existing_node*/, memgraph::storage::v3::View::OLD);
  {
    /*
    Checking result from:
      MATCH (i:I {email: 'rrr@test.com'})<-[:IS_FOR_I]-(p:P)
      MATCH (p:P)-[:IS_FOR_N]->(n:N)
      MATCH (n:N {pId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n =
        NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));
    auto output_p =
        NEXPR("p", IDENT("p")->MapTo(symbol_p))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
    auto output_i =
        NEXPR("i", IDENT("i")->MapTo(symbol_i))->MapTo(symbol_table.CreateSymbol("named_expression_i", true));

    auto produce = MakeProduce(expand_2, output_n, output_p, output_i);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);

    auto transformed_results = std::vector<Result>{};
    std::transform(results.begin(), results.end(), std::back_inserter(transformed_results),
                   [](const std::vector<TypedValue> &result) -> Result {
                     auto n_gid = result[0].ValueVertex().Gid();
                     auto p_gid = result[1].ValueVertex().Gid();
                     auto i_gid = result[2].ValueVertex().Gid();
                     return Result{.n_gid_ = n_gid, .p_gid_ = p_gid, .i_gid_ = i_gid};
                   });

    ASSERT_EQ(transformed_results.size(), expected_final_results.size());

    for (auto result : transformed_results) {
      ASSERT_TRUE(expected_final_results.end() != expected_final_results.find(result));
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    QueryPlanHardCodedQueriesTest, QueryPlanHardCodedQueriesTestFixture,
    ::testing::Values(
        std::make_pair(1, 1),     /* 1 vertex, 1 frame per batch: simple case. */
        std::make_pair(2, 1),     /* 2 vertices, 1 frame per batch: simple case. */
        std::make_pair(2, 2),     /* 2 vertices, 1 frame per batch: simple case. */
        std::make_pair(3, 2),     /* 3 vertices, 2 frame per batch: simple case. */
        std::make_pair(3, 3),     /* 3 vertices, 3 frame per batch: simple case. */
        std::make_pair(100, 1),   /* 100 vertices, 1 frame per batch: to check previous
                                    behavior (ie: pre-batching). */
        std::make_pair(1, 2),     /* 1 vertex, 2 batches. */
        std::make_pair(4, 2),     /* 4 vertices, 2 frames per batch. */
        std::make_pair(5, 2),     /* 5 vertices, 2 frames per batch. */
        std::make_pair(100, 100), /* 100 vertices, 100 frames per batch: to check batching works with 1 iteration. */
        std::make_pair(100, 50),  /* 100 vertices, 50 frames per batch: to check batching works with 2 iterations. */
        std::make_pair(37, 100),  /* 37 vertices, 100 frames per batch: to check resizing of frames works when having 1
                                    iteration only. */
        std::make_pair(42, 10)    /* 42 vertices, 10 frames per batch: to check resizing of frames works when having
                                       several iterations. There will be only 2 vertices left on the last batch.*/
        ));
}  // namespace memgraph::query::v2::tests
