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
#include "query/v2/plan/operator_distributed.hpp"
#include "query_v2_query_plan_common.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"

#include <random>

#define GTEST_COUT std::cerr << "[   INFO   ] "  // #NoCommit

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
    storage::v3::Gid node_gid_;
    storage::v3::Gid permission_gid_;
    storage::v3::Gid identity_gid_;

    bool operator<(const Result &other) const {
      if (node_gid_ != other.node_gid_) {
        return node_gid_ < other.node_gid_;
      }
      if (permission_gid_ != other.permission_gid_) {
        return permission_gid_ < other.permission_gid_;
      }
      return identity_gid_ < other.identity_gid_;
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
  const auto [number_of_vertices, frames_per_batch] = GetParam();
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

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n)
  auto scan_all_1 = MakeScanAllDistributed(storage, symbol_table, "n");

  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduceDistributed(scan_all_1.op_, output);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
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

  const auto [number_of_vertices, frames_per_batch] = GetParam();
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
  auto scan_all_1 = MakeScanAllByLabelDistributed(storage, symbol_table, "n", label_node);

  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduceDistributed(scan_all_1.op_, output);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
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

  const auto [number_of_vertices, frames_per_batch] = GetParam();

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
  auto scan_all_1 = MakeScanAllByLabelPropertyValueDistributed(storage, symbol_table, "n", label_node, property_node,
                                                               "someId", LITERAL("expectedValue"));
  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduceDistributed(scan_all_1.op_, output);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
  ASSERT_EQ(results.size(), gid_of_expected_vertices.size());
  for (auto result : results) {
    ASSERT_TRUE(result[0].IsVertex());
    auto gid = result[0].ValueVertex().Gid();
    auto it_found = gid_of_expected_vertices.find(gid);
    ASSERT_TRUE(it_found != gid_of_expected_vertices.end());
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, MatchAllWithIdFilteringWhileBatching) {
  /*
    QUERY:
      MATCH (n)
      WHERE id(n)=1
      RETURN *;

    QUERY PLAN:
      Produce {n}
      ScanAllById (n)
      Once
  */

  const auto [number_of_vertices, frames_per_batch] = GetParam();
  storage::v3::Gid id;  // We just want to have an idea of any vertex
  {                     // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      id = vertex_node.Gid();
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n) WHERE id(n)=1
  auto scan_all_1 = MakeScanAllByIdDistributed(storage, symbol_table, "n", LITERAL(id.AsInt()));

  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduceDistributed(scan_all_1.op_, output);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
  ASSERT_EQ(results.size(), 1);
  ASSERT_TRUE(results[0][0].IsVertex());
  ASSERT_EQ(results[0][0].ValueVertex().Gid(), id);
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
  const auto [number_of_vertices, frames_per_batch] = GetParam();

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
  auto scan_all = std::make_shared<distributed::ScanAll>(nullptr, symbol_anon1);

  // Expand (n)<-[anon2]-(anon1)
  auto expand_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_type};
  auto expand = std::make_shared<distributed::Expand>(scan_all, symbol_anon1, symbol_n, symbol_anon2,
                                                      EdgeAtom::Direction::OUT, expand_edge_types,
                                                      false /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output_n = NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduceDistributed(expand, output_n);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
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
  const auto [number_of_vertices, frames_per_batch] = GetParam();

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
  auto scan_all = std::make_shared<distributed::ScanAll>(nullptr, symbol_anon1);

  // Expand (n)<-[anon2]-(anon1)
  auto expand_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_type};
  auto expand = std::make_shared<distributed::Expand>(scan_all, symbol_anon1, symbol_n, symbol_anon2,
                                                      EdgeAtom::Direction::OUT, expand_edge_types,
                                                      false /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output_n = NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));
  auto produce = MakeProduceDistributed(expand, output_n);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
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
    CREATE INDEX ON :Node;
    CREATE INDEX ON :Permission;

  QUERY:
    MATCH (n:Node)
    MATCH (p:Permission)-[:IS_EDGE]->(n:Node)
    RETURN n, p;

  QUERY PLAN:
    Produce {n, p}
    Expand (p)-[anon2:IS_EDGE]->(n)
    ScanAllByLabel (n :Node)
    ScanAllByLabel (p :Permission)
    Once
  */
  const auto [number_of_vertices, frames_per_batch] = GetParam();

  storage::v3::EdgeTypeId edge_type{db_v3.NameToEdgeType("IS_EDGE")};
  auto label_node = db_v3.NameToLabel("Node");
  auto label_permission = db_v3.NameToLabel("Permission");

  auto gid_of_expected_vertices = std::set<std::pair<storage::v3::Gid, storage::v3::Gid>>{};
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;

    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_n = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_n.AddLabel(label_node).HasValue());

      auto vertex_p = *dba.InsertVertexAndValidate(schema_label, {},
                                                   {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_p.AddLabel(label_permission).HasValue());

      ASSERT_TRUE(dba.InsertEdge(&vertex_p, &vertex_n, edge_type).HasValue());
      auto [it, inserted] = gid_of_expected_vertices.insert(std::make_pair(vertex_n.Gid(), vertex_p.Gid()));
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }
  db_v3.CreateIndex(label_node);
  db_v3.CreateIndex(label_permission);

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto symbol_n = symbol_table.CreateSymbol("n", true);
  auto symbol_p = symbol_table.CreateSymbol("p", true);
  auto symbol_anon2 = symbol_table.CreateSymbol("anon2", true);

  // ScanAllByLabel (p :Permission)
  auto scan_all_1 = std::make_shared<distributed::ScanAllByLabel>(nullptr, symbol_p, label_permission,
                                                                  memgraph::storage::v3::View::OLD);

  // ScanAllByLabel (n :Node)
  auto scan_all_2 =
      std::make_shared<distributed::ScanAllByLabel>(scan_all_1, symbol_n, label_node, memgraph::storage::v3::View::OLD);

  // Expand (p)-[e]->(n)
  auto expand_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_type};

  auto expand = std::make_shared<distributed::Expand>(scan_all_2, symbol_n, symbol_p, symbol_anon2,
                                                      EdgeAtom::Direction::IN, expand_edge_types,
                                                      true /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output_n = NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));
  auto output_p = NEXPR("p", IDENT("p")->MapTo(symbol_p))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
  auto produce = MakeProduceDistributed(expand, output_n, output_p);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);
  auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);

  auto transformed_results = std::vector<std::pair<storage::v3::Gid, storage::v3::Gid>>{};
  std::transform(results.begin(), results.end(), std::back_inserter(transformed_results),
                 [](const std::vector<TypedValue> &result) {
                   auto gid_n = result[0].ValueVertex().Gid();
                   auto gid_p = result[1].ValueVertex().Gid();
                   return std::make_pair(gid_n, gid_p);
                 });
  for (auto &res : transformed_results) {
    GTEST_COUT << "gid_n=" << res.first.AsUint() << " gid_p=" << res.second.AsUint() << std::endl;
  }
  ASSERT_EQ(transformed_results.size(), gid_of_expected_vertices.size());
  for (auto result : transformed_results) {
    ASSERT_TRUE(gid_of_expected_vertices.end() != gid_of_expected_vertices.find(result));
  }
}

TEST_P(QueryPlanHardCodedQueriesTestFixture, HardCodedQuery) {
  /*
  INDEXES:
    CREATE INDEX ON :Node(platformId);
    CREATE INDEX ON :Permission;
    CREATE INDEX ON :Identity(email);

  QUERY:
    MATCH (i:Identity {email: 'rrr@clientdrive.com'})<-[:IS_FOR_IDENTITY]-(p:Permission)
    MATCH (p:Permission)-[:IS_FOR_NODE]->(n:Node)
    MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
    RETURN *;

  QUERY PLAN:
    Produce {i, n, p}
    Expand (p)-[anon1:IS_FOR_IDENTITY]->(i)
    ScanAllByLabelPropertyValue (i :Identity {email})
    Expand (n)<-[anon3:IS_FOR_NODE]-(p)
    ScanAllByLabel (p :Permission)
    ScanAllByLabelPropertyValue (n :Node {platformId})
    Once
  */
  const auto [number_of_clusters, frames_per_batch] = GetParam();
  auto label_node = db_v3.NameToLabel("Node");
  auto property_node_platformId = db_v3.NameToProperty("platformId");

  auto label_permission = db_v3.NameToLabel("Permission");

  auto label_identity = db_v3.NameToLabel("Idendity");
  auto property_identity_email = db_v3.NameToProperty("email");

  storage::v3::EdgeTypeId edge_is_for_node{db_v3.NameToEdgeType("IS_FOR_NODE")};
  storage::v3::EdgeTypeId edge_is_for_identity{db_v3.NameToEdgeType("IS_FOR_IDENTITY")};
  auto node_gids = std::set<storage::v3::Gid>{};
  auto permission_gids = std::set<storage::v3::Gid>{};
  auto identity_gids = std::set<storage::v3::Gid>{};

  auto expected_final_results = std::set<Result>{};

  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_clusters; ++idx) {
      storage::v3::Gid node_gid;
      storage::v3::Gid permission_gid;
      storage::v3::Gid identity_gid;

      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_node.AddLabel(label_node).HasValue());
      ASSERT_TRUE(vertex_node.SetProperty(property_node_platformId, storage::v3::PropertyValue("XXXXXXXXXXXXZZZZZZZZZ"))
                      .HasValue());
      node_gid = vertex_node.Gid();
      node_gids.insert(node_gid);

      auto vertex_permission = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_permission.AddLabel(label_permission).HasValue());
      permission_gid = vertex_permission.Gid();
      permission_gids.insert(permission_gid);

      auto edge_permission_to_node = dba.InsertEdge(&vertex_permission, &vertex_node, edge_is_for_node);
      ASSERT_TRUE(edge_permission_to_node.HasValue());

      auto vertex_identity = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      ASSERT_TRUE(vertex_identity.AddLabel(label_identity).HasValue());
      ASSERT_TRUE(
          vertex_identity.SetProperty(property_identity_email, storage::v3::PropertyValue("rrr@clientdrive.com"))
              .HasValue());
      identity_gid = vertex_identity.Gid();
      identity_gids.insert(identity_gid);

      auto edge_permission_to_identity = dba.InsertEdge(&vertex_permission, &vertex_identity, edge_is_for_identity);
      ASSERT_TRUE(edge_permission_to_identity.HasValue());

      auto result = Result{.node_gid_ = node_gid, .permission_gid_ = permission_gid, .identity_gid_ = identity_gid};
      auto [it, inserted] = expected_final_results.insert(result);
      ASSERT_TRUE(inserted);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  // INDEX CREATION
  db_v3.CreateIndex(label_node, property_node_platformId);
  db_v3.CreateIndex(label_permission);
  db_v3.CreateIndex(label_identity, property_identity_email);

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

  // example: TEST(QueryPlan, Accumulate) #NoCommit
  // MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
  auto scan_all_1 = std::make_shared<distributed::ScanAllByLabelPropertyValue>(
      nullptr /*input*/, symbol_n, label_node, property_node_platformId, "platformId", LITERAL("XXXXXXXXXXXXZZZZZZZZZ"),
      memgraph::storage::v3::View::OLD);

  // MATCH (p:Permission)
  auto scan_all_2 = std::make_shared<distributed::ScanAllByLabel>(scan_all_1, symbol_p, label_permission,
                                                                  memgraph::storage::v3::View::OLD);

  // (n:Node)<-[:IS_FOR_NODE]-(p:Permission)
  auto expand_1_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_is_for_node};
  // #NoCommit not sure about the two symbol_p, should we creat another symbol and map it (or not)?
  auto expand_1 = std::make_shared<distributed::Expand>(scan_all_2, symbol_p, symbol_p, symbol_anon3,
                                                        EdgeAtom::Direction::OUT, expand_1_edge_types,
                                                        false /*existing_node*/, memgraph::storage::v3::View::OLD);

  // MATCH (i:Identity {email: 'rrr@clientdrive.com'})
  auto scan_all_3 = std::make_shared<distributed::ScanAllByLabelPropertyValue>(
      expand_1, symbol_i, label_identity, property_identity_email, "email", LITERAL("rrr@clientdrive.com"),
      memgraph::storage::v3::View::OLD);

  // (p:Permission)-[:IS_FOR_IDENTITY]->(i:Identity {email: 'rrr@clientdrive.com'})
  auto expand_2_edge_types = std::vector<memgraph::storage::v3::EdgeTypeId>{edge_is_for_identity};
  // #NoCommit not sure about the two symbol_i, should we creat another symbol and map it (or not)?
  auto expand_2 = std::make_shared<distributed::Expand>(scan_all_3, symbol_i, symbol_i, symbol_anon1,
                                                        EdgeAtom::Direction::IN, expand_2_edge_types,
                                                        false /*existing_node*/, memgraph::storage::v3::View::OLD);
  {
    /*
    Checking result from:
      MATCH (i:Identity {email: 'rrr@clientdrive.com'})<-[:IS_FOR_IDENTITY]-(p:Permission)
      MATCH (p:Permission)-[:IS_FOR_NODE]->(n:Node)
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n =
        NEXPR("n", IDENT("n")->MapTo(symbol_n))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));
    auto output_p =
        NEXPR("p", IDENT("p")->MapTo(symbol_p))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
    auto output_i =
        NEXPR("i", IDENT("i")->MapTo(symbol_i))->MapTo(symbol_table.CreateSymbol("named_expression_i", true));

    auto produce = MakeProduceDistributed(expand_2, output_n, output_p, output_i);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);

    auto transformed_results = std::vector<Result>{};
    std::transform(results.begin(), results.end(), std::back_inserter(transformed_results),
                   [](const std::vector<TypedValue> &result) -> Result {
                     auto node_gid = result[0].ValueVertex().Gid();
                     auto permission_gid = result[1].ValueVertex().Gid();
                     auto identity_gid = result[2].ValueVertex().Gid();
                     return Result{
                         .node_gid_ = node_gid, .permission_gid_ = permission_gid, .identity_gid_ = identity_gid};
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
        std::make_pair(342, 100)  /* 342 vertices, 100 frames per batch: to check resizing of frames works when having
                                     several iterations. There will be only 42 vertices left on the last batch.*/
        ));
}  // namespace memgraph::query::v2::tests
