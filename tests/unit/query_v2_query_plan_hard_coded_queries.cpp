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

  storage::v3::Storage db_v3;
  const storage::v3::LabelId schema_label{db_v3.NameToLabel("label")};
  const storage::v3::PropertyId schema_property{db_v3.NameToProperty("property")};
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

TEST_P(QueryPlanHardCodedQueriesTestFixture, DISABLED_MatchAllWithExpandWhileBatching) {
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

  // ScanAll (anon1)
  auto scan_all = MakeScanAllDistributed(storage, symbol_table, "anon1");

  {
    auto output = NEXPR("anon1", IDENT("anon1")->MapTo(scan_all.sym_))
                      ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduceDistributed(scan_all.op_, output);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, frames_per_batch);
    ASSERT_EQ(results.size(), 1 + number_of_vertices);  // Center node + number_of_vertices
  }

  // Expand (n)<-[anon2]-(anon1)
  auto expand =
      MakeExpandDistributed(storage, symbol_table, scan_all.op_, scan_all.sym_, "anon2", EdgeAtom::Direction::OUT,
                            {edge_type}, "n", false /*existing_node*/, memgraph::storage::v3::View::OLD);

  auto output =
      NEXPR("n", IDENT("n")->MapTo(expand.node_sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduceDistributed(expand.op_, output);
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

TEST_P(QueryPlanHardCodedQueriesTestFixture, DISABLED_HardCodedQuery) {
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
  const auto [unused, number_of_frames_per_batch] = GetParam();
  auto label_node = db_v3.NameToLabel("Node");
  auto property_node_platformId = db_v3.NameToProperty("platformId");

  auto label_permission = db_v3.NameToLabel("Permission");

  auto label_identity = db_v3.NameToLabel("Idendity");
  auto property_identity_email = db_v3.NameToProperty("email");

  storage::v3::EdgeTypeId edge_is_for_node{db_v3.NameToEdgeType("IS_FOR_NODE")};
  storage::v3::EdgeTypeId edge_is_for_identity{db_v3.NameToEdgeType("IS_FOR_IDENTITY")};

  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    auto vertex_node = *dba.InsertVertexAndValidate(schema_label, {},
                                                    {{schema_property, storage::v3::PropertyValue(++property_index)}});
    ASSERT_TRUE(vertex_node.AddLabel(label_node).HasValue());
    ASSERT_TRUE(vertex_node.SetProperty(property_node_platformId, storage::v3::PropertyValue("XXXXXXXXXXXXZZZZZZZZZ"))
                    .HasValue());

    auto vertex_permission = *dba.InsertVertexAndValidate(
        schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
    ASSERT_TRUE(vertex_permission.AddLabel(label_permission).HasValue());

    auto edge_permission_to_node = dba.InsertEdge(&vertex_permission, &vertex_node, edge_is_for_node);
    ASSERT_TRUE(edge_permission_to_node.HasValue());

    auto vertex_identity = *dba.InsertVertexAndValidate(
        schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
    ASSERT_TRUE(vertex_identity.AddLabel(label_identity).HasValue());
    ASSERT_TRUE(vertex_identity.SetProperty(property_identity_email, storage::v3::PropertyValue("rrr@clientdrive.com"))
                    .HasValue());

    auto edge_permission_to_identity = dba.InsertEdge(&vertex_permission, &vertex_identity, edge_is_for_identity);
    ASSERT_TRUE(edge_permission_to_identity.HasValue());

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

  // MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
  auto scan_all_1 = MakeScanAllByLabelPropertyValueDistributed(
      storage, symbol_table, "n", label_node, property_node_platformId, "platformId", LITERAL("XXXXXXXXXXXXZZZZZZZZZ"));

  {
    /*
    Checking temporary result from:
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
    */
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduceDistributed(scan_all_1.op_, output);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, number_of_frames_per_batch);
    ASSERT_EQ(results.size(), 1);
  }

  // MATCH (p:Permission)
  auto scan_all_2 = MakeScanAllByLabelDistributed(storage, symbol_table, "p", label_permission, scan_all_1.op_);
  {
    /*
    Checking temporary result from:
      MATCH (p:Permission)
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n = NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
    auto output_p =
        NEXPR("p", IDENT("p")->MapTo(scan_all_2.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
    auto produce = MakeProduceDistributed(scan_all_2.op_, output_n, output_p);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, number_of_frames_per_batch);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 2);
  }

  // (p:Permission)-[:IS_FOR_NODE]->(n:Node)
  auto expand_1 =
      MakeExpandDistributed(storage, symbol_table, scan_all_2.op_, scan_all_2.sym_, "anon3", EdgeAtom::Direction::OUT,
                            {edge_is_for_node}, "p", false /*existing_node*/, memgraph::storage::v3::View::OLD);
  {
    /*
    Checking temporary result from:
      MATCH (p:Permission)-[:IS_FOR_NODE]->(n:Node)
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n = NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
    auto output_p = NEXPR("p", IDENT("p")->MapTo(expand_1.node_sym_))->MapTo(symbol_table.CreateSymbol("p", true));
    auto produce = MakeProduceDistributed(expand_1.op_, output_n, output_p);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, number_of_frames_per_batch);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 2);
  }

  // MATCH (i:Identity {email: 'rrr@clientdrive.com'})
  auto scan_all_3 =
      MakeScanAllByLabelPropertyValueDistributed(storage, symbol_table, "i", label_identity, property_identity_email,
                                                 "email", LITERAL("rrr@clientdrive.com"), expand_1.op_);

  {
    /*
    Checking temporary result from:
      MATCH (i:Identity {email: 'rrr@clientdrive.com'})
      MATCH (p:Permission)-[:IS_FOR_NODE]->(n:Node)
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n = NEXPR("n", IDENT("n")->MapTo(scan_all_3.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
    auto output_p = NEXPR("p", IDENT("p")->MapTo(expand_1.node_sym_))->MapTo(symbol_table.CreateSymbol("p", true));
    auto output_i = NEXPR("i", IDENT("i")->MapTo(scan_all_3.sym_))->MapTo(symbol_table.CreateSymbol("i", true));

    auto produce = MakeProduceDistributed(scan_all_3.op_, output_n, output_p, output_i);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, number_of_frames_per_batch);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
  }

  // (i:Identity {email: 'rrr@clientdrive.com'})<-[:IS_FOR_IDENTITY]-(p:Permission)
  auto expand_2 =
      MakeExpandDistributed(storage, symbol_table, scan_all_3.op_, scan_all_3.sym_, "e", EdgeAtom::Direction::OUT,
                            {edge_is_for_identity}, "i", false /*existing_node*/, memgraph::storage::v3::View::OLD);
  {
    /*
    Checking temporary result from:
      MATCH (i:Identity {email: 'rrr@clientdrive.com'})<-[:IS_FOR_IDENTITY]-(p:Permission)
      MATCH (p:Permission)-[:IS_FOR_NODE]->(n:Node)
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n = NEXPR("n", IDENT("n")->MapTo(scan_all_3.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
    auto output_p = NEXPR("p", IDENT("p")->MapTo(expand_1.node_sym_))->MapTo(symbol_table.CreateSymbol("p", true));
    auto output_i = NEXPR("i", IDENT("i")->MapTo(expand_2.node_sym_))->MapTo(symbol_table.CreateSymbol("i", true));
    auto produce = MakeProduceDistributed(scan_all_3.op_, output_n, output_p, output_i);
    auto context = MakeContextDistributed(storage, symbol_table, &dba);
    auto results = CollectProduceDistributed(*produce, &context, number_of_frames_per_batch);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
  }
}

INSTANTIATE_TEST_CASE_P(
    QueryPlanHardCodedQueriesTest, QueryPlanHardCodedQueriesTestFixture,
    ::testing::Values(
        std::make_pair(1, 1),     /* 1 vertex, 1 frame per batch: simple case. */
        std::make_pair(2, 1),     /* 2 vertices, 2 frame per batch: simple case. */
        std::make_pair(3, 3),     /* 2 vertices, 2 frame per batch: simple case. */
        std::make_pair(100, 1),   /* 100 vertices, 1 frame per batch: to check previous
                                    behavior (ie: pre-batching). */
        std::make_pair(1, 2),     /* 1 vertex, 2 batches. */
        std::make_pair(4, 2),     /* 4 vertices, 2 frames per batch. */
        std::make_pair(100, 100), /* 100 vertices, 100 frames per batch: to check batching works with 1 iteration. */
        std::make_pair(100, 50),  /* 100 vertices, 50 frames per batch: to check batching works with 2 iterations. */
        std::make_pair(37, 100),  /* 37 vertices, 100 frames per batch: to check resizing of frames works when having 1
                                    iteration only. */
        std::make_pair(342, 100)  /* 342 vertices, 100 frames per batch: to check resizing of frames works when having
                                     several iterations. There will be only 42 vertices left on the last batch.*/
        ));
}  // namespace memgraph::query::v2::tests
