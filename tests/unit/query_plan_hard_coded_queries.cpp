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

#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"
#include "query_plan_common.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using test_common::ToIntList;
using test_common::ToIntMap;
using testing::UnorderedElementsAre;

namespace memgraph::query::v2::tests {

class QueryPlanHardCodedQueriesTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  memgraph::storage::Storage db_v2;
};

TEST_F(QueryPlanHardCodedQueriesTest, HardCodedQuery_v2_scanAll) {
  /*
  QUERY:
    MATCH (n)
    RETURN *;

  QUERY PLAN:
    Produce {n}
    ScanAll (n)
    Once
  */
  {  // Inserting data
    auto storage_dba = db_v2.Access();
    DbAccessor dba(&storage_dba);

    auto vertex_node = dba.InsertVertex();

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v2.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n)
  auto scan_all_1 = MakeScanAll(storage, symbol_table, "n");

  {
    /*
    Checking temporary result from:
      MATCH (n)
    */
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all_1.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 1);
  }
}

TEST_F(QueryPlanHardCodedQueriesTest, HardCodedQuery_v2) {
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
  auto label_node = db_v2.NameToLabel("Node");
  auto property_node_platformId = db_v2.NameToProperty("platformId");

  auto label_permission = db_v2.NameToLabel("Permission");

  auto label_identity = db_v2.NameToLabel("Idendity");
  auto property_identity_email = db_v2.NameToProperty("email");

  storage::EdgeTypeId edge_is_for_node{db_v2.NameToEdgeType("IS_FOR_NODE")};
  storage::EdgeTypeId edge_is_for_identity{db_v2.NameToEdgeType("IS_FOR_IDENTITY")};

  {  // Inserting data
    auto storage_dba = db_v2.Access();
    DbAccessor dba(&storage_dba);

    auto vertex_node = dba.InsertVertex();
    ASSERT_TRUE(vertex_node.AddLabel(label_node).HasValue());
    ASSERT_TRUE(
        vertex_node.SetProperty(property_node_platformId, storage::PropertyValue("XXXXXXXXXXXXZZZZZZZZZ")).HasValue());

    auto vertex_permission = dba.InsertVertex();
    ASSERT_TRUE(vertex_permission.AddLabel(label_permission).HasValue());

    auto edge_permission_to_node = dba.InsertEdge(&vertex_permission, &vertex_node, edge_is_for_node);
    ASSERT_TRUE(edge_permission_to_node.HasValue());

    auto vertex_identity = dba.InsertVertex();
    ASSERT_TRUE(vertex_identity.AddLabel(label_identity).HasValue());
    ASSERT_TRUE(
        vertex_identity.SetProperty(property_identity_email, storage::PropertyValue("rrr@clientdrive.com")).HasValue());

    auto edge_permission_to_identity = dba.InsertEdge(&vertex_permission, &vertex_identity, edge_is_for_identity);
    ASSERT_TRUE(edge_permission_to_identity.HasValue());

    ASSERT_FALSE(dba.Commit().HasError());
  }

  // INDEX CREATION
  db_v2.CreateIndex(label_node, property_node_platformId);
  db_v2.CreateIndex(label_permission);
  db_v2.CreateIndex(label_identity, property_identity_email);

  auto storage_dba = db_v2.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
  auto scan_all_1 = MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label_node, property_node_platformId,
                                                    "platformId", LITERAL("XXXXXXXXXXXXZZZZZZZZZ"));

  {
    /*
    Checking temporary result from:
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
    */
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all_1.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), 1);
  }

  // MATCH (p:Permission)
  auto scan_all_2 = MakeScanAllByLabel(storage, symbol_table, "p", label_permission, scan_all_1.op_);
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
    auto produce = MakeProduce(scan_all_2.op_, output_n, output_p);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 2);
  }

  // (p:Permission)-[:IS_FOR_NODE]->(n:Node)
  auto expand_1 = MakeExpand(storage, symbol_table, scan_all_2.op_, scan_all_2.sym_, "e", EdgeAtom::Direction::OUT,
                             {edge_is_for_node}, "p", false /*existing_node*/, memgraph::storage::View::OLD);
  {
    /*
    Checking temporary result from:
      MATCH (p:Permission)-[:IS_FOR_NODE]->(n:Node)
      MATCH (n:Node {platformId: 'XXXXXXXXXXXXZZZZZZZZZ'})
      RETURN *;
    */
    auto output_n = NEXPR("n", IDENT("n")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
    auto output_p = NEXPR("p", IDENT("p")->MapTo(expand_1.node_sym_))->MapTo(symbol_table.CreateSymbol("p", true));
    auto produce = MakeProduce(expand_1.op_, output_n, output_p);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 2);
  }

  // MATCH (i:Identity {email: 'rrr@clientdrive.com'})
  auto scan_all_3 = MakeScanAllByLabelPropertyValue(storage, symbol_table, "i", label_identity, property_identity_email,
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

    auto produce = MakeProduce(scan_all_3.op_, output_n, output_p, output_i);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
  }

  // (i:Identity {email: 'rrr@clientdrive.com'})<-[:IS_FOR_IDENTITY]-(p:Permission)
  auto expand_2 = MakeExpand(storage, symbol_table, scan_all_3.op_, scan_all_3.sym_, "e", EdgeAtom::Direction::OUT,
                             {edge_is_for_identity}, "i", false /*existing_node*/, memgraph::storage::View::OLD);
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
    auto produce = MakeProduce(scan_all_3.op_, output_n, output_p, output_i);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
  }
}
}  // namespace memgraph::query::v2::tests
