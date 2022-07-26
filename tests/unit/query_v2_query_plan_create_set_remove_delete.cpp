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

#include <iterator>
#include <memory>
#include <variant>
#include <vector>

#include "common/types.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"

#include "query_plan_common.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schemas.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/view.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

namespace memgraph::query::tests {

class QueryPlanCRUDTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(db.CreateSchema(label, {storage::SchemaProperty{property, common::SchemaType::INT}}));
  }

  storage::Storage db;
  const storage::LabelId label{db.NameToLabel("label")};
  const storage::PropertyId property{db.NameToProperty("property")};
};

TEST_F(QueryPlanCRUDTest, CreateNodeWithAttributes) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  AstStorage storage;
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto context = MakeContext(storage, symbol_table, &dba);
  PullAll(*create, &context);
  dba.AdvanceCommand();

  // count the number of vertices
  int vertex_count = 0;
  for (auto vertex : dba.Vertices(storage::View::OLD)) {
    vertex_count++;
    auto maybe_labels = vertex.Labels(storage::View::OLD);
    ASSERT_TRUE(maybe_labels.HasValue());
    const auto &labels = *maybe_labels;
    EXPECT_EQ(labels.size(), 0);

    auto maybe_properties = vertex.Properties(storage::View::OLD);
    ASSERT_TRUE(maybe_properties.HasValue());
    const auto &properties = *maybe_properties;
    EXPECT_EQ(properties.size(), 1);
    auto maybe_prop = vertex.GetProperty(storage::View::OLD, property);
    ASSERT_TRUE(maybe_prop.HasValue());
    auto prop_eq = TypedValue(*maybe_prop) == TypedValue(42);
    ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
    EXPECT_TRUE(prop_eq.ValueBool());
  }
  EXPECT_EQ(vertex_count, 1);
}

TEST(QueryPlan, CreateReturn) {
  // test CREATE (n:Person {age: 42}) RETURN n, n.age
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  storage::LabelId label = dba.NameToLabel("Person");
  auto property = PROPERTY_PAIR("property");
  db.CreateSchema(label, {storage::SchemaProperty{property.second, common::SchemaType::INT}});

  AstStorage storage;
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property.second, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto named_expr_n =
      NEXPR("n", IDENT("n")->MapTo(node.symbol))->MapTo(symbol_table.CreateSymbol("named_expr_n", true));
  auto prop_lookup = PROPERTY_LOOKUP(IDENT("n")->MapTo(node.symbol), property);
  auto named_expr_n_p = NEXPR("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("named_expr_n_p", true));

  auto produce = MakeProduce(create, named_expr_n, named_expr_n_p);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(2, results[0].size());
  EXPECT_EQ(TypedValue::Type::Vertex, results[0][0].type());
  auto maybe_labels = results[0][0].ValueVertex().Labels(storage::View::NEW);
  EXPECT_EQ(maybe_labels->size(), 0);

  EXPECT_EQ(TypedValue::Type::Int, results[0][1].type());
  EXPECT_EQ(42, results[0][1].ValueInt());

  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
}

TEST(QueryPlan, CreateExpand) {
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  storage::LabelId label_node_1 = dba.NameToLabel("Node1");
  storage::LabelId label_node_2 = dba.NameToLabel("Node2");
  auto property = PROPERTY_PAIR("property");
  storage::EdgeTypeId edge_type = dba.NameToEdgeType("edge_type");
  db.CreateSchema(label_node_1, {storage::SchemaProperty{property.second, common::SchemaType::INT}});
  db.CreateSchema(label_node_2, {storage::SchemaProperty{property.second, common::SchemaType::INT}});

  SymbolTable symbol_table;
  AstStorage storage;

  auto test_create_path = [&](bool cycle, int expected_nodes_created, int expected_edges_created) {
    int before_v = CountIterable(dba.Vertices(storage::View::OLD));
    int before_e = CountEdges(&dba, storage::View::OLD);

    // data for the first node
    NodeCreationInfo n;
    n.symbol = symbol_table.CreateSymbol("n", true);
    n.labels.emplace_back(label_node_1);
    std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(n.properties)
        .emplace_back(property.second, LITERAL(1));

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n.symbol : symbol_table.CreateSymbol("m", true);
    m.labels.emplace_back(label_node_2);
    std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(m.properties)
        .emplace_back(property.second, LITERAL(2));

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.edge_type = edge_type;
    std::get<0>(r.properties).emplace_back(property.second, LITERAL(3));

    auto create_op = std::make_shared<CreateNode>(nullptr, n);
    auto create_expand = std::make_shared<CreateExpand>(m, r, create_op, n.symbol, cycle);
    auto context = MakeContext(storage, symbol_table, &dba);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();

    EXPECT_EQ(CountIterable(dba.Vertices(storage::View::OLD)) - before_v, expected_nodes_created);
    EXPECT_EQ(CountEdges(&dba, storage::View::OLD) - before_e, expected_edges_created);
  };

  test_create_path(false, 2, 1);
  test_create_path(true, 1, 1);

  for (auto vertex : dba.Vertices(storage::View::OLD)) {
    auto maybe_labels = vertex.Labels(storage::View::OLD);
    MG_ASSERT(maybe_labels.HasValue());
    const auto &labels = *maybe_labels;
    EXPECT_EQ(labels.size(), 0);
    auto maybe_primary_label = vertex.PrimaryLabel(storage::View::OLD);
    ASSERT_TRUE(maybe_primary_label.HasValue());
    if (*maybe_primary_label == label_node_1) {
      // node created by first op
      EXPECT_EQ(vertex.GetProperty(storage::View::OLD, property.second)->ValueInt(), 1);
    } else if (*maybe_primary_label == label_node_2) {
      // node create by expansion
      EXPECT_EQ(vertex.GetProperty(storage::View::OLD, property.second)->ValueInt(), 2);
    } else {
      // should not happen
      FAIL();
    }

    for (auto vertex : dba.Vertices(storage::View::OLD)) {
      auto maybe_edges = vertex.OutEdges(storage::View::OLD);
      MG_ASSERT(maybe_edges.HasValue());
      for (auto edge : *maybe_edges) {
        EXPECT_EQ(edge.EdgeType(), edge_type);
        EXPECT_EQ(edge.GetProperty(storage::View::OLD, property.second)->ValueInt(), 3);
      }
    }
  }
}

TEST_F(QueryPlanCRUDTest, MatchCreateNode) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}}).HasValue());
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}}).HasValue());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  AstStorage storage;

  // first node
  auto n_scan_all = MakeScanAll(storage, symbol_table, "n");
  // second node
  NodeCreationInfo m;
  m.symbol = symbol_table.CreateSymbol("m", true);
  m.labels = {label};
  std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(m.properties).emplace_back(property, LITERAL(1));

  // creation op
  auto create_node = std::make_shared<CreateNode>(n_scan_all.op_, m);

  EXPECT_EQ(CountIterable(dba.Vertices(storage::View::OLD)), 3);
  auto context = MakeContext(storage, symbol_table, &dba);
  PullAll(*create_node, &context);
  dba.AdvanceCommand();
  EXPECT_EQ(CountIterable(dba.Vertices(storage::View::OLD)), 6);
}

TEST_F(QueryPlanCRUDTest, MatchCreateExpand) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}}).HasValue());
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}}).HasValue());
  dba.AdvanceCommand();

  //  storage::LabelId label_node_1 = dba.NameToLabel("Node1");
  //  storage::LabelId label_node_2 = dba.NameToLabel("Node2");
  //  storage::PropertyId property = dba.NameToLabel("prop");
  storage::EdgeTypeId edge_type = dba.NameToEdgeType("edge_type");

  SymbolTable symbol_table;
  AstStorage storage;

  auto test_create_path = [&](bool cycle, int expected_nodes_created, int expected_edges_created) {
    int before_v = CountIterable(dba.Vertices(storage::View::OLD));
    int before_e = CountEdges(&dba, storage::View::OLD);

    // data for the first node
    auto n_scan_all = MakeScanAll(storage, symbol_table, "n");

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n_scan_all.sym_ : symbol_table.CreateSymbol("m", true);
    m.labels = {label};
    std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(m.properties)
        .emplace_back(property, LITERAL(1));

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.direction = EdgeAtom::Direction::OUT;
    r.edge_type = edge_type;

    auto create_expand = std::make_shared<CreateExpand>(m, r, n_scan_all.op_, n_scan_all.sym_, cycle);
    auto context = MakeContext(storage, symbol_table, &dba);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();

    EXPECT_EQ(CountIterable(dba.Vertices(storage::View::OLD)) - before_v, expected_nodes_created);
    EXPECT_EQ(CountEdges(&dba, storage::View::OLD) - before_e, expected_edges_created);
  };

  test_create_path(false, 3, 3);
  test_create_path(true, 0, 6);
}

TEST_F(QueryPlanCRUDTest, Delete) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 4; ++i) {
    vertices.push_back(*dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(i)}}));
  }
  auto type = dba.NameToEdgeType("type");
  for (int j = 0; j < 4; ++j)
    for (int k = j + 1; k < 4; ++k) ASSERT_TRUE(dba.InsertEdge(&vertices[j], &vertices[k], type).HasValue());

  dba.AdvanceCommand();
  EXPECT_EQ(4, CountIterable(dba.Vertices(storage::View::OLD)));
  EXPECT_EQ(6, CountEdges(&dba, storage::View::OLD));

  AstStorage storage;
  SymbolTable symbol_table;

  // attempt to delete a vertex, and fail
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*delete_op, &context), QueryRuntimeException);
    dba.AdvanceCommand();
    EXPECT_EQ(4, CountIterable(dba.Vertices(storage::View::OLD)));
    EXPECT_EQ(6, CountEdges(&dba, storage::View::OLD));
  }

  // detach delete a single vertex
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, true);
    Frame frame(symbol_table.max_position());
    auto context = MakeContext(storage, symbol_table, &dba);
    delete_op->MakeCursor(utils::NewDeleteResource())->Pull(frame, context);
    dba.AdvanceCommand();
    EXPECT_EQ(3, CountIterable(dba.Vertices(storage::View::OLD)));
    EXPECT_EQ(3, CountEdges(&dba, storage::View::OLD));
  }

  // delete all remaining edges
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                          storage::View::NEW);
    auto r_get = storage.Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto delete_op = std::make_shared<plan::Delete>(r_m.op_, std::vector<Expression *>{r_get}, false);
    auto context = MakeContext(storage, symbol_table, &dba);
    PullAll(*delete_op, &context);
    dba.AdvanceCommand();
    EXPECT_EQ(3, CountIterable(dba.Vertices(storage::View::OLD)));
    EXPECT_EQ(0, CountEdges(&dba, storage::View::OLD));
  }

  // delete all remaining vertices
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
    auto context = MakeContext(storage, symbol_table, &dba);
    PullAll(*delete_op, &context);
    dba.AdvanceCommand();
    EXPECT_EQ(0, CountIterable(dba.Vertices(storage::View::OLD)));
    EXPECT_EQ(0, CountEdges(&dba, storage::View::OLD));
  }
}

TEST_F(QueryPlanCRUDTest, DeleteTwiceDeleteBlockingEdge) {
  // test deleting the same vertex and edge multiple times
  //
  // also test vertex deletion succeeds if the prohibiting
  // edge is deleted in the same logical op
  //
  // we test both with the following queries (note the
  // undirected edge in MATCH):
  //
  // CREATE (:label{property: 1})-[:T]->(:label{property: 2})
  // MATCH (n)-[r]-(m) [DETACH] DELETE n, r, m

  auto test_delete = [this](bool detach) {
    auto storage_dba = db.Access();
    DbAccessor dba(&storage_dba);

    auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
    auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
    ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("T")).HasValue());
    dba.AdvanceCommand();
    EXPECT_EQ(2, CountIterable(dba.Vertices(storage::View::OLD)));
    EXPECT_EQ(1, CountEdges(&dba, storage::View::OLD));

    AstStorage storage;
    SymbolTable symbol_table;

    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::BOTH, {}, "m", false,
                          storage::View::OLD);

    // getter expressions for deletion
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto r_get = storage.Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto m_get = storage.Create<Identifier>("m")->MapTo(r_m.node_sym_);

    auto delete_op = std::make_shared<plan::Delete>(r_m.op_, std::vector<Expression *>{n_get, r_get, m_get}, detach);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(2, PullAll(*delete_op, &context));
    dba.AdvanceCommand();
    EXPECT_EQ(0, CountIterable(dba.Vertices(storage::View::OLD)));
    EXPECT_EQ(0, CountEdges(&dba, storage::View::OLD));
  };

  test_delete(true);
  test_delete(false);
}

TEST_F(QueryPlanCRUDTest, DeleteReturn) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  for (int i = 0; i < 4; ++i) {
    const auto property_value = storage::PropertyValue(i);
    auto va = *dba.InsertVertexAndValidate(label, {}, {{property, property_value}});
    EXPECT_EQ(*va.GetProperty(storage::View::NEW, property), property_value);
  }

  dba.AdvanceCommand();
  EXPECT_EQ(4, CountIterable(dba.Vertices(storage::View::OLD)));
  EXPECT_EQ(0, CountEdges(&dba, storage::View::OLD));

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");

  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, true);

  auto prop_lookup = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), property);
  auto n_p = storage.Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
  auto produce = MakeProduce(delete_op, n_p);

  auto context = MakeContext(storage, symbol_table, &dba);
  ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
}

TEST(QueryPlan, DeleteNull) {
  // test (simplified) WITH Null as x delete x
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto once = std::make_shared<Once>();
  auto delete_op = std::make_shared<plan::Delete>(once, std::vector<Expression *>{LITERAL(TypedValue())}, false);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*delete_op, &context));
}

TEST_F(QueryPlanCRUDTest, DeleteAdvance) {
  // test queries on empty DB:
  // CREATE (n: label{property: 1})
  // MATCH (n) DELETE n WITH n ...
  // this fails only if the deleted record `n` is actually used in subsequent
  // clauses, which is compatible with Neo's behavior.
  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto advance = std::make_shared<Accumulate>(delete_op, std::vector<Symbol>{n.sym_}, true);
  auto res_sym = symbol_table.CreateSymbol("res", true);
  {
    auto storage_dba = db.Access();
    DbAccessor dba(&storage_dba);
    ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
    dba.AdvanceCommand();
    auto produce = MakeProduce(advance, NEXPR("res", LITERAL(42))->MapTo(res_sym));
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(1, PullAll(*produce, &context));
  }
  {
    auto storage_dba = db.Access();
    DbAccessor dba(&storage_dba);
    ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}}).HasValue());
    dba.AdvanceCommand();
    auto n_prop = PROPERTY_LOOKUP(n_get, dba.NameToProperty("prop"));
    auto produce = MakeProduce(advance, NEXPR("res", n_prop)->MapTo(res_sym));
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*produce, &context), QueryRuntimeException);
  }
}

TEST_F(QueryPlanCRUDTest, SetProperty) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set

  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
  auto v3 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}});
  auto v4 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(4)}});
  auto edge_type = dba.NameToEdgeType("edge_type");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v2, &v4, edge_type).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        storage::View::OLD);

  // set prop1 to 42 on n and r
  auto prop1 = dba.NameToProperty("prop1");
  auto literal = LITERAL(42);

  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop1);
  auto set_n_p = std::make_shared<plan::SetProperty>(r_m.op_, prop1, n_p, literal);

  auto r_p = PROPERTY_LOOKUP(IDENT("r")->MapTo(r_m.edge_sym_), prop1);
  auto set_r_p = std::make_shared<plan::SetProperty>(set_n_p, prop1, r_p, literal);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set_r_p, &context));
  dba.AdvanceCommand();

  EXPECT_EQ(CountEdges(&dba, storage::View::OLD), 2);
  for (auto vertex : dba.Vertices(storage::View::OLD)) {
    auto maybe_edges = vertex.OutEdges(storage::View::OLD);
    ASSERT_TRUE(maybe_edges.HasValue());
    for (auto edge : *maybe_edges) {
      ASSERT_EQ(edge.GetProperty(storage::View::OLD, prop1)->type(), storage::PropertyValue::Type::Int);
      EXPECT_EQ(edge.GetProperty(storage::View::OLD, prop1)->ValueInt(), 42);
      auto from = edge.From();
      auto to = edge.To();
      ASSERT_EQ(from.GetProperty(storage::View::OLD, prop1)->type(), storage::PropertyValue::Type::Int);
      EXPECT_EQ(from.GetProperty(storage::View::OLD, prop1)->ValueInt(), 42);
      ASSERT_EQ(to.GetProperty(storage::View::OLD, prop1)->type(), storage::PropertyValue::Type::Null);
    }
  }
}

TEST_F(QueryPlanCRUDTest, SetProperties) {
  auto test_set_properties = [this](bool update) {
    auto storage_dba = db.Access();
    DbAccessor dba(&storage_dba);

    // graph: ({a: 0})-[:R {b:1}]->({c:2})
    auto prop_a = dba.NameToProperty("a");
    auto prop_b = dba.NameToProperty("b");
    auto prop_c = dba.NameToProperty("c");
    auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
    auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
    dba.AdvanceCommand();

    auto e = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("R"));
    ASSERT_TRUE(v1.SetPropertyAndValidate(prop_a, storage::PropertyValue(0)).HasValue());
    ASSERT_TRUE(e->SetProperty(prop_b, storage::PropertyValue(1)).HasValue());
    ASSERT_TRUE(v2.SetPropertyAndValidate(prop_c, storage::PropertyValue(2)).HasValue());
    dba.AdvanceCommand();

    AstStorage storage;
    SymbolTable symbol_table;

    // scan (n)-[r]->(m)
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                          storage::View::OLD);

    auto op = update ? plan::SetProperties::Op::UPDATE : plan::SetProperties::Op::REPLACE;

    // set properties on r to n, and on r to m
    auto r_ident = IDENT("r")->MapTo(r_m.edge_sym_);
    auto m_ident = IDENT("m")->MapTo(r_m.node_sym_);
    auto set_r_to_n = std::make_shared<plan::SetProperties>(r_m.op_, n.sym_, r_ident, op);
    auto set_m_to_r = std::make_shared<plan::SetProperties>(set_r_to_n, r_m.edge_sym_, m_ident, op);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(1, PullAll(*set_m_to_r, &context));
    dba.AdvanceCommand();

    EXPECT_EQ(CountEdges(&dba, storage::View::OLD), 1);
    for (auto vertex : dba.Vertices(storage::View::OLD)) {
      auto maybe_edges = vertex.OutEdges(storage::View::OLD);
      ASSERT_TRUE(maybe_edges.HasValue());
      for (auto edge : *maybe_edges) {
        auto from = edge.From();
        EXPECT_EQ(from.Properties(storage::View::OLD)->size(), update ? 3 : 1);
        if (update) {
          ASSERT_EQ(from.GetProperty(storage::View::OLD, prop_a)->type(), storage::PropertyValue::Type::Int);
          EXPECT_EQ(from.GetProperty(storage::View::OLD, prop_a)->ValueInt(), 0);
        }
        ASSERT_EQ(from.GetProperty(storage::View::OLD, prop_b)->type(), storage::PropertyValue::Type::Int);
        EXPECT_EQ(from.GetProperty(storage::View::OLD, prop_b)->ValueInt(), 1);

        EXPECT_EQ(edge.Properties(storage::View::OLD)->size(), update ? 3 : 2);
        if (update) {
          ASSERT_EQ(edge.GetProperty(storage::View::OLD, prop_b)->type(), storage::PropertyValue::Type::Int);
          EXPECT_EQ(edge.GetProperty(storage::View::OLD, prop_b)->ValueInt(), 1);
        }
        ASSERT_EQ(edge.GetProperty(storage::View::OLD, prop_c)->type(), storage::PropertyValue::Type::Int);
        EXPECT_EQ(edge.GetProperty(storage::View::OLD, prop_c)->ValueInt(), 2);

        auto to = edge.To();
        EXPECT_EQ(to.Properties(storage::View::OLD)->size(), 2);
        ASSERT_EQ(to.GetProperty(storage::View::OLD, prop_c)->type(), storage::PropertyValue::Type::Int);
        EXPECT_EQ(to.GetProperty(storage::View::OLD, prop_c)->ValueInt(), 2);
      }
    }
  };

  test_set_properties(true);
  test_set_properties(false);
}

TEST_F(QueryPlanCRUDTest, SetSecondaryLabels) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});

  auto label1 = dba.NameToLabel("label1");
  auto label2 = dba.NameToLabel("label2");
  auto label3 = dba.NameToLabel("label3");
  ASSERT_TRUE(v1.AddLabel(label1).HasValue());
  ASSERT_TRUE(v2.AddLabel(label1).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_set = std::make_shared<plan::SetLabels>(n.op_, n.sym_, std::vector<storage::LabelId>{label2, label3});
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*label_set, &context));

  for (auto vertex : dba.Vertices(storage::View::OLD)) {
    EXPECT_EQ(3, vertex.Labels(storage::View::NEW)->size());
    EXPECT_TRUE(*vertex.HasLabel(storage::View::NEW, label2));
    EXPECT_TRUE(*vertex.HasLabel(storage::View::NEW, label3));
  }
}

TEST_F(QueryPlanCRUDTest, RemoveProperty) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set
  auto prop1 = dba.NameToProperty("prop1");
  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
  auto v3 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}});
  auto v4 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(4)}});
  auto edge_type = dba.NameToEdgeType("edge_type");
  {
    auto e = dba.InsertEdge(&v1, &v3, edge_type);
    ASSERT_TRUE(e.HasValue());
    ASSERT_TRUE(e->SetProperty(prop1, storage::PropertyValue(42)).HasValue());
  }
  ASSERT_TRUE(dba.InsertEdge(&v2, &v4, edge_type).HasValue());
  ASSERT_TRUE(v2.SetProperty(prop1, storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(v3.SetProperty(prop1, storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(v4.SetProperty(prop1, storage::PropertyValue(42)).HasValue());
  auto prop2 = dba.NameToProperty("prop2");
  ASSERT_TRUE(v1.SetProperty(prop2, storage::PropertyValue(0)).HasValue());
  ASSERT_TRUE(v2.SetProperty(prop2, storage::PropertyValue(0)).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        storage::View::OLD);

  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop1);
  auto set_n_p = std::make_shared<plan::RemoveProperty>(r_m.op_, prop1, n_p);

  auto r_p = PROPERTY_LOOKUP(IDENT("r")->MapTo(r_m.edge_sym_), prop1);
  auto set_r_p = std::make_shared<plan::RemoveProperty>(set_n_p, prop1, r_p);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set_r_p, &context));
  dba.AdvanceCommand();

  EXPECT_EQ(CountEdges(&dba, storage::View::OLD), 2);
  for (auto vertex : dba.Vertices(storage::View::OLD)) {
    auto maybe_edges = vertex.OutEdges(storage::View::OLD);
    ASSERT_TRUE(maybe_edges.HasValue());
    for (auto edge : *maybe_edges) {
      EXPECT_EQ(edge.GetProperty(storage::View::OLD, prop1)->type(), storage::PropertyValue::Type::Null);
      auto from = edge.From();
      auto to = edge.To();
      EXPECT_EQ(from.GetProperty(storage::View::OLD, prop1)->type(), storage::PropertyValue::Type::Null);
      EXPECT_EQ(from.GetProperty(storage::View::OLD, prop2)->type(), storage::PropertyValue::Type::Int);
      EXPECT_EQ(to.GetProperty(storage::View::OLD, prop1)->type(), storage::PropertyValue::Type::Int);
    }
  }
}

TEST_F(QueryPlanCRUDTest, RemoveLabels) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  auto label1 = dba.NameToLabel("label1");
  auto label2 = dba.NameToLabel("label2");
  auto label3 = dba.NameToLabel("label3");
  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  ASSERT_TRUE(v1.AddLabel(label1).HasValue());
  ASSERT_TRUE(v1.AddLabel(label2).HasValue());
  ASSERT_TRUE(v1.AddLabel(label3).HasValue());
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
  ASSERT_TRUE(v2.AddLabel(label1).HasValue());
  ASSERT_TRUE(v2.AddLabel(label3).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_remove =
      std::make_shared<plan::RemoveLabels>(n.op_, n.sym_, std::vector<storage::LabelId>{label1, label2});
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*label_remove, &context));

  for (auto vertex : dba.Vertices(storage::View::OLD)) {
    EXPECT_EQ(1, vertex.Labels(storage::View::NEW)->size());
    EXPECT_FALSE(*vertex.HasLabel(storage::View::NEW, label1));
    EXPECT_FALSE(*vertex.HasLabel(storage::View::NEW, label2));
  }
}

TEST_F(QueryPlanCRUDTest, NodeFilterSet) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Create a graph such that (v1 {prop: 42}) is connected to v2 and v3.
  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto prop = PROPERTY_PAIR("prop");
  ASSERT_TRUE(v1.SetProperty(prop.second, storage::PropertyValue(42)).HasValue());
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
  auto v3 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}});
  auto edge_type = dba.NameToEdgeType("Edge");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  dba.AdvanceCommand();
  // Create operations which match (v1 {prop: 42}) -- (v) and increment the
  // v1.prop. The expected result is two incremenentations, since v1 is matched
  // twice for 2 edges it has.
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n {prop: 42}) -[r]- (m)
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  std::get<0>(scan_all.node_->properties_)[storage.GetPropertyIx(prop.first)] = LITERAL(42);
  auto expand = MakeExpand(storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::BOTH, {}, "m",
                           false, storage::View::OLD);
  auto *filter_expr =
      EQ(storage.Create<PropertyLookup>(scan_all.node_->identifier_, storage.GetPropertyIx(prop.first)), LITERAL(42));
  auto node_filter = std::make_shared<Filter>(expand.op_, filter_expr);
  // SET n.prop = n.prop + 1
  auto set_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), prop);
  auto add = ADD(set_prop, LITERAL(1));
  auto set = std::make_shared<plan::SetProperty>(node_filter, prop.second, set_prop, add);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set, &context));
  dba.AdvanceCommand();
  auto prop_eq = TypedValue(*v1.GetProperty(storage::View::OLD, prop.second)) == TypedValue(42 + 2);
  ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
  EXPECT_TRUE(prop_eq.ValueBool());
}

TEST_F(QueryPlanCRUDTest, FilterRemove) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Create a graph such that (v1 {prop: 42}) is connected to v2 and v3.
  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto prop = PROPERTY_PAIR("prop");
  ASSERT_TRUE(v1.SetProperty(prop.second, storage::PropertyValue(42)).HasValue());
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});
  auto v3 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}});
  auto edge_type = dba.NameToEdgeType("Edge");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  dba.AdvanceCommand();
  // Create operations which match (v1 {prop: 42}) -- (v) and remove v1.prop.
  // The expected result is two matches, for each edge of v1.
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) -[r]- (m) WHERE n.prop < 43
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  std::get<0>(scan_all.node_->properties_)[storage.GetPropertyIx(prop.first)] = LITERAL(42);
  auto expand = MakeExpand(storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::BOTH, {}, "m",
                           false, storage::View::OLD);
  auto filter_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), prop);
  auto filter = std::make_shared<Filter>(expand.op_, LESS(filter_prop, LITERAL(43)));
  // REMOVE n.prop
  auto rem_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), prop);
  auto rem = std::make_shared<plan::RemoveProperty>(filter, prop.second, rem_prop);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*rem, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(v1.GetProperty(storage::View::OLD, prop.second)->type(), storage::PropertyValue::Type::Null);
}

TEST_F(QueryPlanCRUDTest, SetRemove) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  auto v = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto label1 = dba.NameToLabel("label1");
  auto label2 = dba.NameToLabel("label2");
  dba.AdvanceCommand();
  // Create operations which match (v) and set and remove v :label.
  // The expected result is single (v) as it was at the start.
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) SET n :label1 :label2 REMOVE n :label1 :label2
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto set =
      std::make_shared<plan::SetLabels>(scan_all.op_, scan_all.sym_, std::vector<storage::LabelId>{label1, label2});
  auto rem = std::make_shared<plan::RemoveLabels>(set, scan_all.sym_, std::vector<storage::LabelId>{label1, label2});
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*rem, &context));
  dba.AdvanceCommand();
  EXPECT_FALSE(*v.HasLabel(storage::View::OLD, label1));
  EXPECT_FALSE(*v.HasLabel(storage::View::OLD, label2));
}

TEST_F(QueryPlanCRUDTest, Merge) {
  // test setup:
  //  - three nodes, two of them connected with T
  //  - merge input branch matches all nodes
  //  - merge_match branch looks for an expansion (any direction)
  //    and sets some property (for result validation)
  //  - merge_create branch just sets some other property
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  auto v1 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
  auto v2 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(2)}});

  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("Type")).HasValue());
  auto v3 = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(3)}});
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto prop = PROPERTY_PAIR("prop");
  auto n = MakeScanAll(storage, symbol_table, "n");

  // merge_match branch
  auto r_m = MakeExpand(storage, symbol_table, std::make_shared<Once>(), n.sym_, "r", EdgeAtom::Direction::BOTH, {},
                        "m", false, storage::View::OLD);
  auto m_p = PROPERTY_LOOKUP(IDENT("m")->MapTo(r_m.node_sym_), prop);
  auto m_set = std::make_shared<plan::SetProperty>(r_m.op_, prop.second, m_p, LITERAL(1));

  // merge_create branch
  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto n_set = std::make_shared<plan::SetProperty>(std::make_shared<Once>(), prop.second, n_p, LITERAL(2));

  auto merge = std::make_shared<plan::Merge>(n.op_, m_set, n_set);
  auto context = MakeContext(storage, symbol_table, &dba);
  ASSERT_EQ(3, PullAll(*merge, &context));
  dba.AdvanceCommand();

  ASSERT_EQ(v1.GetProperty(storage::View::OLD, prop.second)->type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(v1.GetProperty(storage::View::OLD, prop.second)->ValueInt(), 1);
  ASSERT_EQ(v2.GetProperty(storage::View::OLD, prop.second)->type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(v2.GetProperty(storage::View::OLD, prop.second)->ValueInt(), 1);
  ASSERT_EQ(v3.GetProperty(storage::View::OLD, prop.second)->type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(v3.GetProperty(storage::View::OLD, prop.second)->ValueInt(), 2);
}

TEST_F(QueryPlanCRUDTest, MergeNoInput) {
  // merge with no input, creates a single node

  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels = {label};
  std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, LITERAL(1));
  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto merge = std::make_shared<plan::Merge>(nullptr, create, create);

  EXPECT_EQ(0, CountIterable(dba.Vertices(storage::View::OLD)));
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*merge, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
}

TEST(QueryPlan, SetPropertyOnNull) {
  // SET (Null).prop = 42
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR("property");
  auto null = LITERAL(TypedValue());
  auto literal = LITERAL(42);
  auto n_prop = PROPERTY_LOOKUP(null, prop);
  auto once = std::make_shared<Once>();
  auto set_op = std::make_shared<plan::SetProperty>(once, prop.second, n_prop, literal);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TEST(QueryPlan, SetPropertiesOnNull) {
  // OPTIONAL MATCH (n) SET n = n
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_ident = IDENT("n")->MapTo(n.sym_);
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto set_op = std::make_shared<plan::SetProperties>(optional, n.sym_, n_ident, plan::SetProperties::Op::REPLACE);
  EXPECT_EQ(0, CountIterable(dba.Vertices(storage::View::OLD)));
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TEST(QueryPlan, SetLabelsOnNull) {
  // OPTIONAL MATCH (n) SET n :label
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  auto label = dba.NameToLabel("label");
  AstStorage storage;
  SymbolTable symbol_table;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto set_op = std::make_shared<plan::SetLabels>(optional, n.sym_, std::vector<storage::LabelId>{label});
  EXPECT_EQ(0, CountIterable(dba.Vertices(storage::View::OLD)));
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TEST(QueryPlan, RemovePropertyOnNull) {
  // REMOVE (Null).prop
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR("property");
  auto null = LITERAL(TypedValue());
  auto n_prop = PROPERTY_LOOKUP(null, prop);
  auto once = std::make_shared<Once>();
  auto remove_op = std::make_shared<plan::RemoveProperty>(once, prop.second, n_prop);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*remove_op, &context));
}

TEST(QueryPlan, RemoveLabelsOnNull) {
  // OPTIONAL MATCH (n) REMOVE n :label
  storage::Storage db;
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  auto label = dba.NameToLabel("label");
  AstStorage storage;
  SymbolTable symbol_table;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto remove_op = std::make_shared<plan::RemoveLabels>(optional, n.sym_, std::vector<storage::LabelId>{label});
  EXPECT_EQ(0, CountIterable(dba.Vertices(storage::View::OLD)));
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*remove_op, &context));
}

TEST_F(QueryPlanCRUDTest, DeleteSetProperty) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Add a single vertex.
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n SET n.prop = 42
  auto n = MakeScanAllNew(storage, symbol_table, "n");

  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR("prop");
  auto n_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto set_op = std::make_shared<plan::SetProperty>(delete_op, prop.second, n_prop, LITERAL(42));
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*set_op, &context), QueryRuntimeException);
}

TEST_F(QueryPlanCRUDTest, DeleteSetPropertiesFromMap) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Add a single vertex.
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n SET n = {prop: 42}
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR("prop");
  std::unordered_map<PropertyIx, Expression *> prop_map;
  prop_map.emplace(storage.GetPropertyIx(prop.first), LITERAL(42));
  auto *rhs = storage.Create<MapLiteral>(prop_map);
  for (auto op_type : {plan::SetProperties::Op::REPLACE, plan::SetProperties::Op::UPDATE}) {
    auto set_op = std::make_shared<plan::SetProperties>(delete_op, n.sym_, rhs, op_type);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*set_op, &context), QueryRuntimeException);
  }
}

TEST_F(QueryPlanCRUDTest, DeleteSetPropertiesFrom) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Add a single vertex.
  {
    auto v = *dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}});
    ASSERT_TRUE(v.SetProperty(dba.NameToProperty("prop"), storage::PropertyValue(1)).HasValue());
  }
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n SET n = n
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto *rhs = IDENT("n")->MapTo(n.sym_);
  for (auto op_type : {plan::SetProperties::Op::REPLACE, plan::SetProperties::Op::UPDATE}) {
    auto set_op = std::make_shared<plan::SetProperties>(delete_op, n.sym_, rhs, op_type);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*set_op, &context), QueryRuntimeException);
  }
}

TEST_F(QueryPlanCRUDTest, DeleteRemoveLabels) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Add a single vertex.
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n REMOVE n :label
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  std::vector<storage::LabelId> labels{dba.NameToLabel("label1")};
  auto rem_op = std::make_shared<plan::RemoveLabels>(delete_op, n.sym_, labels);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*rem_op, &context), QueryRuntimeException);
}

TEST_F(QueryPlanCRUDTest, DeleteRemoveProperty) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  // Add a single vertex.
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::PropertyValue(1)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(storage::View::OLD)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n REMOVE n.prop
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR("prop");
  auto n_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto rem_op = std::make_shared<plan::RemoveProperty>(delete_op, prop.second, n_prop);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*rem_op, &context), QueryRuntimeException);
}
}  // namespace memgraph::query::tests
