#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"

#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;

TEST(QueryPlan, CreateNodeWithAttributes) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  storage::Label label = dba.Label("Person");
  auto property = PROPERTY_PAIR("prop");

  AstStorage storage;
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  node.properties.emplace_back(property.second, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto context = MakeContext(storage, symbol_table, &dba);
  PullAll(*create, &context);
  dba.AdvanceCommand();

  // count the number of vertices
  int vertex_count = 0;
  for (VertexAccessor vertex : dba.Vertices(false)) {
    vertex_count++;
    EXPECT_EQ(vertex.labels().size(), 1);
    EXPECT_EQ(*vertex.labels().begin(), label);
    EXPECT_EQ(vertex.Properties().size(), 1);
    auto prop_eq = vertex.PropsAt(property.second) == TypedValue(42);
    ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
    EXPECT_TRUE(prop_eq.Value<bool>());
  }
  EXPECT_EQ(vertex_count, 1);
}

TEST(QueryPlan, CreateReturn) {
  // test CREATE (n:Person {age: 42}) RETURN n, n.age
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  storage::Label label = dba.Label("Person");
  auto property = PROPERTY_PAIR("property");

  AstStorage storage;
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  node.properties.emplace_back(property.second, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto named_expr_n =
      NEXPR("n", IDENT("n")->MapTo(node.symbol))
          ->MapTo(symbol_table.CreateSymbol("named_expr_n", true));
  auto prop_lookup = PROPERTY_LOOKUP(IDENT("n")->MapTo(node.symbol), property);
  auto named_expr_n_p =
      NEXPR("n", prop_lookup)
          ->MapTo(symbol_table.CreateSymbol("named_expr_n_p", true));

  auto produce = MakeProduce(create, named_expr_n, named_expr_n_p);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(2, results[0].size());
  EXPECT_EQ(TypedValue::Type::Vertex, results[0][0].type());
  EXPECT_EQ(1, results[0][0].Value<VertexAccessor>().labels().size());
  EXPECT_EQ(label, results[0][0].Value<VertexAccessor>().labels()[0]);
  EXPECT_EQ(TypedValue::Type::Int, results[0][1].type());
  EXPECT_EQ(42, results[0][1].Value<int64_t>());

  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(false)));
}

TEST(QueryPlan, CreateExpand) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  storage::Label label_node_1 = dba.Label("Node1");
  storage::Label label_node_2 = dba.Label("Node2");
  auto property = PROPERTY_PAIR("property");
  storage::EdgeType edge_type = dba.EdgeType("edge_type");

  SymbolTable symbol_table;
  AstStorage storage;

  auto test_create_path = [&](bool cycle, int expected_nodes_created,
                              int expected_edges_created) {
    int before_v = CountIterable(dba.Vertices(false));
    int before_e = CountIterable(dba.Edges(false));

    // data for the first node
    NodeCreationInfo n;
    n.symbol = symbol_table.CreateSymbol("n", true);
    n.labels.emplace_back(label_node_1);
    n.properties.emplace_back(property.second, LITERAL(1));

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n.symbol : symbol_table.CreateSymbol("m", true);
    m.labels.emplace_back(label_node_2);
    m.properties.emplace_back(property.second, LITERAL(2));

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.edge_type = edge_type;
    r.properties.emplace_back(property.second, LITERAL(3));

    auto create_op = std::make_shared<CreateNode>(nullptr, n);
    auto create_expand =
        std::make_shared<CreateExpand>(m, r, create_op, n.symbol, cycle);
    auto context = MakeContext(storage, symbol_table, &dba);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();

    EXPECT_EQ(CountIterable(dba.Vertices(false)) - before_v,
              expected_nodes_created);
    EXPECT_EQ(CountIterable(dba.Edges(false)) - before_e,
              expected_edges_created);
  };

  test_create_path(false, 2, 1);
  test_create_path(true, 1, 1);

  for (VertexAccessor vertex : dba.Vertices(false)) {
    EXPECT_EQ(vertex.labels().size(), 1);
    storage::Label label = vertex.labels()[0];
    if (label == label_node_1) {
      // node created by first op
      EXPECT_EQ(vertex.PropsAt(property.second).Value<int64_t>(), 1);
    } else if (label == label_node_2) {
      // node create by expansion
      EXPECT_EQ(vertex.PropsAt(property.second).Value<int64_t>(), 2);
    } else {
      // should not happen
      FAIL();
    }

    for (EdgeAccessor edge : dba.Edges(false)) {
      EXPECT_EQ(edge.EdgeType(), edge_type);
      EXPECT_EQ(edge.PropsAt(property.second).Value<int64_t>(), 3);
    }
  }
}

TEST(QueryPlan, MatchCreateNode) {
  database::GraphDb db;
  auto dba = db.Access();

  // add three nodes we'll match and expand-create from
  dba->InsertVertex();
  dba->InsertVertex();
  dba->InsertVertex();
  dba->AdvanceCommand();

  SymbolTable symbol_table;
  AstStorage storage;

  // first node
  auto n_scan_all = MakeScanAll(storage, symbol_table, "n");
  // second node
  NodeCreationInfo m;
  m.symbol = symbol_table.CreateSymbol("m", true);
  // creation op
  auto create_node = std::make_shared<CreateNode>(n_scan_all.op_, m);

  EXPECT_EQ(CountIterable(dba->Vertices(false)), 3);
  auto context = MakeContext(storage, symbol_table, dba.get());
  PullAll(*create_node, &context);
  dba->AdvanceCommand();
  EXPECT_EQ(CountIterable(dba->Vertices(false)), 6);
}

TEST(QueryPlan, MatchCreateExpand) {
  database::GraphDb db;
  auto dba = db.Access();

  // add three nodes we'll match and expand-create from
  dba->InsertVertex();
  dba->InsertVertex();
  dba->InsertVertex();
  dba->AdvanceCommand();

  //  storage::Label label_node_1 = dba->Label("Node1");
  //  storage::Label label_node_2 = dba->Label("Node2");
  //  storage::Property property = dba->Label("prop");
  storage::EdgeType edge_type = dba->EdgeType("edge_type");

  SymbolTable symbol_table;
  AstStorage storage;

  auto test_create_path = [&](bool cycle, int expected_nodes_created,
                              int expected_edges_created) {
    int before_v = CountIterable(dba->Vertices(false));
    int before_e = CountIterable(dba->Edges(false));

    // data for the first node
    auto n_scan_all = MakeScanAll(storage, symbol_table, "n");

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n_scan_all.sym_ : symbol_table.CreateSymbol("m", true);

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.direction = EdgeAtom::Direction::OUT;
    r.edge_type = edge_type;

    auto create_expand = std::make_shared<CreateExpand>(m, r, n_scan_all.op_,
                                                        n_scan_all.sym_, cycle);
    auto context = MakeContext(storage, symbol_table, dba.get());
    PullAll(*create_expand, &context);
    dba->AdvanceCommand();

    EXPECT_EQ(CountIterable(dba->Vertices(false)) - before_v,
              expected_nodes_created);
    EXPECT_EQ(CountIterable(dba->Edges(false)) - before_e,
              expected_edges_created);
  };

  test_create_path(false, 3, 3);
  test_create_path(true, 0, 6);
}

TEST(QueryPlan, Delete) {
  database::GraphDb db;
  auto dba = db.Access();

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 4; ++i) vertices.push_back(dba->InsertVertex());
  auto type = dba->EdgeType("type");
  for (int j = 0; j < 4; ++j)
    for (int k = j + 1; k < 4; ++k)
      dba->InsertEdge(vertices[j], vertices[k], type);

  dba->AdvanceCommand();
  EXPECT_EQ(4, CountIterable(dba->Vertices(false)));
  EXPECT_EQ(6, CountIterable(dba->Edges(false)));

  AstStorage storage;
  SymbolTable symbol_table;

  // attempt to delete a vertex, and fail
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(
        n.op_, std::vector<Expression *>{n_get}, false);
    auto context = MakeContext(storage, symbol_table, dba.get());
    EXPECT_THROW(PullAll(*delete_op, &context), QueryRuntimeException);
    dba->AdvanceCommand();
    EXPECT_EQ(4, CountIterable(dba->Vertices(false)));
    EXPECT_EQ(6, CountIterable(dba->Edges(false)));
  }

  // detach delete a single vertex
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(
        n.op_, std::vector<Expression *>{n_get}, true);
    Frame frame(symbol_table.max_position());
    auto context = MakeContext(storage, symbol_table, dba.get());
    delete_op->MakeCursor(*dba)->Pull(frame, context);
    dba->AdvanceCommand();
    EXPECT_EQ(3, CountIterable(dba->Vertices(false)));
    EXPECT_EQ(3, CountIterable(dba->Edges(false)));
  }

  // delete all remaining edges
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m =
        MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                   EdgeAtom::Direction::OUT, {}, "m", false, GraphView::NEW);
    auto r_get = storage.Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto delete_op = std::make_shared<plan::Delete>(
        r_m.op_, std::vector<Expression *>{r_get}, false);
    auto context = MakeContext(storage, symbol_table, dba.get());
    PullAll(*delete_op, &context);
    dba->AdvanceCommand();
    EXPECT_EQ(3, CountIterable(dba->Vertices(false)));
    EXPECT_EQ(0, CountIterable(dba->Edges(false)));
  }

  // delete all remaining vertices
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(
        n.op_, std::vector<Expression *>{n_get}, false);
    auto context = MakeContext(storage, symbol_table, dba.get());
    PullAll(*delete_op, &context);
    dba->AdvanceCommand();
    EXPECT_EQ(0, CountIterable(dba->Vertices(false)));
    EXPECT_EQ(0, CountIterable(dba->Edges(false)));
  }
}

TEST(QueryPlan, DeleteTwiceDeleteBlockingEdge) {
  // test deleting the same vertex and edge multiple times
  //
  // also test vertex deletion succeeds if the prohibiting
  // edge is deleted in the same logical op
  //
  // we test both with the following queries (note the
  // undirected edge in MATCH):
  //
  // CREATE ()-[:T]->()
  // MATCH (n)-[r]-(m) [DETACH] DELETE n, r, m

  auto test_delete = [](bool detach) {
    database::GraphDb db;
    auto dba = db.Access();

    auto v1 = dba->InsertVertex();
    auto v2 = dba->InsertVertex();
    dba->InsertEdge(v1, v2, dba->EdgeType("T"));
    dba->AdvanceCommand();
    EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
    EXPECT_EQ(1, CountIterable(dba->Edges(false)));

    AstStorage storage;
    SymbolTable symbol_table;

    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m =
        MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                   EdgeAtom::Direction::BOTH, {}, "m", false, GraphView::OLD);

    // getter expressions for deletion
    auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
    auto r_get = storage.Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto m_get = storage.Create<Identifier>("m")->MapTo(r_m.node_sym_);

    auto delete_op = std::make_shared<plan::Delete>(
        r_m.op_, std::vector<Expression *>{n_get, r_get, m_get}, detach);
    auto context = MakeContext(storage, symbol_table, dba.get());
    EXPECT_EQ(2, PullAll(*delete_op, &context));
    dba->AdvanceCommand();
    EXPECT_EQ(0, CountIterable(dba->Vertices(false)));
    EXPECT_EQ(0, CountIterable(dba->Edges(false)));
  };

  test_delete(true);
  test_delete(false);
}

TEST(QueryPlan, DeleteReturn) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  auto prop = PROPERTY_PAIR("property");
  for (int i = 0; i < 4; ++i) {
    auto va = dba.InsertVertex();
    va.PropsSet(prop.second, 42);
  }

  dba.AdvanceCommand();
  EXPECT_EQ(4, CountIterable(dba.Vertices(false)));
  EXPECT_EQ(0, CountIterable(dba.Edges(false)));

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");

  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, true);

  auto prop_lookup = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto n_p = storage.Create<NamedExpression>("n", prop_lookup)
                 ->MapTo(symbol_table.CreateSymbol("bla", true));
  auto produce = MakeProduce(delete_op, n_p);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(4, results.size());
  dba.AdvanceCommand();
  EXPECT_EQ(0, CountIterable(dba.Vertices(false)));
}

TEST(QueryPlan, DeleteNull) {
  // test (simplified) WITH Null as x delete x
  database::GraphDb db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;

  auto once = std::make_shared<Once>();
  auto delete_op = std::make_shared<plan::Delete>(
      once, std::vector<Expression *>{LITERAL(TypedValue::Null)}, false);
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(1, PullAll(*delete_op, &context));
}

TEST(QueryPlan, DeleteAdvance) {
  // test queries on empty DB:
  // CREATE (n)
  // MATCH (n) DELETE n WITH n ...
  // this fails due to us advancing the command
  // when processing the WITH clause
  //
  // note that Neo does not fail when the deleted
  // record is not used in subsequent clauses, but
  // we are not yet compatible with that
  database::GraphDb db;
  auto dba = db.Access();
  dba->InsertVertex();
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, false);
  auto advance = std::make_shared<Accumulate>(
      delete_op, std::vector<Symbol>{n.sym_}, true);
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_THROW(PullAll(*advance, &context), ReconstructionException);
}

TEST(QueryPlan, SetProperty) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto v4 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("edge_type");
  dba.InsertEdge(v1, v3, edge_type);
  dba.InsertEdge(v2, v4, edge_type);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m =
      MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);

  // set prop1 to 42 on n and r
  auto prop1 = dba.Property("prop1");
  auto literal = LITERAL(42);

  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop1);
  auto set_n_p =
      std::make_shared<plan::SetProperty>(r_m.op_, prop1, n_p, literal);

  auto r_p = PROPERTY_LOOKUP(IDENT("r")->MapTo(r_m.edge_sym_), prop1);
  auto set_r_p =
      std::make_shared<plan::SetProperty>(set_n_p, prop1, r_p, literal);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set_r_p, &context));
  dba.AdvanceCommand();

  EXPECT_EQ(CountIterable(dba.Edges(false)), 2);
  for (EdgeAccessor edge : dba.Edges(false)) {
    ASSERT_EQ(edge.PropsAt(prop1).type(), PropertyValue::Type::Int);
    EXPECT_EQ(edge.PropsAt(prop1).Value<int64_t>(), 42);
    VertexAccessor from = edge.from();
    VertexAccessor to = edge.to();
    ASSERT_EQ(from.PropsAt(prop1).type(), PropertyValue::Type::Int);
    EXPECT_EQ(from.PropsAt(prop1).Value<int64_t>(), 42);
    ASSERT_EQ(to.PropsAt(prop1).type(), PropertyValue::Type::Null);
  }
}

TEST(QueryPlan, SetProperties) {
  auto test_set_properties = [](bool update) {
    database::GraphDb db;
    auto dba = db.Access();

    // graph: ({a: 0})-[:R {b:1}]->({c:2})
    auto prop_a = dba->Property("a");
    auto prop_b = dba->Property("b");
    auto prop_c = dba->Property("c");
    auto v1 = dba->InsertVertex();
    auto v2 = dba->InsertVertex();
    auto e = dba->InsertEdge(v1, v2, dba->EdgeType("R"));
    v1.PropsSet(prop_a, 0);
    e.PropsSet(prop_b, 1);
    v2.PropsSet(prop_c, 2);
    dba->AdvanceCommand();

    AstStorage storage;
    SymbolTable symbol_table;

    // scan (n)-[r]->(m)
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m =
        MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                   EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);

    auto op = update ? plan::SetProperties::Op::UPDATE
                     : plan::SetProperties::Op::REPLACE;

    // set properties on r to n, and on r to m
    auto r_ident = IDENT("r")->MapTo(r_m.edge_sym_);
    auto m_ident = IDENT("m")->MapTo(r_m.node_sym_);
    auto set_r_to_n =
        std::make_shared<plan::SetProperties>(r_m.op_, n.sym_, r_ident, op);
    auto set_m_to_r = std::make_shared<plan::SetProperties>(
        set_r_to_n, r_m.edge_sym_, m_ident, op);
    auto context = MakeContext(storage, symbol_table, dba.get());
    EXPECT_EQ(1, PullAll(*set_m_to_r, &context));
    dba->AdvanceCommand();

    EXPECT_EQ(CountIterable(dba->Edges(false)), 1);
    for (EdgeAccessor edge : dba->Edges(false)) {
      VertexAccessor from = edge.from();
      EXPECT_EQ(from.Properties().size(), update ? 2 : 1);
      if (update) {
        ASSERT_EQ(from.PropsAt(prop_a).type(), PropertyValue::Type::Int);
        EXPECT_EQ(from.PropsAt(prop_a).Value<int64_t>(), 0);
      }
      ASSERT_EQ(from.PropsAt(prop_b).type(), PropertyValue::Type::Int);
      EXPECT_EQ(from.PropsAt(prop_b).Value<int64_t>(), 1);

      EXPECT_EQ(edge.Properties().size(), update ? 2 : 1);
      if (update) {
        ASSERT_EQ(edge.PropsAt(prop_b).type(), PropertyValue::Type::Int);
        EXPECT_EQ(edge.PropsAt(prop_b).Value<int64_t>(), 1);
      }
      ASSERT_EQ(edge.PropsAt(prop_c).type(), PropertyValue::Type::Int);
      EXPECT_EQ(edge.PropsAt(prop_c).Value<int64_t>(), 2);

      VertexAccessor to = edge.to();
      EXPECT_EQ(to.Properties().size(), 1);
      ASSERT_EQ(to.PropsAt(prop_c).type(), PropertyValue::Type::Int);
      EXPECT_EQ(to.PropsAt(prop_c).Value<int64_t>(), 2);
    }
  };

  test_set_properties(true);
  test_set_properties(false);
}

TEST(QueryPlan, SetLabels) {
  database::GraphDb db;
  auto dba = db.Access();

  auto label1 = dba->Label("label1");
  auto label2 = dba->Label("label2");
  auto label3 = dba->Label("label3");
  dba->InsertVertex().add_label(label1);
  dba->InsertVertex().add_label(label1);
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_set = std::make_shared<plan::SetLabels>(
      n.op_, n.sym_, std::vector<storage::Label>{label2, label3});
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(2, PullAll(*label_set, &context));

  for (VertexAccessor vertex : dba->Vertices(false)) {
    vertex.SwitchNew();
    EXPECT_EQ(3, vertex.labels().size());
    EXPECT_TRUE(vertex.has_label(label2));
    EXPECT_TRUE(vertex.has_label(label3));
  }
}

TEST(QueryPlan, RemoveProperty) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set
  auto prop1 = dba.Property("prop1");
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto v4 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("edge_type");
  dba.InsertEdge(v1, v3, edge_type).PropsSet(prop1, 42);
  dba.InsertEdge(v2, v4, edge_type);
  v2.PropsSet(prop1, 42);
  v3.PropsSet(prop1, 42);
  v4.PropsSet(prop1, 42);
  auto prop2 = dba.Property("prop2");
  v1.PropsSet(prop2, 0);
  v2.PropsSet(prop2, 0);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m =
      MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);

  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop1);
  auto set_n_p = std::make_shared<plan::RemoveProperty>(r_m.op_, prop1, n_p);

  auto r_p = PROPERTY_LOOKUP(IDENT("r")->MapTo(r_m.edge_sym_), prop1);
  auto set_r_p = std::make_shared<plan::RemoveProperty>(set_n_p, prop1, r_p);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set_r_p, &context));
  dba.AdvanceCommand();

  EXPECT_EQ(CountIterable(dba.Edges(false)), 2);
  for (EdgeAccessor edge : dba.Edges(false)) {
    EXPECT_EQ(edge.PropsAt(prop1).type(), PropertyValue::Type::Null);
    VertexAccessor from = edge.from();
    VertexAccessor to = edge.to();
    EXPECT_EQ(from.PropsAt(prop1).type(), PropertyValue::Type::Null);
    EXPECT_EQ(from.PropsAt(prop2).type(), PropertyValue::Type::Int);
    EXPECT_EQ(to.PropsAt(prop1).type(), PropertyValue::Type::Int);
  }
}

TEST(QueryPlan, RemoveLabels) {
  database::GraphDb db;
  auto dba = db.Access();

  auto label1 = dba->Label("label1");
  auto label2 = dba->Label("label2");
  auto label3 = dba->Label("label3");
  auto v1 = dba->InsertVertex();
  v1.add_label(label1);
  v1.add_label(label2);
  v1.add_label(label3);
  auto v2 = dba->InsertVertex();
  v2.add_label(label1);
  v2.add_label(label3);
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_remove = std::make_shared<plan::RemoveLabels>(
      n.op_, n.sym_, std::vector<storage::Label>{label1, label2});
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(2, PullAll(*label_remove, &context));

  for (VertexAccessor vertex : dba->Vertices(false)) {
    vertex.SwitchNew();
    EXPECT_EQ(1, vertex.labels().size());
    EXPECT_FALSE(vertex.has_label(label1));
    EXPECT_FALSE(vertex.has_label(label2));
  }
}

TEST(QueryPlan, NodeFilterSet) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  // Create a graph such that (v1 {prop: 42}) is connected to v2 and v3.
  auto v1 = dba.InsertVertex();
  auto prop = PROPERTY_PAIR("property");
  v1.PropsSet(prop.second, 42);
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("Edge");
  dba.InsertEdge(v1, v2, edge_type);
  dba.InsertEdge(v1, v3, edge_type);
  dba.AdvanceCommand();
  // Create operations which match (v1 {prop: 42}) -- (v) and increment the
  // v1.prop. The expected result is two incremenentations, since v1 is matched
  // twice for 2 edges it has.
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n {prop: 42}) -[r]- (m)
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  scan_all.node_->properties_[storage.GetPropertyIx(prop.first)] = LITERAL(42);
  auto expand =
      MakeExpand(storage, symbol_table, scan_all.op_, scan_all.sym_, "r",
                 EdgeAtom::Direction::BOTH, {}, "m", false, GraphView::OLD);
  auto *filter_expr =
      EQ(storage.Create<PropertyLookup>(scan_all.node_->identifier_,
                                        storage.GetPropertyIx(prop.first)),
         LITERAL(42));
  auto node_filter = std::make_shared<Filter>(expand.op_, filter_expr);
  // SET n.prop = n.prop + 1
  auto set_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), prop);
  auto add = ADD(set_prop, LITERAL(1));
  auto set = std::make_shared<plan::SetProperty>(node_filter, prop.second,
                                                 set_prop, add);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set, &context));
  dba.AdvanceCommand();
  v1.Reconstruct();
  auto prop_eq = v1.PropsAt(prop.second) == TypedValue(42 + 2);
  ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
  EXPECT_TRUE(prop_eq.Value<bool>());
}

TEST(QueryPlan, FilterRemove) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  // Create a graph such that (v1 {prop: 42}) is connected to v2 and v3.
  auto v1 = dba.InsertVertex();
  auto prop = PROPERTY_PAIR("property");
  v1.PropsSet(prop.second, 42);
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("Edge");
  dba.InsertEdge(v1, v2, edge_type);
  dba.InsertEdge(v1, v3, edge_type);
  dba.AdvanceCommand();
  // Create operations which match (v1 {prop: 42}) -- (v) and remove v1.prop.
  // The expected result is two matches, for each edge of v1.
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) -[r]- (m) WHERE n.prop < 43
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  scan_all.node_->properties_[storage.GetPropertyIx(prop.first)] = LITERAL(42);
  auto expand =
      MakeExpand(storage, symbol_table, scan_all.op_, scan_all.sym_, "r",
                 EdgeAtom::Direction::BOTH, {}, "m", false, GraphView::OLD);
  auto filter_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), prop);
  auto filter =
      std::make_shared<Filter>(expand.op_, LESS(filter_prop, LITERAL(43)));
  // REMOVE n.prop
  auto rem_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), prop);
  auto rem =
      std::make_shared<plan::RemoveProperty>(filter, prop.second, rem_prop);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*rem, &context));
  dba.AdvanceCommand();
  v1.Reconstruct();
  EXPECT_EQ(v1.PropsAt(prop.second).type(), PropertyValue::Type::Null);
}

TEST(QueryPlan, SetRemove) {
  database::GraphDb db;
  auto dba = db.Access();
  auto v = dba->InsertVertex();
  auto label1 = dba->Label("label1");
  auto label2 = dba->Label("label2");
  dba->AdvanceCommand();
  // Create operations which match (v) and set and remove v :label.
  // The expected result is single (v) as it was at the start.
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) SET n :label1 :label2 REMOVE n :label1 :label2
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto set = std::make_shared<plan::SetLabels>(
      scan_all.op_, scan_all.sym_, std::vector<storage::Label>{label1, label2});
  auto rem = std::make_shared<plan::RemoveLabels>(
      set, scan_all.sym_, std::vector<storage::Label>{label1, label2});
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(1, PullAll(*rem, &context));
  dba->AdvanceCommand();
  v.Reconstruct();
  EXPECT_FALSE(v.has_label(label1));
  EXPECT_FALSE(v.has_label(label2));
}

TEST(QueryPlan, Merge) {
  // test setup:
  //  - three nodes, two of them connected with T
  //  - merge input branch matches all nodes
  //  - merge_match branch looks for an expansion (any direction)
  //    and sets some property (for result validation)
  //  - merge_create branch just sets some other property
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  dba.InsertEdge(v1, v2, dba.EdgeType("Type"));
  auto v3 = dba.InsertVertex();
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto prop = PROPERTY_PAIR("property");
  auto n = MakeScanAll(storage, symbol_table, "n");

  // merge_match branch
  auto r_m =
      MakeExpand(storage, symbol_table, std::make_shared<Once>(), n.sym_, "r",
                 EdgeAtom::Direction::BOTH, {}, "m", false, GraphView::OLD);
  auto m_p = PROPERTY_LOOKUP(IDENT("m")->MapTo(r_m.node_sym_), prop);
  auto m_set = std::make_shared<plan::SetProperty>(r_m.op_, prop.second, m_p,
                                                   LITERAL(1));

  // merge_create branch
  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto n_set = std::make_shared<plan::SetProperty>(
      std::make_shared<Once>(), prop.second, n_p, LITERAL(2));

  auto merge = std::make_shared<plan::Merge>(n.op_, m_set, n_set);
  auto context = MakeContext(storage, symbol_table, &dba);
  ASSERT_EQ(3, PullAll(*merge, &context));
  dba.AdvanceCommand();
  v1.Reconstruct();
  v2.Reconstruct();
  v3.Reconstruct();

  ASSERT_EQ(v1.PropsAt(prop.second).type(), PropertyValue::Type::Int);
  ASSERT_EQ(v1.PropsAt(prop.second).Value<int64_t>(), 1);
  ASSERT_EQ(v2.PropsAt(prop.second).type(), PropertyValue::Type::Int);
  ASSERT_EQ(v2.PropsAt(prop.second).Value<int64_t>(), 1);
  ASSERT_EQ(v3.PropsAt(prop.second).type(), PropertyValue::Type::Int);
  ASSERT_EQ(v3.PropsAt(prop.second).Value<int64_t>(), 2);
}

TEST(QueryPlan, MergeNoInput) {
  // merge with no input, creates a single node

  database::GraphDb db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto merge = std::make_shared<plan::Merge>(nullptr, create, create);

  EXPECT_EQ(0, CountIterable(dba->Vertices(false)));
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(1, PullAll(*merge, &context));
  dba->AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba->Vertices(false)));
}

TEST(QueryPlan, SetPropertyOnNull) {
  // SET (Null).prop = 42
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  AstStorage storage;
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR("property");
  auto null = LITERAL(TypedValue::Null);
  auto literal = LITERAL(42);
  auto n_prop = PROPERTY_LOOKUP(null, prop);
  auto once = std::make_shared<Once>();
  auto set_op =
      std::make_shared<plan::SetProperty>(once, prop.second, n_prop, literal);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TEST(QueryPlan, SetPropertiesOnNull) {
  // OPTIONAL MATCH (n) SET n = n
  database::GraphDb db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_ident = IDENT("n")->MapTo(n.sym_);
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  auto set_op = std::make_shared<plan::SetProperties>(
      optional, n.sym_, n_ident, plan::SetProperties::Op::REPLACE);
  EXPECT_EQ(0, CountIterable(dba->Vertices(false)));
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TEST(QueryPlan, SetLabelsOnNull) {
  // OPTIONAL MATCH (n) SET n :label
  database::GraphDb db;
  auto dba = db.Access();
  auto label = dba->Label("label");
  AstStorage storage;
  SymbolTable symbol_table;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  auto set_op = std::make_shared<plan::SetLabels>(
      optional, n.sym_, std::vector<storage::Label>{label});
  EXPECT_EQ(0, CountIterable(dba->Vertices(false)));
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TEST(QueryPlan, RemovePropertyOnNull) {
  // REMOVE (Null).prop
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  AstStorage storage;
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR("property");
  auto null = LITERAL(TypedValue::Null);
  auto n_prop = PROPERTY_LOOKUP(null, prop);
  auto once = std::make_shared<Once>();
  auto remove_op =
      std::make_shared<plan::RemoveProperty>(once, prop.second, n_prop);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*remove_op, &context));
}

TEST(QueryPlan, RemoveLabelsOnNull) {
  // OPTIONAL MATCH (n) REMOVE n :label
  database::GraphDb db;
  auto dba = db.Access();
  auto label = dba->Label("label");
  AstStorage storage;
  SymbolTable symbol_table;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  auto remove_op = std::make_shared<plan::RemoveLabels>(
      optional, n.sym_, std::vector<storage::Label>{label});
  EXPECT_EQ(0, CountIterable(dba->Vertices(false)));
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_EQ(1, PullAll(*remove_op, &context));
}

TEST(QueryPlan, DeleteSetProperty) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n SET n.property = 42
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR("property");
  auto n_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto set_op = std::make_shared<plan::SetProperty>(delete_op, prop.second,
                                                    n_prop, LITERAL(42));
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*set_op, &context), QueryRuntimeException);
}

TEST(QueryPlan, DeleteSetPropertiesFromMap) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n SET n = {property: 42}
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR("property");
  std::unordered_map<PropertyIx, Expression *> prop_map;
  prop_map.emplace(storage.GetPropertyIx(prop.first), LITERAL(42));
  auto *rhs = storage.Create<MapLiteral>(prop_map);
  for (auto op_type :
       {plan::SetProperties::Op::REPLACE, plan::SetProperties::Op::UPDATE}) {
    auto set_op =
        std::make_shared<plan::SetProperties>(delete_op, n.sym_, rhs, op_type);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*set_op, &context), QueryRuntimeException);
  }
}

TEST(QueryPlan, DeleteSetPropertiesFromVertex) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  // Add a single vertex.
  {
    auto v = dba.InsertVertex();
    v.PropsSet(dba.Property("property"), 1);
  }
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n SET n = n
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, false);
  auto *rhs = IDENT("n")->MapTo(n.sym_);
  for (auto op_type :
       {plan::SetProperties::Op::REPLACE, plan::SetProperties::Op::UPDATE}) {
    auto set_op =
        std::make_shared<plan::SetProperties>(delete_op, n.sym_, rhs, op_type);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*set_op, &context), QueryRuntimeException);
  }
}

TEST(QueryPlan, DeleteRemoveLabels) {
  database::GraphDb db;
  auto dba = db.Access();
  // Add a single vertex.
  dba->InsertVertex();
  dba->AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba->Vertices(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n REMOVE n :label
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, false);
  std::vector<storage::Label> labels{dba->Label("label")};
  auto rem_op = std::make_shared<plan::RemoveLabels>(delete_op, n.sym_, labels);
  auto context = MakeContext(storage, symbol_table, dba.get());
  EXPECT_THROW(PullAll(*rem_op, &context), QueryRuntimeException);
}

TEST(QueryPlan, DeleteRemoveProperty) {
  database::GraphDb db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // MATCH (n) DELETE n REMOVE n.property
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_get = storage.Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR("property");
  auto n_prop = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto rem_op =
      std::make_shared<plan::RemoveProperty>(delete_op, prop.second, n_prop);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*rem_op, &context), QueryRuntimeException);
}
