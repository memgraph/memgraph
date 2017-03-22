//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "dbms/dbms.hpp"
#include "query/entry.hpp"

using namespace query;

/**
 * Helper function that collects all the results from the given
 * Produce into a ResultStreamFaker and returns that object.
 *
 * @param produce
 * @param symbol_table
 * @param db_accessor
 * @return
 */
auto CollectProduce(std::shared_ptr<Produce> produce, SymbolTable &symbol_table,
                    GraphDbAccessor &db_accessor) {
  ResultStreamFaker stream;
  Frame frame(symbol_table.max_position());

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // generate header
  std::vector<std::string> header;
  for (auto named_expression : produce->named_expressions())
    header.push_back(named_expression->name_);
  stream.Header(header);

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce->named_expressions())
    symbols.emplace_back(symbol_table[*named_expression]);

  // stream out results
  auto cursor = produce->MakeCursor(db_accessor);
  while (cursor->Pull(frame, symbol_table)) {
    std::vector<TypedValue> values;
    for (auto &symbol : symbols) values.emplace_back(frame[symbol]);
    stream.Result(values);
  }

  stream.Summary({{std::string("type"), TypedValue("r")}});

  return stream;
}

void ExecuteCreate(std::shared_ptr<LogicalOperator> create, GraphDbAccessor &db,
                   SymbolTable symbol_table) {
  Frame frame(symbol_table.max_position());
  auto cursor = create->MakeCursor(db);
  while (cursor->Pull(frame, symbol_table)) {
    continue;
  }
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input,
                 TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(
      input, std::vector<NamedExpression *>{named_expressions...});
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting
 * from the node with the given name.
 *
 * Returns (node_atom, scan_all_logical_op, symbol).
 */
auto MakeScanAll(AstTreeStorage &ast_storage, SymbolTable &symbol_table,
                 const std::string &identifier) {
  auto node =
      ast_storage.Create<NodeAtom>(ast_storage.Create<Identifier>(identifier));
  auto logical_op = std::make_shared<ScanAll>(node);
  auto symbol = symbol_table.CreateSymbol(identifier);
  symbol_table[*node->identifier_] = symbol;
  return std::make_tuple(node, logical_op, symbol);
}

auto MakeExpand(AstTreeStorage &ast_storage, SymbolTable &symbol_table,
                std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
                const std::string &edge_identifier,
                EdgeAtom::Direction direction, bool edge_cycle,
                const std::string &node_identifier, bool node_cycle) {
  auto edge = ast_storage.Create<EdgeAtom>(
      ast_storage.Create<Identifier>(edge_identifier), direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier);
  symbol_table[*edge->identifier_] = edge_sym;

  auto node = ast_storage.Create<NodeAtom>(
      ast_storage.Create<Identifier>(node_identifier));
  auto node_sym = symbol_table.CreateSymbol(node_identifier);
  symbol_table[*node->identifier_] = node_sym;

  auto op = std::make_shared<Expand>(node, edge, input, input_symbol,
                                     node_cycle, edge_cycle);

  return std::make_tuple(edge, edge_sym, node, node_sym, op);
}

template <typename TIterable>
auto CountIterable(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}

/*
 * Actual tests start here.
 */

TEST(Interpreter, MatchReturn) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  dba->insert_vertex();
  dba->insert_vertex();
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto output =
      storage.Create<NamedExpression>("n", storage.Create<Identifier>("n"));
  auto produce = MakeProduce(std::get<1>(scan_all), output);
  symbol_table[*output->expression_] = std::get<2>(scan_all);
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 2);
}

TEST(Interpreter, NodeFilterLabelsAndProperties) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  GraphDb::Label label = dba->label("Label");
  GraphDb::Property property = dba->property("Property");
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto v3 = dba->insert_vertex();
  auto v4 = dba->insert_vertex();
  auto v5 = dba->insert_vertex();
  dba->insert_vertex();
  // test all combination of (label | no_label) * (no_prop | wrong_prop |
  // right_prop)
  // only v1 will have the right labels
  v1.add_label(label);
  v2.add_label(label);
  v3.add_label(label);
  v1.PropsSet(property, 42);
  v2.PropsSet(property, 1);
  v4.PropsSet(property, 42);
  v5.PropsSet(property, 1);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  std::get<0>(n)->labels_.emplace_back(label);
  std::get<0>(n)->properties_[property] = storage.Create<Literal>(42);

  // node filtering
  auto node_filter = std::make_shared<NodeFilter>(
      std::get<1>(n), std::get<2>(n), std::get<0>(n));

  // make a named expression and a produce
  auto output =
      storage.Create<NamedExpression>("x", storage.Create<Identifier>("n"));
  symbol_table[*output->expression_] = std::get<2>(n);
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  auto produce = MakeProduce(node_filter, output);

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 1);
}

TEST(Interpreter, NodeFilterMultipleLabels) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  GraphDb::Label label1 = dba->label("label1");
  GraphDb::Label label2 = dba->label("label2");
  GraphDb::Label label3 = dba->label("label3");
  // the test will look for nodes that have label1 and label2
  dba->insert_vertex();                    // NOT accepted
  dba->insert_vertex().add_label(label1);  // NOT accepted
  dba->insert_vertex().add_label(label2);  // NOT accepted
  dba->insert_vertex().add_label(label3);  // NOT accepted
  auto v1 = dba->insert_vertex();          // YES accepted
  v1.add_label(label1);
  v1.add_label(label2);
  auto v2 = dba->insert_vertex();  // NOT accepted
  v2.add_label(label1);
  v2.add_label(label3);
  auto v3 = dba->insert_vertex();  // YES accepted
  v3.add_label(label1);
  v3.add_label(label2);
  v3.add_label(label3);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  std::get<0>(n)->labels_.emplace_back(label1);
  std::get<0>(n)->labels_.emplace_back(label2);

  // node filtering
  auto node_filter = std::make_shared<NodeFilter>(
      std::get<1>(n), std::get<2>(n), std::get<0>(n));

  // make a named expression and a produce
  auto output =
      storage.Create<NamedExpression>("n", storage.Create<Identifier>("n"));
  auto produce = MakeProduce(node_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  symbol_table[*output->expression_] = std::get<2>(n);

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 2);
}

TEST(Interpreter, CreateNodeWithAttributes) {
  Dbms dbms;
  auto dba = dbms.active();

  GraphDb::Label label = dba->label("Person");
  GraphDb::Property property = dba->label("age");

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  symbol_table[*node->identifier_] = symbol_table.CreateSymbol("n");
  node->labels_.emplace_back(label);
  node->properties_[property] = storage.Create<Literal>(42);

  auto create = std::make_shared<CreateOp>(node);
  ExecuteCreate(create, *dba, symbol_table);
  dba->advance_command();

  // count the number of vertices
  int vertex_count = 0;
  for (VertexAccessor vertex : dba->vertices()) {
    vertex_count++;
    EXPECT_EQ(vertex.labels().size(), 1);
    EXPECT_EQ(*vertex.labels().begin(), label);
    EXPECT_EQ(vertex.Properties().size(), 1);
    auto prop_eq = vertex.PropsAt(property) == TypedValue(42);
    ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
    EXPECT_TRUE(prop_eq.Value<bool>());
  }
  EXPECT_EQ(vertex_count, 1);
}

TEST(Interpreter, CreateReturn) {
  // test CREATE (n:Person {age: 42}) RETURN n, n.age
  Dbms dbms;
  auto dba = dbms.active();

  GraphDb::Label label = dba->label("Person");
  GraphDb::Property property = dba->label("age");

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  auto sym_n = symbol_table.CreateSymbol("n");
  symbol_table[*node->identifier_] = sym_n;
  node->labels_.emplace_back(label);
  node->properties_[property] = storage.Create<Literal>(42);

  auto create = std::make_shared<CreateOp>(node);
  auto named_expr_n =
      storage.Create<NamedExpression>("n", storage.Create<Identifier>("n"));
  symbol_table[*named_expr_n] = symbol_table.CreateSymbol("named_expr_n");
  symbol_table[*named_expr_n->expression_] = sym_n;
  auto prop_lookup =
      storage.Create<PropertyLookup>(storage.Create<Identifier>("n"), property);
  symbol_table[*prop_lookup->expression_] = sym_n;
  auto named_expr_n_p = storage.Create<NamedExpression>("n", prop_lookup);
  symbol_table[*named_expr_n_p] = symbol_table.CreateSymbol("named_expr_n_p");
  symbol_table[*named_expr_n->expression_] = sym_n;

  auto produce = MakeProduce(create, named_expr_n, named_expr_n_p);
  auto result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(1, result.GetResults().size());
  EXPECT_EQ(2, result.GetResults()[0].size());
  EXPECT_EQ(TypedValue::Type::Vertex, result.GetResults()[0][0].type());
  EXPECT_EQ(1,
            result.GetResults()[0][0].Value<VertexAccessor>().labels().size());
  EXPECT_EQ(label,
            result.GetResults()[0][0].Value<VertexAccessor>().labels()[0]);
  EXPECT_EQ(TypedValue::Type::Int, result.GetResults()[0][1].type());
  EXPECT_EQ(42, result.GetResults()[0][1].Value<int64_t>());

  dba->advance_command();
  EXPECT_EQ(1, CountIterable(dba->vertices()));
}

TEST(Interpreter, CreateExpand) {
  Dbms dbms;
  auto dba = dbms.active();

  GraphDb::Label label_node_1 = dba->label("Node1");
  GraphDb::Label label_node_2 = dba->label("Node2");
  GraphDb::Property property = dba->label("prop");
  GraphDb::EdgeType edge_type = dba->label("edge_type");

  SymbolTable symbol_table;
  AstTreeStorage storage;

  auto test_create_path = [&](bool cycle, int expected_nodes_created,
                              int expected_edges_created) {
    int before_v = CountIterable(dba->vertices());
    int before_e = CountIterable(dba->edges());

    // data for the first node
    auto n = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
    n->labels_.emplace_back(label_node_1);
    n->properties_[property] = storage.Create<Literal>(1);
    auto n_sym = symbol_table.CreateSymbol("n");
    symbol_table[*n->identifier_] = n_sym;

    // data for the second node
    auto m = storage.Create<NodeAtom>(storage.Create<Identifier>("m"));
    m->labels_.emplace_back(label_node_2);
    m->properties_[property] = storage.Create<Literal>(2);
    if (cycle)
      symbol_table[*m->identifier_] = n_sym;
    else
      symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m");

    auto r = storage.Create<EdgeAtom>(storage.Create<Identifier>("r"),
                                      EdgeAtom::Direction::RIGHT);
    r->edge_types_.emplace_back(edge_type);
    r->properties_[property] = storage.Create<Literal>(3);

    auto create_op = std::make_shared<CreateOp>(n);
    auto create_expand =
        std::make_shared<CreateExpand>(m, r, create_op, n_sym, cycle);
    ExecuteCreate(create_expand, *dba, symbol_table);
    dba->advance_command();

    EXPECT_EQ(CountIterable(dba->vertices()) - before_v,
              expected_nodes_created);
    EXPECT_EQ(CountIterable(dba->edges()) - before_e, expected_edges_created);
  };

  test_create_path(false, 2, 1);
  test_create_path(true, 1, 1);

  for (VertexAccessor vertex : dba->vertices()) {
    EXPECT_EQ(vertex.labels().size(), 1);
    GraphDb::Label label = vertex.labels()[0];
    if (label == label_node_1) {
      // node created by first op
      EXPECT_EQ(vertex.PropsAt(property).Value<int64_t>(), 1);
    } else if (label == label_node_2) {
      // node create by expansion
      EXPECT_EQ(vertex.PropsAt(property).Value<int64_t>(), 2);
    } else {
      // should not happen
      FAIL();
    }

    for (EdgeAccessor edge : dba->edges()) {
      EXPECT_EQ(edge.edge_type(), edge_type);
      EXPECT_EQ(edge.PropsAt(property).Value<int64_t>(), 3);
    }
  }
}

TEST(Interpreter, MatchCreate) {
  Dbms dbms;
  auto dba = dbms.active();

  // add three nodes we'll match and expand-create from
  dba->insert_vertex();
  dba->insert_vertex();
  dba->insert_vertex();
  dba->advance_command();

  //  GraphDb::Label label_node_1 = dba->label("Node1");
  //  GraphDb::Label label_node_2 = dba->label("Node2");
  //  GraphDb::Property property = dba->label("prop");
  GraphDb::EdgeType edge_type = dba->label("edge_type");

  SymbolTable symbol_table;
  AstTreeStorage storage;

  auto test_create_path = [&](bool cycle, int expected_nodes_created,
                              int expected_edges_created) {
    int before_v = CountIterable(dba->vertices());
    int before_e = CountIterable(dba->edges());

    // data for the first node
    auto n_scan_all = MakeScanAll(storage, symbol_table, "n");
    auto n_sym = symbol_table.CreateSymbol("n");
    symbol_table[*std::get<0>(n_scan_all)->identifier_] = n_sym;

    // data for the second node
    auto m = storage.Create<NodeAtom>(storage.Create<Identifier>("m"));
    if (cycle)
      symbol_table[*m->identifier_] = n_sym;
    else
      symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m");

    auto r = storage.Create<EdgeAtom>(storage.Create<Identifier>("r"),
                                      EdgeAtom::Direction::RIGHT);
    r->edge_types_.emplace_back(edge_type);

    auto create_expand = std::make_shared<CreateExpand>(
        m, r, std::get<1>(n_scan_all), n_sym, cycle);
    ExecuteCreate(create_expand, *dba, symbol_table);
    dba->advance_command();

    EXPECT_EQ(CountIterable(dba->vertices()) - before_v,
              expected_nodes_created);
    EXPECT_EQ(CountIterable(dba->edges()) - before_e, expected_edges_created);
  };

  test_create_path(false, 3, 3);
  test_create_path(true, 0, 6);
}

TEST(Interpreter, Expand) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  auto v1 = dba->insert_vertex();
  v1.add_label((GraphDb::Label)1);
  auto v2 = dba->insert_vertex();
  v2.add_label((GraphDb::Label)2);
  auto v3 = dba->insert_vertex();
  v3.add_label((GraphDb::Label)3);
  auto edge_type = dba->edge_type("Edge");
  dba->insert_edge(v1, v2, edge_type);
  dba->insert_edge(v1, v3, edge_type);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_expand = [&](EdgeAtom::Direction direction,
                         int expected_result_count) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, std::get<1>(n), std::get<2>(n),
                          "r", direction, false, "m", false);

    // make a named expression and a produce
    auto output =
        storage.Create<NamedExpression>("m", storage.Create<Identifier>("m"));
    symbol_table[*output->expression_] = std::get<3>(r_m);
    symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
    auto produce = MakeProduce(std::get<4>(r_m), output);

    ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
    EXPECT_EQ(result.GetResults().size(), expected_result_count);
  };

  test_expand(EdgeAtom::Direction::RIGHT, 2);
  test_expand(EdgeAtom::Direction::LEFT, 2);
  test_expand(EdgeAtom::Direction::BOTH, 4);
}

TEST(Interpreter, ExpandNodeCycle) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a graph (v1)->(v2) that
  // has a recursive edge (v1)->(v1)
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto edge_type = dba->edge_type("Edge");
  dba->insert_edge(v1, v1, edge_type);
  dba->insert_edge(v1, v2, edge_type);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_cycle = [&](bool with_cycle, int expected_result_count) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_n =
        MakeExpand(storage, symbol_table, std::get<1>(n), std::get<2>(n), "r",
                   EdgeAtom::Direction::RIGHT, false, "n", with_cycle);
    if (with_cycle)
      symbol_table[*std::get<2>(r_n)->identifier_] =
          symbol_table[*std::get<0>(n)->identifier_];

    // make a named expression and a produce
    auto output =
        storage.Create<NamedExpression>("n", storage.Create<Identifier>("n"));
    symbol_table[*output->expression_] = std::get<2>(n);
    symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
    auto produce = MakeProduce(std::get<4>(r_n), output);

    ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
    EXPECT_EQ(result.GetResults().size(), expected_result_count);
  };

  test_cycle(true, 1);
  test_cycle(false, 2);
}

TEST(Interpreter, ExpandEdgeCycle) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  auto v1 = dba->insert_vertex();
  v1.add_label((GraphDb::Label)1);
  auto v2 = dba->insert_vertex();
  v2.add_label((GraphDb::Label)2);
  auto v3 = dba->insert_vertex();
  v3.add_label((GraphDb::Label)3);
  auto edge_type = dba->edge_type("Edge");
  dba->insert_edge(v1, v2, edge_type);
  dba->insert_edge(v1, v3, edge_type);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_cycle = [&](bool with_cycle, int expected_result_count) {
    auto i = MakeScanAll(storage, symbol_table, "i");
    auto r_j = MakeExpand(storage, symbol_table, std::get<1>(i), std::get<2>(i),
                          "r", EdgeAtom::Direction::BOTH, false, "j", false);
    auto r_k =
        MakeExpand(storage, symbol_table, std::get<4>(r_j), std::get<3>(r_j),
                   "r", EdgeAtom::Direction::BOTH, with_cycle, "k", false);
    if (with_cycle)
      symbol_table[*std::get<0>(r_k)->identifier_] =
          symbol_table[*std::get<0>(r_j)->identifier_];

    // make a named expression and a produce
    auto output =
        storage.Create<NamedExpression>("r", storage.Create<Identifier>("r"));
    symbol_table[*output->expression_] = std::get<1>(r_j);
    symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
    auto produce = MakeProduce(std::get<4>(r_k), output);

    ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
    EXPECT_EQ(result.GetResults().size(), expected_result_count);

  };

  test_cycle(true, 4);
  test_cycle(false, 6);
}

TEST(Interpreter, EdgeFilter) {
  Dbms dbms;
  auto dba = dbms.active();

  // make an N-star expanding from (v1)
  // where only one edge will qualify
  // and there are all combinations of
  // (edge_type yes|no) * (property yes|absent|no)
  std::vector<GraphDb::EdgeType> edge_types;
  for (int j = 0; j < 2; ++j)
    edge_types.push_back(dba->edge_type("et" + std::to_string(j)));
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 7; ++i) vertices.push_back(dba->insert_vertex());
  GraphDb::Property prop = dba->property("prop");
  std::vector<EdgeAccessor> edges;
  for (int i = 0; i < 6; ++i) {
    edges.push_back(
        dba->insert_edge(vertices[0], vertices[i + 1], edge_types[i % 2]));
    switch (i % 3) {
      case 0:
        edges.back().PropsSet(prop, 42);
        break;
      case 1:
        edges.back().PropsSet(prop, 100);
        break;
      default:
        break;
    }
  }
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // define an operator tree for query
  // MATCH (n)-[r]->(m) RETURN m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, std::get<1>(n), std::get<2>(n),
                        "r", EdgeAtom::Direction::RIGHT, false, "m", false);
  std::get<0>(r_m)->edge_types_.push_back(edge_types[0]);
  // TODO when int literal expression becomes available
  // add a property filter
  auto edge_filter = std::make_shared<EdgeFilter>(
      std::get<4>(r_m), std::get<1>(r_m), std::get<0>(r_m));

  // make a named expression and a produce
  auto output =
      storage.Create<NamedExpression>("m", storage.Create<Identifier>("m"));
  symbol_table[*output->expression_] = std::get<3>(r_m);
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  auto produce = MakeProduce(edge_filter, output);

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  // TODO change the expected value to 1 once property filtering is available
  EXPECT_EQ(result.GetResults().size(), 3);
}
