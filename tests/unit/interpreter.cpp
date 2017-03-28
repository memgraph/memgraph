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
#include "query/context.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/logical/planner.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;

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

int PullAll(std::shared_ptr<LogicalOperator> logical_op, GraphDbAccessor &db,
            SymbolTable symbol_table) {
  Frame frame(symbol_table.max_position());
  auto cursor = logical_op->MakeCursor(db);
  int count = 0;
  while (cursor->Pull(frame, symbol_table)) count++;
  return count;
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input,
                 TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(
      input, std::vector<NamedExpression *>{named_expressions...});
}

struct ScanAllTuple {
  NodeAtom *node_;
  std::shared_ptr<LogicalOperator> op_;
  Symbol sym_;
};

/**
 * Creates and returns a tuple of stuff for a scan-all starting
 * from the node with the given name.
 *
 * Returns (node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAll(AstTreeStorage &storage, SymbolTable &symbol_table,
                         const std::string &identifier) {
  auto node = NODE(identifier);
  auto logical_op = std::make_shared<ScanAll>(node);
  auto symbol = symbol_table.CreateSymbol(identifier);
  symbol_table[*node->identifier_] = symbol;
  //  return std::make_tuple(node, logical_op, symbol);
  return ScanAllTuple{node, logical_op, symbol};
}

struct ExpandTuple {
  EdgeAtom *edge_;
  Symbol edge_sym_;
  NodeAtom *node_;
  Symbol node_sym_;
  std::shared_ptr<LogicalOperator> op_;
};

ExpandTuple MakeExpand(AstTreeStorage &storage, SymbolTable &symbol_table,
                       std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, const std::string &edge_identifier,
                       EdgeAtom::Direction direction, bool edge_cycle,
                       const std::string &node_identifier, bool node_cycle) {
  auto edge = EDGE(edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier);
  symbol_table[*edge->identifier_] = edge_sym;

  auto node = NODE(node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier);
  symbol_table[*node->identifier_] = node_sym;

  auto op = std::make_shared<Expand>(node, edge, input, input_symbol,
                                     node_cycle, edge_cycle);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
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
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(scan_all.op_, output);
  symbol_table[*output->expression_] = scan_all.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 2);
}

TEST(Interpreter, StandaloneReturn) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  dba->insert_vertex();
  dba->insert_vertex();
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto output = NEXPR("n", LITERAL(42));
  auto produce = MakeProduce(std::shared_ptr<LogicalOperator>(nullptr), output);
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 1);
  EXPECT_EQ(result.GetResults()[0].size(), 1);
  EXPECT_EQ(result.GetResults()[0][0].Value<int64_t>(), 42);
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
  n.node_->labels_.emplace_back(label);
  n.node_->properties_[property] = LITERAL(42);

  // node filtering
  auto node_filter = std::make_shared<NodeFilter>(n.op_, n.sym_, n.node_);

  // make a named expression and a produce
  auto output = NEXPR("x", IDENT("n"));
  symbol_table[*output->expression_] = n.sym_;
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
  n.node_->labels_.emplace_back(label1);
  n.node_->labels_.emplace_back(label2);

  // node filtering
  auto node_filter = std::make_shared<NodeFilter>(n.op_, n.sym_, n.node_);

  // make a named expression and a produce
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(node_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  symbol_table[*output->expression_] = n.sym_;

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

  auto node = NODE("n");
  symbol_table[*node->identifier_] = symbol_table.CreateSymbol("n");
  node->labels_.emplace_back(label);
  node->properties_[property] = LITERAL(42);

  auto create = std::make_shared<CreateNode>(node, nullptr);
  PullAll(create, *dba, symbol_table);
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

  auto node = NODE("n");
  auto sym_n = symbol_table.CreateSymbol("n");
  symbol_table[*node->identifier_] = sym_n;
  node->labels_.emplace_back(label);
  node->properties_[property] = LITERAL(42);

  auto create = std::make_shared<CreateNode>(node, nullptr);
  auto named_expr_n = NEXPR("n", IDENT("n"));
  symbol_table[*named_expr_n] = symbol_table.CreateSymbol("named_expr_n");
  symbol_table[*named_expr_n->expression_] = sym_n;
  auto prop_lookup = PROPERTY_LOOKUP("n", property);
  symbol_table[*prop_lookup->expression_] = sym_n;
  auto named_expr_n_p = NEXPR("n", prop_lookup);
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
    auto n = NODE("n");
    n->labels_.emplace_back(label_node_1);
    n->properties_[property] = LITERAL(1);
    auto n_sym = symbol_table.CreateSymbol("n");
    symbol_table[*n->identifier_] = n_sym;

    // data for the second node
    auto m = NODE("m");
    m->labels_.emplace_back(label_node_2);
    m->properties_[property] = LITERAL(2);
    if (cycle)
      symbol_table[*m->identifier_] = n_sym;
    else
      symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m");

    auto r = EDGE("r", EdgeAtom::Direction::RIGHT);
    symbol_table[*r->identifier_] = symbol_table.CreateSymbol("r");
    r->edge_types_.emplace_back(edge_type);
    r->properties_[property] = LITERAL(3);

    auto create_op = std::make_shared<CreateNode>(n, nullptr);
    auto create_expand =
        std::make_shared<CreateExpand>(m, r, create_op, n_sym, cycle);
    PullAll(create_expand, *dba, symbol_table);
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

TEST(Interpreter, MatchCreateNode) {
  Dbms dbms;
  auto dba = dbms.active();

  // add three nodes we'll match and expand-create from
  dba->insert_vertex();
  dba->insert_vertex();
  dba->insert_vertex();
  dba->advance_command();

  SymbolTable symbol_table;
  AstTreeStorage storage;

  // first node
  auto n_scan_all = MakeScanAll(storage, symbol_table, "n");
  // second node
  auto m = NODE("m");
  symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m");
  // creation op
  auto create_node = std::make_shared<CreateNode>(m, n_scan_all.op_);

  EXPECT_EQ(CountIterable(dba->vertices()), 3);
  PullAll(create_node, *dba, symbol_table);
  dba->advance_command();
  EXPECT_EQ(CountIterable(dba->vertices()), 6);
}

TEST(Interpreter, MatchCreateExpand) {
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

    // data for the second node
    auto m = NODE("m");
    if (cycle)
      symbol_table[*m->identifier_] = n_scan_all.sym_;
    else
      symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m");

    auto r = EDGE("r", EdgeAtom::Direction::RIGHT);
    symbol_table[*r->identifier_] = symbol_table.CreateSymbol("r");
    r->edge_types_.emplace_back(edge_type);

    auto create_expand = std::make_shared<CreateExpand>(m, r, n_scan_all.op_,
                                                        n_scan_all.sym_, cycle);
    PullAll(create_expand, *dba, symbol_table);
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
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", direction,
                          false, "m", false);

    // make a named expression and a produce
    auto output = NEXPR("m", IDENT("m"));
    symbol_table[*output->expression_] = r_m.node_sym_;
    symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
    auto produce = MakeProduce(r_m.op_, output);

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
    auto r_n = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::RIGHT, false, "n", with_cycle);
    if (with_cycle)
      symbol_table[*r_n.node_->identifier_] =
          symbol_table[*n.node_->identifier_];

    // make a named expression and a produce
    auto output = NEXPR("n", IDENT("n"));
    symbol_table[*output->expression_] = n.sym_;
    symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
    auto produce = MakeProduce(r_n.op_, output);

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
    auto r_j = MakeExpand(storage, symbol_table, i.op_, i.sym_, "r",
                          EdgeAtom::Direction::BOTH, false, "j", false);
    auto r_k = MakeExpand(storage, symbol_table, r_j.op_, r_j.node_sym_, "r",
                          EdgeAtom::Direction::BOTH, with_cycle, "k", false);
    if (with_cycle)
      symbol_table[*r_k.edge_->identifier_] =
          symbol_table[*r_j.edge_->identifier_];

    // make a named expression and a produce
    auto output = NEXPR("r", IDENT("r"));
    symbol_table[*output->expression_] = r_j.edge_sym_;
    symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
    auto produce = MakeProduce(r_k.op_, output);

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
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                        EdgeAtom::Direction::RIGHT, false, "m", false);
  r_m.edge_->edge_types_.push_back(edge_types[0]);
  r_m.edge_->properties_[prop] = LITERAL(42);
  auto edge_filter =
      std::make_shared<EdgeFilter>(r_m.op_, r_m.edge_sym_, r_m.edge_);

  // make a named expression and a produce
  auto output = NEXPR("m", IDENT("m"));
  symbol_table[*output->expression_] = r_m.node_sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  auto produce = MakeProduce(edge_filter, output);

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 1);
}

TEST(Interpreter, EdgeFilterMultipleTypes) {
  Dbms dbms;
  auto dba = dbms.active();

  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto type_1 = dba->edge_type("type_1");
  auto type_2 = dba->edge_type("type_2");
  auto type_3 = dba->edge_type("type_3");
  dba->insert_edge(v1, v2, type_1);
  dba->insert_edge(v1, v2, type_2);
  dba->insert_edge(v1, v2, type_3);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                        EdgeAtom::Direction::RIGHT, false, "m", false);
  // add a property filter
  auto edge_filter =
      std::make_shared<EdgeFilter>(r_m.op_, r_m.edge_sym_, r_m.edge_);
  r_m.edge_->edge_types_.push_back(type_1);
  r_m.edge_->edge_types_.push_back(type_2);

  // make a named expression and a produce
  auto output = NEXPR("m", IDENT("m"));
  auto produce = MakeProduce(edge_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  symbol_table[*output->expression_] = r_m.node_sym_;

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 2);
}

TEST(Interpreter, Delete) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 4; ++i) vertices.push_back(dba->insert_vertex());
  auto type = dba->edge_type("type");
  for (int j = 0; j < 4; ++j)
    for (int k = j + 1; k < 4; ++k)
      dba->insert_edge(vertices[j], vertices[k], type);

  dba->advance_command();
  EXPECT_EQ(4, CountIterable(dba->vertices()));
  EXPECT_EQ(6, CountIterable(dba->edges()));

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // attempt to delete a vertex, and fail
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n");
    symbol_table[*n_get] = n.sym_;
    auto delete_op = std::make_shared<plan::Delete>(
        n.op_, std::vector<Expression *>{n_get}, false);
    EXPECT_THROW(PullAll(delete_op, *dba, symbol_table), QueryRuntimeException);
    dba->advance_command();
    EXPECT_EQ(4, CountIterable(dba->vertices()));
    EXPECT_EQ(6, CountIterable(dba->edges()));
  }

  // detach delete a single vertex
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n");
    symbol_table[*n_get] = n.sym_;
    auto delete_op = std::make_shared<plan::Delete>(
        n.op_, std::vector<Expression *>{n_get}, true);
    Frame frame(symbol_table.max_position());
    delete_op->MakeCursor(*dba)->Pull(frame, symbol_table);
    dba->advance_command();
    EXPECT_EQ(3, CountIterable(dba->vertices()));
    EXPECT_EQ(3, CountIterable(dba->edges()));
  }

  // delete all remaining edges
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::RIGHT, false, "m", false);
    auto r_get = storage.Create<Identifier>("r");
    symbol_table[*r_get] = r_m.edge_sym_;
    auto delete_op = std::make_shared<plan::Delete>(
        r_m.op_, std::vector<Expression *>{r_get}, false);
    PullAll(delete_op, *dba, symbol_table);
    dba->advance_command();
    EXPECT_EQ(3, CountIterable(dba->vertices()));
    EXPECT_EQ(0, CountIterable(dba->edges()));
  }

  // delete all remaining vertices
  {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_get = storage.Create<Identifier>("n");
    symbol_table[*n_get] = n.sym_;
    auto delete_op = std::make_shared<plan::Delete>(
        n.op_, std::vector<Expression *>{n_get}, false);
    PullAll(delete_op, *dba, symbol_table);
    dba->advance_command();
    EXPECT_EQ(0, CountIterable(dba->vertices()));
    EXPECT_EQ(0, CountIterable(dba->edges()));
  }
}

TEST(Interpreter, DeleteReturn) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  auto prop = dba->property("prop");
  for (int i = 0; i < 4; ++i) {
    auto va = dba->insert_vertex();
    va.PropsSet(prop, 42);
  }

  dba->advance_command();
  EXPECT_EQ(4, CountIterable(dba->vertices()));
  EXPECT_EQ(0, CountIterable(dba->edges()));

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");

  auto n_get = storage.Create<Identifier>("n");
  symbol_table[*n_get] = n.sym_;
  auto delete_op = std::make_shared<plan::Delete>(
      n.op_, std::vector<Expression *>{n_get}, true);

  auto prop_lookup =
      storage.Create<PropertyLookup>(storage.Create<Identifier>("n"), prop);
  symbol_table[*prop_lookup->expression_] = n.sym_;
  auto n_p = storage.Create<NamedExpression>("n", prop_lookup);
  symbol_table[*n_p] = symbol_table.CreateSymbol("bla");
  auto produce = MakeProduce(delete_op, n_p);

  auto result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(4, result.GetResults().size());
  dba->advance_command();
  EXPECT_EQ(0, CountIterable(dba->vertices()));
}

TEST(Interpreter, Filter) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a 6 nodes with property 'prop', 2 have true as value
  GraphDb::Property property = dba->property("Property");
  for (int i = 0; i < 6; ++i)
    dba->insert_vertex().PropsSet(property, i % 3 == 0);
  dba->insert_vertex();  // prop not set, gives NULL
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto e =
      storage.Create<PropertyLookup>(storage.Create<Identifier>("n"), property);
  symbol_table[*e->expression_] = n.sym_;
  auto f = std::make_shared<Filter>(n.op_, e);

  auto output =
      storage.Create<NamedExpression>("x", storage.Create<Identifier>("n"));
  symbol_table[*output->expression_] = n.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  auto produce = MakeProduce(f, output);

  EXPECT_EQ(CollectProduce(produce, symbol_table, *dba).GetResults().size(), 2);
}

TEST(Interpreter, SetProperty) {
  Dbms dbms;
  auto dba = dbms.active();

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto v3 = dba->insert_vertex();
  auto v4 = dba->insert_vertex();
  auto edge_type = dba->edge_type("edge_type");
  dba->insert_edge(v1, v3, edge_type);
  dba->insert_edge(v2, v4, edge_type);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                        EdgeAtom::Direction::RIGHT, false, "m", false);

  // set prop1 to 42 on n and r
  auto prop1 = dba->property("prop1");
  auto literal = LITERAL(42);

  auto n_p = PROPERTY_LOOKUP("n", prop1);
  symbol_table[*n_p->expression_] = n.sym_;
  auto set_n_p = std::make_shared<plan::SetProperty>(r_m.op_, n_p, literal);

  auto r_p = PROPERTY_LOOKUP("r", prop1);
  symbol_table[*r_p->expression_] = r_m.edge_sym_;
  auto set_r_p = std::make_shared<plan::SetProperty>(set_n_p, r_p, literal);
  EXPECT_EQ(2, PullAll(set_r_p, *dba, symbol_table));
  dba->advance_command();

  EXPECT_EQ(CountIterable(dba->edges()), 2);
  for (EdgeAccessor edge : dba->edges()) {
    ASSERT_EQ(edge.PropsAt(prop1).type(), PropertyValue::Type::Int);
    EXPECT_EQ(edge.PropsAt(prop1).Value<int64_t>(), 42);
    VertexAccessor from = edge.from();
    VertexAccessor to = edge.to();
    ASSERT_EQ(from.PropsAt(prop1).type(), PropertyValue::Type::Int);
    EXPECT_EQ(from.PropsAt(prop1).Value<int64_t>(), 42);
    ASSERT_EQ(to.PropsAt(prop1).type(), PropertyValue::Type::Null);
  }
}

TEST(Interpreter, SetProperties) {
  auto test_set_properties = [](bool update) {
    Dbms dbms;
    auto dba = dbms.active();

    // graph: ({a: 0})-[:R {b:1}]->({c:2})
    auto prop_a = dba->property("a");
    auto prop_b = dba->property("b");
    auto prop_c = dba->property("c");
    auto v1 = dba->insert_vertex();
    auto v2 = dba->insert_vertex();
    auto e = dba->insert_edge(v1, v2, dba->edge_type("R"));
    v1.PropsSet(prop_a, 0);
    e.PropsSet(prop_b, 1);
    v2.PropsSet(prop_c, 2);
    dba->advance_command();

    AstTreeStorage storage;
    SymbolTable symbol_table;

    // scan (n)-[r]->(m)
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::RIGHT, false, "m", false);

    auto op = update ? plan::SetProperties::Op::UPDATE
                     : plan::SetProperties::Op::REPLACE;

    // set properties on r to n, and on r to m
    auto r_ident = IDENT("r");
    symbol_table[*r_ident] = r_m.edge_sym_;
    auto m_ident = IDENT("m");
    symbol_table[*m_ident] = r_m.node_sym_;
    auto set_r_to_n =
        std::make_shared<plan::SetProperties>(r_m.op_, n.sym_, r_ident, op);
    auto set_m_to_r = std::make_shared<plan::SetProperties>(
        set_r_to_n, r_m.edge_sym_, m_ident, op);
    EXPECT_EQ(1, PullAll(set_m_to_r, *dba, symbol_table));
    dba->advance_command();

    EXPECT_EQ(CountIterable(dba->edges()), 1);
    for (EdgeAccessor edge : dba->edges()) {
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

TEST(Interpreter, SetLabels) {
  Dbms dbms;
  auto dba = dbms.active();

  auto label1 = dba->label("label1");
  auto label2 = dba->label("label2");
  auto label3 = dba->label("label3");
  dba->insert_vertex().add_label(label1);
  dba->insert_vertex().add_label(label1);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_set = std::make_shared<plan::SetLabels>(
      n.op_, n.sym_, std::vector<GraphDb::Label>{label2, label3});
  EXPECT_EQ(2, PullAll(label_set, *dba, symbol_table));

  for (VertexAccessor vertex : dba->vertices()) {
    EXPECT_EQ(3, vertex.labels().size());
    EXPECT_TRUE(vertex.has_label(label2));
    EXPECT_TRUE(vertex.has_label(label3));
  }
}
