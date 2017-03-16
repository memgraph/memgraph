//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

#include <memory>
#include <vector>

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
    for (auto &symbol : symbols)
      values.emplace_back(frame[symbol]);
    stream.Result(values);
  }

  stream.Summary({{std::string("type"), TypedValue("r")}});

  return stream;
}

void ExecuteCreate(std::shared_ptr<CreateOp> create, GraphDbAccessor &db) {
  SymbolTable symbol_table;
  Frame frame(symbol_table.max_position());
  auto cursor = create->MakeCursor(db);
  while (cursor->Pull(frame, symbol_table)) {
    continue;
  }
}

auto MakeScanAll(NodeAtom *node_atom) {
  return std::make_shared<ScanAll>(node_atom);
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input,
                 TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(
      input, std::vector<NamedExpression *>{named_expressions...});
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

  AstTreeStorage storage;

  // make a scan all
  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  auto scan_all = MakeScanAll(node);

  // make a named expression and a produce
  auto output =
      storage.Create<NamedExpression>("n", storage.Create<Identifier>("n"));
  auto produce = MakeProduce(scan_all, output);

  // fill up the symbol table
  SymbolTable symbol_table;
  auto n_symbol = symbol_table.CreateSymbol("n");
  symbol_table[*node->identifier_] = n_symbol;
  symbol_table[*output->expression_] = n_symbol;
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

  AstTreeStorage storage;

  // make a scan all
  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  auto scan_all = MakeScanAll(node);

  // node filtering
  SymbolTable symbol_table;
  auto n_symbol = symbol_table.CreateSymbol("n");
  // TODO implement the test once int-literal expressions are available
  auto node_filter = std::make_shared<NodeFilter>(
      scan_all, n_symbol, std::vector<GraphDb::Label>{label},
      std::map<GraphDb::Property, Expression*>{});

  // make a named expression and a produce
  auto output =
      storage.Create<NamedExpression>("x", storage.Create<Identifier>("n"));
  auto produce = MakeProduce(node_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  symbol_table[*node->identifier_] = n_symbol;
  symbol_table[*output->expression_] = n_symbol;

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
  dba->insert_vertex();                   // NOT accepted
  dba->insert_vertex().add_label(label1); // NOT accepted
  dba->insert_vertex().add_label(label2); // NOT accepted
  dba->insert_vertex().add_label(label3); // NOT accepted
  auto v1 = dba->insert_vertex();         // YES accepted
  v1.add_label(label1);
  v1.add_label(label2);
  auto v2 = dba->insert_vertex(); // NOT accepted
  v2.add_label(label1);
  v2.add_label(label3);
  auto v3 = dba->insert_vertex(); // YES accepted
  v3.add_label(label1);
  v3.add_label(label2);
  v3.add_label(label3);

  AstTreeStorage storage;

  // make a scan all
  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  auto scan_all = MakeScanAll(node);

  // node filtering
  SymbolTable symbol_table;
  auto n_symbol = symbol_table.CreateSymbol("n");
  // TODO implement the test once int-literal expressions are available
  auto node_filter = std::make_shared<NodeFilter>(
      scan_all, n_symbol, std::vector<GraphDb::Label>{label1, label2},
      std::map<GraphDb::Property, Expression *>());

  // make a named expression and a produce
  auto output =
      storage.Create<NamedExpression>("n", storage.Create<Identifier>("n"));
  auto produce = MakeProduce(node_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  symbol_table[*node->identifier_] = n_symbol;
  symbol_table[*output->expression_] = n_symbol;

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 2);
}

TEST(Interpreter, CreateNodeWithAttributes) {
  Dbms dbms;
  auto dba = dbms.active();

  GraphDb::Label label = dba->label("Person");
  GraphDb::Property property = dba->label("age");

  AstTreeStorage storage;

  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  node->labels_.emplace_back(label);
  // TODO make a property here with an int literal expression
  //  node->properties_[property] = TypedValue(42);

  auto create = std::make_shared<CreateOp>(node);
  ExecuteCreate(create, *dba);

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
