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
    for (auto &symbol : symbols) values.emplace_back(frame[symbol]);
    stream.Result(values);
  }

  stream.Summary({{std::string("type"), TypedValue("r")}});

  return stream;
}

/*
 * Following are helper functions that create high level AST
 * and logical operator objects.
 */

auto MakeNamedExpression(Context &ctx, const std::string name,
                         std::shared_ptr<Expression> expression) {
  auto named_expression = std::make_shared<NamedExpression>(ctx.next_uid());
  named_expression->name_ = name;
  named_expression->expression_ = expression;
  return named_expression;
}

auto MakeIdentifier(Context &ctx, const std::string name) {
  return std::make_shared<Identifier>(ctx.next_uid(), name);
}

auto MakeNode(Context &ctx, std::shared_ptr<Identifier> identifier) {
  auto node = std::make_shared<NodeAtom>(ctx.next_uid());
  node->identifier_ = identifier;
  return node;
}

auto MakeScanAll(std::shared_ptr<NodeAtom> node_atom) {
  return std::make_shared<ScanAll>(node_atom);
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input,
                 TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(
      input,
      std::vector<std::shared_ptr<NamedExpression>>{named_expressions...});
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

  Config config;
  Context ctx(config, *dba);

  // make a scan all
  auto node = MakeNode(ctx, MakeIdentifier(ctx, "n"));
  auto scan_all = MakeScanAll(node);

  // make a named expression and a produce
  auto output = MakeNamedExpression(ctx, "n", MakeIdentifier(ctx, "n"));
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
  // test all combination of (label | no_label) * (no_prop | wrong_prop | right_prop)
  // only v1 will have the right labels
  v1.add_label(label);
  v2.add_label(label);
  v3.add_label(label);
  v1.PropsSet(property, 42);
  v2.PropsSet(property, 1);
  v4.PropsSet(property, 42);
  v5.PropsSet(property, 1);

  Config config;
  Context ctx(config, *dba);

  // make a scan all
  auto node = MakeNode(ctx, MakeIdentifier(ctx, "n"));
  auto scan_all = MakeScanAll(node);

  // node filtering
  SymbolTable symbol_table;
  auto n_symbol = symbol_table.CreateSymbol("n");
  // TODO implement the test once int-literal expressions are available
  auto node_filter = std::make_shared<NodeFilter>(
      scan_all, n_symbol, std::vector<GraphDb::Label>{label},
      std::map<GraphDb::Property, std::shared_ptr<Expression>>());

  // make a named expression and a produce
  auto output = MakeNamedExpression(ctx, "n", MakeIdentifier(ctx, "n"));
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
  dba->insert_vertex(); // NOT accepted
  dba->insert_vertex().add_label(label1); // NOT accepted
  dba->insert_vertex().add_label(label2); // NOT accepted
  dba->insert_vertex().add_label(label3); // NOT accepted
  auto v1 = dba->insert_vertex(); // YES accepted
  v1.add_label(label1);
  v1.add_label(label2);
  auto v2 = dba->insert_vertex(); // NOT accepted
  v2.add_label(label1);
  v2.add_label(label3);
  auto v3 = dba->insert_vertex(); // YES accepted
  v3.add_label(label1);
  v3.add_label(label2);
  v3.add_label(label3);

  Config config;
  Context ctx(config, *dba);

  // make a scan all
  auto node = MakeNode(ctx, MakeIdentifier(ctx, "n"));
  auto scan_all = MakeScanAll(node);

  // node filtering
  SymbolTable symbol_table;
  auto n_symbol = symbol_table.CreateSymbol("n");
  // TODO implement the test once int-literal expressions are available
  auto node_filter = std::make_shared<NodeFilter>(
      scan_all, n_symbol, std::vector<GraphDb::Label>{label1, label2},
      std::map<GraphDb::Property, std::shared_ptr<Expression>>());

  // make a named expression and a produce
  auto output = MakeNamedExpression(ctx, "n", MakeIdentifier(ctx, "n"));
  auto produce = MakeProduce(node_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1");
  symbol_table[*node->identifier_] = n_symbol;
  symbol_table[*output->expression_] = n_symbol;

  ResultStreamFaker result = CollectProduce(produce, symbol_table, *dba);
  EXPECT_EQ(result.GetResults().size(), 2);
}
