// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>
#include <sstream>
#include <string>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/parameters.hpp"

std::string AssembleQueryString(const std::string &expression_string) {
  return "return " + expression_string + " as expr";
}

memgraph::query::Query *ParseQuery(const std::string &query_string, memgraph::query::AstStorage *ast_storage) {
  memgraph::query::frontend::ParsingContext context;
  memgraph::query::Parameters parameters;
  memgraph::query::frontend::opencypher::Parser parser(query_string);
  memgraph::query::frontend::CypherMainVisitor visitor(context, ast_storage, &parameters);

  visitor.visit(parser.tree());
  return visitor.query();
}

memgraph::query::Expression *GetExpression(memgraph::query::Query *query) {
  auto cypher_query = dynamic_cast<memgraph::query::CypherQuery *>(query);
  auto ret = dynamic_cast<memgraph::query::Return *>(cypher_query->single_query_->clauses_[0]);
  auto expr = ret->body_.named_expressions[0]->expression_;

  return expr;
}

int main() {
  memgraph::query::AstStorage ast_storage;
  std::ostringstream ss;
  ss << std::cin.rdbuf();

  auto query_string = AssembleQueryString(ss.str());
  auto query = ParseQuery(query_string, &ast_storage);
  auto expr = GetExpression(query);

  memgraph::query::PrintExpression(expr, &std::cout);
  std::cout << std::endl;

  return 0;
}
