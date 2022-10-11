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

#include <chrono>
#include <limits>
#include <thread>

#include <gtest/gtest.h>

#include "common/types.hpp"
#include "exceptions.hpp"
#include "io/rsm/rsm_client.hpp"
#include "parser/opencypher/parser.hpp"
#include "query/v2/bindings/cypher_main_visitor.hpp"
#include "query/v2/bindings/eval.hpp"
#include "query/v2/bindings/frame.hpp"
#include "query/v2/bindings/symbol_generator.hpp"
#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "storage/v3/id_types.hpp"
#include "utils/string.hpp"

#include "expr/ast/pretty_print_ast_to_original_expression.hpp"

namespace memgraph::query::v2::test {

class ExpressiontoStringTest : public ::testing::TestWithParam<std::pair<std::string, std::string>> {
 protected:
  AstStorage storage;
};

TEST_P(ExpressiontoStringTest, Example) {
  const auto [original_expression, expected_expression] = GetParam();

  memgraph::frontend::opencypher::Parser<frontend::opencypher::ParserOpTag::EXPRESSION> parser(original_expression);
  expr::ParsingContext pc;
  CypherMainVisitor visitor(pc, &storage);

  auto *ast = parser.tree();
  auto expression = visitor.visit(ast);

  const auto rewritten_expression =
      expr::ExpressiontoStringWhileReplacingNodeAndEdgeSymbols(std::any_cast<Expression *>(expression));

  // We check that the expression is what we expect
  EXPECT_EQ(rewritten_expression, expected_expression);

  // We check that the rewritten expression can be parsed again
  memgraph::frontend::opencypher::Parser<frontend::opencypher::ParserOpTag::EXPRESSION> parser2(rewritten_expression);
  expr::ParsingContext pc2;
  CypherMainVisitor visitor2(pc2, &storage);

  auto *ast2 = parser2.tree();
  auto expression2 = visitor2.visit(ast2);
  const auto rewritten_expression2 =
      expr::ExpressiontoStringWhileReplacingNodeAndEdgeSymbols(std::any_cast<Expression *>(expression));

  // We check that the re-written expression from the re-written expression is exactly the same
  EXPECT_EQ(rewritten_expression, rewritten_expression2);
}

INSTANTIATE_TEST_CASE_P(
    PARAMETER, ExpressiontoStringTest,
    ::testing::Values(
        std::make_pair(std::string("2 / 1"), std::string("(2 / 1)")),
        std::make_pair(std::string("2 + 1 + 5 + 2"), std::string("(((2 + 1) + 5) + 2)")),
        std::make_pair(std::string("2 + 1 * 5 + 2"), std::string("((2 + (1 * 5)) + 2)")),
        std::make_pair(std::string("2 + 1 * (5 + 2)"), std::string("(2 + (1 * (5 + 2)))")),
        std::make_pair(std::string("n"), std::string("MG_SYMBOL_NODE")),
        std::make_pair(std::string("n.property1"), std::string("MG_SYMBOL_NODE.property1")),
        std::make_pair(std::string("n.property1 > 3"), std::string("(MG_SYMBOL_NODE.property1 > 3)")),
        std::make_pair(std::string("n.property1 != n.property2"),
                       std::string("(MG_SYMBOL_NODE.property1 != MG_SYMBOL_NODE.property2)")),
        std::make_pair(std::string("n And n"), std::string("(MG_SYMBOL_NODE And MG_SYMBOL_NODE)")),
        std::make_pair(std::string("n.property1 > 3 And n.property + 7 < 10"),
                       std::string("((MG_SYMBOL_NODE.property1 > 3) And ((MG_SYMBOL_NODE.property + 7) < 10))")),
        std::make_pair(std::string("MG_SYMBOL_NODE.property1 > 3 And (MG_SYMBOL_NODE.property + 7 < 10 Or "
                                   "MG_SYMBOL_NODE.property3 = true)"),
                       std::string("((MG_SYMBOL_NODE.property1 > 3) And (((MG_SYMBOL_NODE.property + 7) < 10) Or "
                                   "(MG_SYMBOL_NODE.property3 = true)))")),
        std::make_pair(std::string("(MG_SYMBOL_NODE.property1 > 3 Or MG_SYMBOL_NODE.property + 7 < 10) And "
                                   "MG_SYMBOL_NODE.property3 = true"),
                       std::string("(((MG_SYMBOL_NODE.property1 > 3) Or ((MG_SYMBOL_NODE.property + 7) < 10)) And "
                                   "(MG_SYMBOL_NODE.property3 = true))"))));

}  // namespace memgraph::query::v2::test
