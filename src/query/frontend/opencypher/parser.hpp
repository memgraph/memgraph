#pragma once

#include <string>

#include "antlr4-runtime.h"
#include "query/frontend/opencypher/generated/CypherLexer.h"
#include "query/frontend/opencypher/generated/CypherParser.h"

namespace frontend {
namespace opencypher {

using namespace antlrcpptest;
using namespace antlr4;

/**
 * Generates openCypher AST
 */
class Parser {
 public:
  /**
   * @param query incomming query that has to be compiled into query plan
   *        the first step is to generate AST
   */
  auto generate_ast(const std::string &query) {
    // get tokens
    ANTLRInputStream input(query.c_str());
    CypherLexer lexer(&input);
    CommonTokenStream tokens(&lexer);

    // generate ast
    CypherParser parser(&tokens);
    tree::ParseTree *tree = parser.cypher();

    return tree;
  }
};
}
}
