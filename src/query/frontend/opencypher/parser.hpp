#pragma once

#include <string>

#include "antlr4-runtime.h"
#include "query/exceptions.hpp"
#include "query/frontend/opencypher/generated/CypherLexer.h"
#include "query/frontend/opencypher/generated/CypherParser.h"

namespace frontend {
namespace opencypher {

using namespace antlropencypher;
using namespace antlr4;

/**
 * Generates openCypher AST
 * This thing must me a class since parser.cypher() returns pointer and there is
 * no way for us to get ownership over the object.
 */
class Parser {
 public:
  /**
   * @param query incomming query that has to be compiled into query plan
   *        the first step is to generate AST
   */
  Parser(const std::string query) : query_(std::move(query)) {
    if (parser_.getNumberOfSyntaxErrors()) {
      throw query::SyntaxException();
    }
  }

  auto tree() { return tree_; }

 private:
  std::string query_;
  ANTLRInputStream input_{query_.c_str()};
  CypherLexer lexer_{&input_};
  CommonTokenStream tokens_{&lexer_};

  // generate ast
  CypherParser parser_{&tokens_};
  tree::ParseTree *tree_{parser_.cypher()};
};
}
}
