#pragma once

#include <string>

#include "antlr4-runtime.h"
#include "query/exceptions.hpp"
#include "query/frontend/opencypher/generated/MemgraphCypher.h"
#include "query/frontend/opencypher/generated/MemgraphCypherLexer.h"

namespace query {
namespace frontend {
namespace opencypher {

/**
 * Generates openCypher AST
 * This thing must me a class since parser.cypher() returns pointer and there is
 * no way for us to get ownership over the object.
 */
class Parser {
 public:
  /**
   * @param query incoming query that has to be compiled into query plan
   *        the first step is to generate AST
   */
  Parser(const std::string query) : query_(std::move(query)) {
    parser_.removeErrorListeners();
    parser_.addErrorListener(&error_listener_);
    tree_ = parser_.cypher();
    if (parser_.getNumberOfSyntaxErrors()) {
      throw query::SyntaxException(error_listener_.error_);
    }
  }

  auto tree() { return tree_; }

 private:
  class FirstMessageErrorListener : public antlr4::BaseErrorListener {
    void syntaxError(antlr4::IRecognizer *, antlr4::Token *, size_t line, size_t position, const std::string &message,
                     std::exception_ptr) override {
      if (error_.empty()) {
        error_ = "line " + std::to_string(line) + ":" + std::to_string(position + 1) + " " + message;
      }
    }

   public:
    std::string error_;
  };

  FirstMessageErrorListener error_listener_;
  std::string query_;
  antlr4::ANTLRInputStream input_{query_.c_str()};
  antlropencypher::MemgraphCypherLexer lexer_{&input_};
  antlr4::CommonTokenStream tokens_{&lexer_};

  // generate ast
  antlropencypher::MemgraphCypher parser_{&tokens_};
  antlr4::tree::ParseTree *tree_ = nullptr;
};
}  // namespace opencypher
}  // namespace frontend
}  // namespace query
