// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <string>

#include "antlr4-runtime.h"
#include "query/exceptions.hpp"
#include "query/frontend/opencypher/generated/MemgraphCypher.h"
#include "query/frontend/opencypher/generated/MemgraphCypherLexer.h"

namespace memgraph::query::frontend::opencypher {

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
  explicit Parser(const std::string query) : query_(std::move(query)) {
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
    void syntaxError(antlr4::Recognizer *, antlr4::Token *, size_t line, size_t position, const std::string &message,
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
  antlr4::ANTLRInputStream input_{query_};
  antlropencypher::MemgraphCypherLexer lexer_{&input_};
  antlr4::CommonTokenStream tokens_{&lexer_};

  // generate ast
  antlropencypher::MemgraphCypher parser_{&tokens_};
  antlr4::tree::ParseTree *tree_ = nullptr;
};
}  // namespace memgraph::query::frontend::opencypher
