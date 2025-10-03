// Copyright 2025 Memgraph Ltd.
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

#include <cstdio>          // Ensure EOF macro is defined
#pragma push_macro("EOF")  // hide EOF for antlr headers
#include "antlr4-runtime/antlr4-runtime.h"
#include "query/exceptions.hpp"
#include "query/frontend/opencypher/generated/MemgraphCypher.h"
#include "query/frontend/opencypher/generated/MemgraphCypherLexer.h"
#pragma pop_macro("EOF")  // bring EOF back

#include <string>
#include <string_view>

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
  explicit Parser(std::string query) : query_(std::move(query)) {
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
   public:
    explicit FirstMessageErrorListener(const std::string &query) : query_(query) {}
    void syntaxError(antlr4::Recognizer * /* unused */, antlr4::Token *token, size_t line, size_t position,
                     const std::string &message, std::exception_ptr exception) override {
      if (error_.empty()) {
        try {
          if (exception) std::rethrow_exception(exception);
        } catch (const antlr4::NoViableAltException &ex) {
          error_ = "Error on line " + std::to_string(line) + " position " + std::to_string(position + 1) +
                   " with the " + token->getText() + " token. The underlying parsing error is " + message + "." +
                   " Take a look at clauses around and try to fix the query.";
          return;
        } catch (...) {
          // Handled below
          (void)0;
        }
        error_ = "Error on line " + std::to_string(line) + " position " + std::to_string(position + 1) +
                 ". The underlying parsing error is " + message;
      }
    }

   private:
    friend class Parser;

    std::string error_{};
    std::string_view query_;
  };

  std::string query_;
  FirstMessageErrorListener error_listener_{query_};
  antlr4::ANTLRInputStream input_{query_};
  antlropencypher::MemgraphCypherLexer lexer_{&input_};
  antlr4::CommonTokenStream tokens_{&lexer_};

  // generate ast
  antlropencypher::MemgraphCypher parser_{&tokens_};
  antlr4::tree::ParseTree *tree_ = nullptr;
};
}  // namespace memgraph::query::frontend::opencypher
