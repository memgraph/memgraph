// Copyright 2026 Memgraph Ltd.
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

#include <cstddef>
#include <memory>
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
    // Two-stage parsing. SLL prediction ignores the parser context, so an ambiguity is resolved without the
    // full-context simulation LL runs for it. The grammar has no semantic predicates or actions, so SLL either
    // returns the tree LL would or fails. Only a failure is parsed again with LL, which also words the syntax error.
    parser_.removeErrorListeners();
    parser_.addErrorListener(&full_context_counter_);
    tree_ = ParseSLL();
    if (!tree_) tree_ = ParseLL();
  }

  auto tree() { return tree_; }

  /// How many decisions needed full-context prediction. Only the LL pass makes one; zero means SLL parsed alone.
  size_t FullContextPredictions() const { return full_context_counter_.count_; }

 private:
  /// Returns nullptr when SLL cannot parse the query.
  antlr4::tree::ParseTree *ParseSLL() {
    parser_.getInterpreter<antlr4::atn::ParserATNSimulator>()->setPredictionMode(antlr4::atn::PredictionMode::SLL);
    parser_.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
    try {
      return parser_.cypher();
    } catch (const antlr4::ParseCancellationException &) {
      return nullptr;
    }
  }

  antlr4::tree::ParseTree *ParseLL() {
    parser_.reset();
    parser_.getInterpreter<antlr4::atn::ParserATNSimulator>()->setPredictionMode(antlr4::atn::PredictionMode::LL);
    parser_.setErrorHandler(std::make_shared<antlr4::DefaultErrorStrategy>());
    parser_.addErrorListener(&error_listener_);
    auto *tree = parser_.cypher();
    if (parser_.getNumberOfSyntaxErrors()) {
      throw query::SyntaxException(error_listener_.error_);
    }
    return tree;
  }

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

  class FullContextCounter : public antlr4::BaseErrorListener {
   public:
    void reportAttemptingFullContext(antlr4::Parser * /* unused */, const antlr4::dfa::DFA & /* unused */,
                                     size_t /* unused */, size_t /* unused */, const antlrcpp::BitSet & /* unused */,
                                     antlr4::atn::ATNConfigSet * /* unused */) override {
      ++count_;
    }

    size_t count_{0};
  };

  std::string query_;
  FirstMessageErrorListener error_listener_{query_};
  FullContextCounter full_context_counter_;
  antlr4::ANTLRInputStream input_{query_};
  antlropencypher::MemgraphCypherLexer lexer_{&input_};
  antlr4::CommonTokenStream tokens_{&lexer_};

  // generate ast
  antlropencypher::MemgraphCypher parser_{&tokens_};
  antlr4::tree::ParseTree *tree_ = nullptr;
};
}  // namespace memgraph::query::frontend::opencypher
