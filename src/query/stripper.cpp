//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 07.03.17.
//
#include <iostream>
#include <string>
#include <vector>

#include "logging/loggable.hpp"
#include "query/stripper.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/assert.hpp"

#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"
#include "query/frontend/opencypher/generated/CypherLexer.h"
#include "query/frontend/opencypher/generated/CypherParser.h"

using namespace antlr4;
using namespace antlropencypher;

namespace query {

/**
 * A visitor that for each literal that is not enclosed
 * by a range literal calls the given callback (which should
 * then replace Tokens of literals with placeholders).
 */
class StripperVisitor : public antlropencypher::CypherBaseVisitor {
public:
  /**
   * @param callback Callback function (see class description) called
   *    with start and stop tokens of a literal.
   */
  StripperVisitor(const std::function<void(Token *, Token *)> callback)
      : callback_(callback) {}

  antlrcpp::Any visitRangeLiteral(
      CypherParser::RangeLiteralContext *ctx) override {
    is_in_range_ = true;
    auto r_val = visitChildren(ctx);
    is_in_range_ = false;
    return r_val;
  }

  antlrcpp::Any visitLiteral(
      CypherParser::LiteralContext *ctx) override {
    if (ctx->booleanLiteral() != nullptr ||
        ctx->StringLiteral() != nullptr ||
        ctx->numberLiteral() != nullptr)
      callback_(ctx->getStart(), ctx->getStop());

    is_in_literal_ = true;
    auto r_val = visitChildren(ctx);
    is_in_literal_ = false;
    return r_val;
  }

  antlrcpp::Any visitIntegerLiteral(
      CypherParser::IntegerLiteralContext *ctx) override {
    // convert integer literals into param placeholders only if not in range
    // literal
    if (!is_in_range_ && !is_in_literal_) callback_(ctx->getStart(), ctx->getStop());
    return visitChildren(ctx);
  }

private:
  const std::function<void(Token *, Token *)> callback_;
  bool is_in_range_{false};
  bool is_in_literal_{false};
};

/**
 * Strips the input query and returns stripped query, stripped arguments and
 * stripped query hash.
 *
 * @param query input query
 * @return stripped query, stripped arguments and stripped query hash as a
 *        single object of class StrippedQuery
 */
StrippedQuery Strip(const std::string &query) {

  // tokenize the query
  ANTLRInputStream input(query);
  CypherLexer lexer(&input);
  CommonTokenStream token_stream(&lexer);
  token_stream.fill();
  std::vector<Token *> tokens = token_stream.getTokens();

  // initialize data structures we return
  Parameters stripped_arguments;

  // convert tokens to strings, perform lowercasing and filtering
  std::vector<std::string> token_strings;
  token_strings.reserve(tokens.size());
  for (int i = 0; i < tokens.size(); ++i)
    switch (tokens[i]->getType()) {
      case CypherLexer::UNION:
      case CypherLexer::ALL:
      case CypherLexer::OPTIONAL:
      case CypherLexer::MATCH:
      case CypherLexer::UNWIND:
      case CypherLexer::AS:
      case CypherLexer::MERGE:
      case CypherLexer::ON:
      case CypherLexer::CREATE:
      case CypherLexer::SET:
      case CypherLexer::DETACH:
      case CypherLexer::DELETE:
      case CypherLexer::REMOVE:
      case CypherLexer::WITH:
      case CypherLexer::DISTINCT:
      case CypherLexer::RETURN:
      case CypherLexer::ORDER:
      case CypherLexer::BY:
      case CypherLexer::L_SKIP:
      case CypherLexer::LIMIT:
      case CypherLexer::ASCENDING:
      case CypherLexer::ASC:
      case CypherLexer::DESCENDING:
      case CypherLexer::DESC:
      case CypherLexer::WHERE:
      case CypherLexer::OR:
      case CypherLexer::XOR:
      case CypherLexer::AND:
      case CypherLexer::NOT:
      case CypherLexer::IN:
      case CypherLexer::STARTS:
      case CypherLexer::ENDS:
      case CypherLexer::CONTAINS:
      case CypherLexer::IS:
      case CypherLexer::CYPHERNULL:
      case CypherLexer::COUNT:
      case CypherLexer::FILTER:
      case CypherLexer::EXTRACT:
      case CypherLexer::ANY:
      case CypherLexer::NONE:
      case CypherLexer::SINGLE:
        token_strings.push_back(tokens[i]->getText());
        std::transform(token_strings.back().begin(),
                       token_strings.back().end(),
                       token_strings.back().begin(), ::tolower);
        break;

      case CypherLexer::SP:
      case Token::EOF:
        token_strings.push_back("");
        break;

      default:
        token_strings.push_back(tokens[i]->getText());
        break;
    }

  // a helper function that generates a new param name for the stripped
  // literal, appends is to the the stripped_query and adds the passed
  // value to stripped args
  auto replace_stripped = [&stripped_arguments, &token_strings](
      const TypedValue &value, size_t token_position) {
    const auto &stripped_name = stripped_arguments.Add(value);
    token_strings[token_position] = "$" + stripped_name;
  };

  // callback for every literal that should be changed
  // TODO consider literal parsing problems (like an int with 100 digits)
  auto callback = [&replace_stripped](Token *start, Token *end) {
    assert(start->getTokenIndex() == end->getTokenIndex());
    switch (start->getType()) {
      case CypherLexer::DecimalInteger:
        replace_stripped(std::stoi(start->getText()),
                         start->getTokenIndex());
        break;
      case CypherLexer::HexInteger:
        replace_stripped(std::stoi(start->getText(), 0, 16),
                         start->getTokenIndex());
        break;
      case CypherLexer::OctalInteger:
        replace_stripped(std::stoi(start->getText(), 0, 8),
                         start->getTokenIndex());
        break;
      case CypherLexer::StringLiteral:
        replace_stripped(start->getText(),
                         start->getTokenIndex());
        break;
      case CypherLexer::RegularDecimalReal:
      case CypherLexer::ExponentDecimalReal:
        replace_stripped(std::stof(start->getText()),
                         start->getTokenIndex());
        break;
      case CypherLexer::TRUE:
        replace_stripped(true, start->getTokenIndex());
        break;
      case CypherLexer::FALSE:
        replace_stripped(false, start->getTokenIndex());
        break;

      default:
        permanent_assert(true, "Unsupported literal type");
    }
  };

  // parse the query and visit the AST with a stripping visitor
  CypherParser parser(&token_stream);
  tree::ParseTree *tree = parser.cypher();
  StripperVisitor stripper_visitor(callback);
  stripper_visitor.visit(tree);

  // concatenate the stripped query tokens
  std::string stripped_query;
  stripped_query.reserve(query.size());
  for (const std::string &token_string : token_strings) {
    stripped_query += token_string;
    if (token_string.size() > 0)
      stripped_query += " ";
  }

  // return stripped query, stripped arguments and stripped query hash
  return StrippedQuery(std::move(stripped_query),
                       std::move(stripped_arguments),
                       fnv(stripped_query));
}
};
