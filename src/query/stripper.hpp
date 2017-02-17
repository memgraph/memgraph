#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "logging/loggable.hpp"
#include "query/stripped.hpp"
#include "storage/typed_value_store.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/string/transform.hpp"
#include "utils/variadic/variadic.hpp"

#include "antlr4-runtime.h"
#include "query/frontend/opencypher/generated/CypherLexer.h"
#include "query/frontend/opencypher/generated/CypherParser.h"

using namespace antlr4;
using namespace antlrcpptest;

/**
 * @brief QueryStripper
 *
 * Strips the input query and returns stripped query, stripped arguments and
 * stripped query hash.
 */
class QueryStripper : public Loggable {
 public:
  QueryStripper() : Loggable("QueryStripper") {}

  QueryStripper(const QueryStripper &) = delete;
  QueryStripper(QueryStripper &&) = delete;
  QueryStripper &operator=(const QueryStripper &) = delete;
  QueryStripper &operator=(QueryStripper &&) = delete;

  /**
   * Strips the input query (openCypher query).
   *
   * @param query input query
   * @param separator char that is added between all literals in the stripped query
   *        because after stripping compiler frontend will use the stripped query to
   *        AST and literals have to be separated
   * @return stripped query, stripped arguments and stripped query hash as a
   *        single object of class StrippedQuery
   */
  auto strip(const std::string &query, const std::string &separator = " ") {
    // generate tokens
    ANTLRInputStream input(query);
    CypherLexer lexer(&input);
    CommonTokenStream tokens(&lexer);

    // initialize data structures that will be populated
    TypedValueStore<> stripped_arguments;
    std::string stripped_query;
    stripped_query.reserve(query.size());

    // iterate through tokens -> take arguments and generate stripped query
    tokens.fill();
    int counter = 0;
    for (auto token : tokens.getTokens()) {
      int type = token->getType();
      if (type == CypherLexer::DecimalInteger ||
          type == CypherLexer::StringLiteral ||
          type == CypherLexer::RegularDecimalReal) {  // TODO: add others
        switch (type) {
          case CypherLexer::DecimalInteger:
            stripped_arguments.set(counter, std::stoi(token->getText()));
            break;
          case CypherLexer::StringLiteral:
            stripped_arguments.set(counter, token->getText());
            break;
          case CypherLexer::RegularDecimalReal:
            stripped_arguments.set(counter, std::stof(token->getText()));
            break;
        }
        stripped_query += std::to_string(counter++) + separator;

      } else if (type == CypherLexer::MATCH || type == CypherLexer::CREATE ||
                 type == CypherLexer::RETURN || type == CypherLexer::DELETE ||
                 type == CypherLexer::SET ||
                 type == CypherLexer::WHERE) {  // TODO: add others
        auto value = token->getText();
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        stripped_query += value + separator;
      } else if (type == CypherLexer::SP || type < 0 ||
                 type > 113) {  // SP || EOF // TODO: add others if exist
        // pass
      } else {
        stripped_query += token->getText() + separator;
      }
    }

    // calculate query hash
    HashType hash = fnv(stripped_query);

    // return stripped query, stripped arguments and stripped query hash
    return StrippedQuery(std::move(stripped_query),
                         std::move(stripped_arguments), hash);
  }
};
