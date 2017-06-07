#include "query/stripped.hpp"

#include <iostream>
#include <string>
#include <vector>

#include "antlr4-runtime.h"
#include "logging/loggable.hpp"
#include "query/common.hpp"
#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"
#include "query/frontend/opencypher/generated/CypherLexer.h"
#include "query/frontend/opencypher/generated/CypherParser.h"
#include "utils/assert.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/string.hpp"

using namespace antlropencypher;
using namespace antlr4;

namespace query {

StrippedQuery::StrippedQuery(const std::string &query)
    : Loggable("StrippedQuery") {
  // Tokenize the query.
  ANTLRInputStream input(query);
  CypherLexer lexer(&input);
  CommonTokenStream token_stream(&lexer);
  token_stream.fill();
  auto tokens = token_stream.getTokens();

  // Initialize data structures we return.
  std::vector<std::string> token_strings;
  token_strings.reserve(tokens.size());

  // A helper function that generates a new param name for the stripped
  // literal, appends is to the the stripped_query and adds the passed
  // value to stripped args.
  auto replace_stripped = [this, &token_strings](const TypedValue &value) {
    const auto &stripped_name = parameters_.Add(value);
    token_strings.push_back("$" + stripped_name);
  };

  // Convert tokens to strings, perform lowercasing and filtering.
  for (const auto *token : tokens) {
    switch (token->getType()) {
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
        token_strings.push_back(utils::ToLowerCase(token->getText()));
        break;

      case CypherLexer::SP:
      case Token::EOF:
        break;

      case CypherLexer::DecimalInteger:
      case CypherLexer::HexInteger:
      case CypherLexer::OctalInteger:
        replace_stripped(ParseIntegerLiteral(token->getText()));
        break;

      case CypherLexer::StringLiteral:
        replace_stripped(ParseStringLiteral(token->getText()));
        break;

      case CypherLexer::RegularDecimalReal:
      case CypherLexer::ExponentDecimalReal:
        replace_stripped(ParseDoubleLiteral(token->getText()));
        break;
      case CypherLexer::TRUE:
        replace_stripped(true);
        break;
      case CypherLexer::FALSE:
        replace_stripped(false);
        break;

      default:
        token_strings.push_back(token->getText());
        break;
    }
  }

  query_ = utils::Join(token_strings, " ");
  hash_ = fnv(query_);
  logger.info("stripped_query = {}", query_);
  logger.info("query_hash = {}", hash_);
}
}
