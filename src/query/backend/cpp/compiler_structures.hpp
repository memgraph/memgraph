#pragma once

#include <climits>
#include <unordered_map>
#include "query/frontend/opencypher/generated/CypherParser.h"
#include "utils/exceptions/basic_exception.hpp"

// TODO: Figure out what information to put in exception.
// Error reporting is tricky since we get stripped query and position of error
// in original query is not same as position of error in stripped query. Most
// correct approach would be to do semantic analysis with original query even
// for already hashed queries, but that has obvious performance issues. Other
// approach would be to report some of the semantic errors in runtime of the
// query and only report line numbers of semantic errors (not position in the
// line) if multiple line strings are not allowed by grammar. We could also
// print whole line that contains error instead of specifying line number.
class SemanticException : BasicException {
 public:
  SemanticException() : BasicException("") {}
};

// enum VariableType { TYPED_VALUE, LIST, MAP, NODE, RELATIONSHIP, PATH };

struct Node {
  std::string output_identifier;
  std::vector<std::string> labels;
  std::unordered_map<std::string,
                     antlropencypher::CypherParser::ExpressionContext*>
      properties;
};

struct Relationship {
  enum Direction { LEFT, RIGHT, BOTH };
  std::string output_identifier;
  Direction direction = Direction::BOTH;
  std::vector<std::string> types;
  std::unordered_map<std::string,
                     antlropencypher::CypherParser::ExpressionContext*>
      properties;
  bool has_range = false;
  // If has_range is false, lower and upper bound values are not important.
  // lower_bound can be larger than upper_bound and in that case there is no
  // results.
  int64_t lower_bound = 1LL;
  int64_t upper_bound = LLONG_MAX;
};

struct PatternPart {
  std::string output_identifier;
  std::vector<Node> nodes;
  std::vector<Relationship> relationships;
};
