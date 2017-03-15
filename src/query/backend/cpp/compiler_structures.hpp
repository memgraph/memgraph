#pragma once

#include <climits>
#include <unordered_map>
#include "query/exceptions.hpp"
#include "query/frontend/opencypher/generated/CypherParser.h"

namespace backend {
namespace cpp {

// enum VariableType { TYPED_VALUE, LIST, MAP, NODE, RELATIONSHIP, PATH };

struct Node {
  std::string output_id;
  std::vector<std::string> labels;
  std::unordered_map<std::string, std::string> properties;
};

struct Relationship {
  enum Direction { LEFT, RIGHT, BOTH };
  std::string output_id;
  Direction direction = Direction::BOTH;
  std::vector<std::string> types;
  std::unordered_map<std::string, std::string> properties;
  bool has_range = false;
  // If has_range is false, lower and upper bound values are not important.
  // lower_bound can be larger than upper_bound and in that case there is no
  // results.
  int64_t lower_bound = 1LL;
  int64_t upper_bound = LLONG_MAX;
};

struct PatternPart {
  std::string output_id;
  std::vector<std::string> nodes;
  std::vector<std::string> relationships;
};

enum class Function {
  LOGICAL_OR,
  LOGICAL_XOR,
  LOGICAL_AND,
  LOGICAL_NOT,
  EQ,
  NE,
  LT,
  GT,
  LE,
  GE,
  ADDITION,
  SUBTRACTION,
  MULTIPLICATION,
  DIVISION,
  MODULO,
  UNARY_MINUS,
  UNARY_PLUS,
  PROPERTY_GETTER,
  LITERAL,
  PARAMETER
};

struct SimpleExpression {
  Function function;
  std::vector<std::string> arguments;
};
}
}
