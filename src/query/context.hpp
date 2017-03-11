#pragma once

#include "antlr4-runtime.h"
#include "query/frontend/ast/cypher_main_visitor.hpp"

class Context {
  int uid_counter;
};

class HighLevelAstConversion {
  void Apply(const Context &ctx, antlr4::tree::ParseTree *tree) {
    query::frontend::CypherMainVisitor visitor(ctx);
    visitor.visit(tree);
  }
};
