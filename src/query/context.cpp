#include "query/context.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"

namespace query {

void HighLevelAstConversion::Apply(Context &ctx,
                                   antlr4::tree::ParseTree *tree) {
  query::frontend::CypherMainVisitor visitor(ctx);
  visitor.visit(tree);
}
}
