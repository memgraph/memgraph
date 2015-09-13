#ifndef MEMGRAPH_CYPHER_AST_WHERE_HPP
#define MEMGRAPH_CYPHER_AST_WHERE_HPP

#include "ast_node.hpp"
#include "expr.hpp"

namespace ast
{

struct Where : public AstNode<Where>
{
    Where(Expr* expr)
        : expr(expr) {}

    Expr* expr;
};

}

#endif
