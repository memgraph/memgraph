#pragma once

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
