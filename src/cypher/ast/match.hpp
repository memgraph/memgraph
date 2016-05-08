#pragma once

#include "ast_node.hpp"
#include "pattern.hpp"
#include "where.hpp"

namespace ast
{

struct Match : public AstNode<Match>
{
    Match(Pattern* pattern, Where* where)
        : pattern(pattern), where(where) {}

    Pattern* pattern;
    Where* where;
};

}
