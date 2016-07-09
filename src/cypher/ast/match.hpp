#pragma once

#include "ast_node.hpp"
#include "pattern.hpp"
#include "where.hpp"

namespace ast
{

struct Match : public AstNode<Match>
{
    Match(PatternList* pattern_list, Where* where)
        : pattern_list(pattern_list), where(where) {}

    PatternList* pattern_list;
    Where* where;
};

}
