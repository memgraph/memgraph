#pragma once

#include "ast_node.hpp"
#include "pattern.hpp"
#include "return.hpp"

namespace ast
{

struct Merge : public AstNode<Merge>
{
    Merge(Pattern* pattern)
        : pattern(pattern) {}

    Pattern* pattern;
};

}
