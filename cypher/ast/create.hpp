#pragma once

#include "ast_node.hpp"
#include "pattern.hpp"
#include "return.hpp"

namespace ast
{

struct Create : public AstNode<Create>
{
    Create(Pattern* pattern)
        : pattern(pattern) {}

    Pattern* pattern;
};

}
