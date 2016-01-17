#pragma once

#include "ast_node.hpp"
#include "identifier.hpp"

namespace ast
{

struct Distinct : public AstNode<Distinct>
{
    Distinct(Identifier* identifier)
        : identifier(identifier) {}

    Identifier* identifier;
};

}
