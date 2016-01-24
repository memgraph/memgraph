#pragma once

#include "ast_node.hpp"
#include "identifier.hpp"

namespace ast
{

struct Delete : public AstNode<Delete>
{
    Delete(Identifier* identifier)
        : identifier(identifier) {}

    Identifier* identifier;
};

}
