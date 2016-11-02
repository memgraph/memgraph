#pragma once

#include "ast_node.hpp"
#include "identifier.hpp"

namespace ast
{

struct Delete : public AstNode<Delete>
{
    Delete(Identifier* identifier, bool is_detached = false)
        : identifier(identifier), is_detached(is_detached) {}
    
    Identifier* identifier;
    bool is_detached;
};

}
