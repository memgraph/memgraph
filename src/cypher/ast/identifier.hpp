#pragma once

#include <string>

#include "ast_node.hpp"
#include "list.hpp"

namespace ast
{

struct Identifier : public AstNode<Identifier>
{
    Identifier(std::string name)
        : name(name) {}

    std::string name;
};

struct IdentifierList : public List<Identifier, IdentifierList>
{
    using List::List;
};

}
