#pragma once

#include <string>

#include "ast_node.hpp"

namespace ast
{

struct Identifier : public AstNode<Identifier>
{
    Identifier(std::string name, std::string alias)
        : name(name), alias(alias) {}

    std::string name;
    std::string alias;
};

}
