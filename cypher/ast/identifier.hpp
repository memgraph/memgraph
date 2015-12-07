#pragma once

#include <string>

#include "ast_node.hpp"

namespace ast
{

struct Identifier : public AstNode<Identifier>
{
    Identifier(std::string name) : name(name) {}
    std::string name;
};

}
