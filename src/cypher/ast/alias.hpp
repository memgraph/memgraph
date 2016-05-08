#pragma once

#include <string>

#include "ast_node.hpp"

namespace ast
{

// TODO: set identifier as base class
struct Alias : public AstNode<Alias>
{
    Alias(std::string name, std::string alias)
        : name(name), alias(alias) {}

    std::string name;
    std::string alias;
};

}
