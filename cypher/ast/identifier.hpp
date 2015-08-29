#ifndef MEMGRAPH_CYPHER_AST_AST_IDENTIFIER_HPP
#define MEMGRAPH_CYPHER_AST_AST_IDENTIFIER_HPP

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

#endif
