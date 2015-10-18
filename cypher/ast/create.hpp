#ifndef MEMGRAPH_CYPHER_AST_CREATE_HPP
#define MEMGRAPH_CYPHER_AST_CREATE_HPP

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

#endif
