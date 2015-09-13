#ifndef MEMGRAPH_CYPHER_AST_START_HPP
#define MEMGRAPH_CYPHER_AST_START_HPP

#include "ast_node.hpp"
#include "queries.hpp"

namespace ast
{

struct Start : public AstNode<Start>
{
    Start(ReadQuery* read_query)
        : read_query(read_query) {}

    ReadQuery* read_query;
};

};

#endif
