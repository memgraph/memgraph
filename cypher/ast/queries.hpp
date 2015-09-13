#ifndef MEMGRAPH_CYPHER_AST_QUERIES_HPP
#define MEMGRAPH_CYPHER_AST_QUERIES_HPP

#include "ast_node.hpp"
#include "match.hpp"
#include "return.hpp"

namespace ast
{

struct ReadQuery : public AstNode<ReadQuery>
{
    ReadQuery(Match* match, ReturnList* return_list)
        : match(match), return_list(return_list) {}

    Match* match;
    ReturnList* return_list;
};

}

#endif
