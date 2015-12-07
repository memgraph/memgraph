#pragma once

#include "ast_node.hpp"
#include "queries.hpp"

namespace ast
{

struct Start : public AstNode<Start>
{
    Start(ReadQuery* read_query, WriteQuery* write_query)
        : read_query(read_query), write_query(write_query) {}

    ReadQuery* read_query;
    WriteQuery* write_query;
    // ReadWriteQuery* read_write_query;
};

};
