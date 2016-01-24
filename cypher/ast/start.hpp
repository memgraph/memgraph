#pragma once

#include "ast_node.hpp"
#include "queries.hpp"

namespace ast
{

struct Start : public AstNode<Start>
{
    Start(ReadQuery* read_query) 
        : read_query(read_query)
    {
    }

    Start(WriteQuery* write_query) 
        : write_query(write_query)
    {
    }

    Start(DeleteQuery* delete_query) 
        : delete_query(delete_query)
    {
    }

    // TODO: the start structure must have a different implementation
    // is this class necessary?
    ReadQuery* read_query{nullptr};
    WriteQuery* write_query{nullptr};
    DeleteQuery* delete_query{nullptr};
};

};
