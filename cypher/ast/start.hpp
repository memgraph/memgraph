#pragma once

#include "ast_node.hpp"
#include "queries.hpp"

namespace ast
{

struct Start : public AstNode<Start>
{
    Start(WriteQuery* write_query) : write_query(write_query) {}
    Start(ReadQuery* read_query) : read_query(read_query) {}
    Start(UpdateQuery* update_query) : update_query(update_query) {}
    Start(DeleteQuery* delete_query) : delete_query(delete_query) {}

    // TODO: the start structure must have a different implementation
    // is this class necessary?
    WriteQuery* write_query {nullptr};
    ReadQuery* read_query {nullptr};
    UpdateQuery* update_query {nullptr};
    DeleteQuery* delete_query {nullptr};
};

};
