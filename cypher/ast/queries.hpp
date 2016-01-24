#pragma once

#include "ast_node.hpp"
#include "match.hpp"
#include "return.hpp"
#include "create.hpp"
#include "delete.hpp"

namespace ast
{

struct ReadQuery : public AstNode<ReadQuery>
{
    ReadQuery(Match* match, Return* return_clause)
        : match(match), return_clause(return_clause) {}

    Match* match;
    Return* return_clause;
};

struct WriteQuery : public AstNode<WriteQuery>
{
    WriteQuery(Create* create, Return* return_clause)
        : create(create), return_clause(return_clause) {}

    Create* create;
    Return* return_clause;
};

struct DeleteQuery : public AstNode<DeleteQuery>
{
    DeleteQuery(Match* match, Delete* delete_clause)
        : match(match), delete_clause(delete_clause) {}

    Match* match;
    Delete* delete_clause;
};

}
