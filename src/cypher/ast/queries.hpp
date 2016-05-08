#pragma once

#include "ast_node.hpp"
#include "create.hpp"
#include "match.hpp"
#include "set.hpp"
#include "delete.hpp"
#include "return.hpp"

namespace ast
{

struct WriteQuery : public AstNode<WriteQuery>
{
    WriteQuery(Create* create, Return* return_clause)
        : create(create), return_clause(return_clause) {}

    Create* create;
    Return* return_clause;
};

struct ReadQuery : public AstNode<ReadQuery>
{
    ReadQuery(Match* match, Return* return_clause)
        : match(match), return_clause(return_clause) {}

    Match* match;
    Return* return_clause;
};

struct UpdateQuery : public AstNode<UpdateQuery>
{
    UpdateQuery(Match* match_clause, Set* set_clause, Return* return_clause)
        : match_clause(match_clause), set_clause(set_clause),
          return_clause(return_clause)
    {
    }

    Match* match_clause;
    Set* set_clause;
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
