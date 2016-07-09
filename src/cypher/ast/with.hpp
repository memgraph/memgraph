#pragma once

#include <string>

#include "ast_node.hpp"
#include "identifier.hpp"
#include "match.hpp"

namespace ast
{

struct WithClause : public AstNode<WithClause>
{
    WithClause(IdentifierList *identifier_list, Match *match_clause)
        : identifier_list(identifier_list), match_clause(match_clause)
    {
    }

    IdentifierList *identifier_list;
    Match *match_clause;
};

struct WithList : public List<WithClause, WithList>
{
    using List::List;
};

struct WithQuery : public AstNode<WithQuery>
{
    WithQuery(Match *match_clause, WithList *with_list, Return *return_clause)
        : match_clause(match_clause), with_list(with_list),
          return_clause(return_clause)
    {
    }

    Match *match_clause;
    WithList *with_list;
    Return *return_clause;
};

}
