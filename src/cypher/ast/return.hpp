#pragma once

#include "list.hpp"
#include "expr.hpp"
#include "identifier.hpp"
#include "distinct.hpp"

namespace ast
{

struct ReturnList : public List<Expr, ReturnList>
{
    using List::List;
};

struct Return : public AstNode<Return>
{
    Return(ReturnList* return_list)
        : return_list(return_list), distinct(nullptr)
    {
    }

    Return(Distinct* distinct)
        : return_list(nullptr), distinct(distinct)
    {
    }

    ReturnList* return_list;
    Distinct* distinct;
};

};
