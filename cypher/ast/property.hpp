#pragma once

#include "list.hpp"
#include "identifier.hpp"
#include "expr.hpp"

namespace ast
{

struct Property : public AstNode<Property>
{
    Property(Identifier* idn, Expr* value)
        : idn(idn), value(value) {}

    Identifier* idn;
    Expr* value;
};

struct PropertyList : public List<Property, PropertyList>
{
    using List::List;
};

}
