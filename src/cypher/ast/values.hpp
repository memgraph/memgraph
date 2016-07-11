#pragma once

#include <string>

#include "expr.hpp"

namespace ast
{

struct Float : public LeafExpr<double, Float>
{
    using LeafExpr::LeafExpr;
};

struct Integer : public LeafExpr<int, Integer>
{
    using LeafExpr::LeafExpr;
};

struct Long : public LeafExpr<int64_t, Long>
{
    using LeafExpr::LeafExpr;
};

struct ULong : public LeafExpr<uint64_t, ULong>
{
    using LeafExpr::LeafExpr;
};

struct Boolean : public LeafExpr<bool, Boolean>
{
    using LeafExpr::LeafExpr;
};

struct String : public LeafExpr<std::string, String>
{
    using LeafExpr::LeafExpr;
};

struct InternalIdExpr : public Expr
{
    InternalIdExpr(Identifier *identifier, Integer *value)
        : identifier(identifier), value(value)
    {
    }

    Identifier *identifier;
    Integer *value;

    virtual void accept(AstVisitor &visitor) { visitor.visit(*this); }
};

}
