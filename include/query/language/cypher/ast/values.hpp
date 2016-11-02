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
    InternalIdExpr(Identifier *entity, Long *id)
        : entity(entity), id(id)
    {
    }

    Identifier *entity;
    Long *id;

    virtual void accept(AstVisitor &visitor) { visitor.visit(*this); }

    bool has_entity() const { return entity != nullptr; }
    bool has_id() const { return id != nullptr; }

    std::string entity_name() const { return entity->name; }
    int64_t entity_id() const { return id->value; }


};

}
