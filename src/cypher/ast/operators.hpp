#pragma once

#include "expr.hpp"

namespace ast
{

struct And : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Or : public BinaryExpr<Or>
{
    using BinaryExpr::BinaryExpr;
};

struct Lt : public BinaryExpr<Lt>
{
    using BinaryExpr::BinaryExpr;
};

struct Gt : public BinaryExpr<Gt>
{
    using BinaryExpr::BinaryExpr;
};

struct Ge : public BinaryExpr<Ge>
{
    using BinaryExpr::BinaryExpr;
};

struct Le : public BinaryExpr<Le>
{
    using BinaryExpr::BinaryExpr;
};

struct Eq : public BinaryExpr<Eq>
{
    using BinaryExpr::BinaryExpr;
};

struct Ne : public BinaryExpr<Ne>
{
    using BinaryExpr::BinaryExpr;
};

struct Plus : public BinaryExpr<Plus>
{
    using BinaryExpr::BinaryExpr;
};

struct Minus : public BinaryExpr<Minus>
{
    using BinaryExpr::BinaryExpr;
};

struct Star : public BinaryExpr<Star>
{
    using BinaryExpr::BinaryExpr;
};

struct Slash : public BinaryExpr<Slash>
{
    using BinaryExpr::BinaryExpr;
};

struct Rem : public BinaryExpr<Rem>
{
    using BinaryExpr::BinaryExpr;
};

}
