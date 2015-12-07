#pragma once

#include "expr.hpp"

namespace ast
{

struct And : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Or : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Lt : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Gt : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Ge : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Le : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Eq : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Ne : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Plus : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Minus : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Star : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Slash : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

struct Rem : public BinaryExpr<And>
{
    using BinaryExpr::BinaryExpr;
};

}
