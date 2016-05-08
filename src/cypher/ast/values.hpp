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

struct Boolean : public LeafExpr<bool, Boolean>
{
    using LeafExpr::LeafExpr;
};

struct String : public LeafExpr<std::string, String>
{
    using LeafExpr::LeafExpr;
};

}
