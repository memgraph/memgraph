#pragma once

#include "expr.hpp"

namespace ast
{

struct CountFunction : public FunctionExpr<std::string, CountFunction>
{
    CountFunction(const std::string &argument) : FunctionExpr("count", argument)
    {
    }
};
}
