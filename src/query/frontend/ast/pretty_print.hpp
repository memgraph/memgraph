#pragma once

#include <iostream>

#include "query/frontend/ast/ast.hpp"

namespace query {

void PrintExpression(Expression *expr, std::ostream *out);
void PrintExpression(NamedExpression *expr, std::ostream *out);

}  // namespace query
