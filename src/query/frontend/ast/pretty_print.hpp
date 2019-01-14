#pragma once

#include <iostream>

#include "query/frontend/ast/ast.hpp"

namespace query {

void PrintExpression(const AstStorage &storage, Expression *expr,
                     std::ostream *out);
void PrintExpression(const AstStorage &storage, NamedExpression *expr,
                     std::ostream *out);

}  // namespace query
