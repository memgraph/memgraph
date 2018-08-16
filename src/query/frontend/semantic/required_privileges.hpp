#pragma once

#include "query/frontend/ast/ast.hpp"

namespace query {
std::vector<AuthQuery::Privilege> GetRequiredPrivileges(
    const AstStorage &ast_storage);
}
