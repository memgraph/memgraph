#pragma once

#include "query/frontend/ast/ast.hpp"

namespace query {
std::vector<AuthQuery::Privilege> GetRequiredPrivileges(Query *query);
}
