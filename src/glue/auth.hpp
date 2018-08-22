#include "auth/models.hpp"
#include "query/frontend/ast/ast.hpp"

namespace glue {

/**
 * This function converts query::AuthQuery::Privilege to its corresponding
 * auth::Permission.
 */
auth::Permission PrivilegeToPermission(query::AuthQuery::Privilege privilege);

}  // namespace glue
