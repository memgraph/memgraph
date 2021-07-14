#include "query/auth_checker.hpp"
#include <algorithm>

#ifdef MG_ENTERPRISE
#include "glue/auth.hpp"
#endif

namespace query {

#ifdef MG_ENTERPRISE
AuthChecker::AuthChecker(utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock> *auth) : auth_{auth} {}

bool AuthChecker::IsUserAuthorized(const auth::User &user, const std::vector<query::AuthQuery::Privilege> &privileges) {
  const auto user_permissions = user.GetPermissions();
  return std::all_of(privileges.begin(), privileges.end(), [&user_permissions](const auto privilege) {
    return user_permissions.Has(glue::PrivilegeToPermission(privilege)) == auth::PermissionLevel::GRANT;
  });
}
#endif

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool AuthChecker::IsUserAuthorized(const std::optional<std::string> &username,
                                   const std::vector<query::AuthQuery::Privilege> &privileges) const {
#ifdef MG_ENTERPRISE
  std::optional<auth::User> maybe_user;
  {
    auto locked_auth = auth_->ReadLock();
    if (!locked_auth->HasUsers()) {
      return true;
    }
    if (username.has_value()) {
      maybe_user = locked_auth->GetUser(*username);
    }
  }

  return maybe_user.has_value() && IsUserAuthorized(*maybe_user, privileges);
#else
  return true;
#endif
}
}  // namespace query