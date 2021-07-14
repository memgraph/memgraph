#pragma once

#include "query/frontend/ast/ast.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

#ifdef MG_ENTERPRISE
#include "auth/auth.hpp"
#endif

namespace query {
class AuthChecker {
 public:
#ifdef MG_ENTERPRISE
  explicit AuthChecker(utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock> *auth);

  static bool IsUserAuthorized(const auth::User &user, const std::vector<query::AuthQuery::Privilege> &privileges);

#else
  AuthChecker() = default;
#endif

  bool IsUserAuthorized(const std::optional<std::string> &username,
                        const std::vector<query::AuthQuery::Privilege> &privileges) const;

 private:
#ifdef MG_ENTERPRISE
  utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock> *auth_{nullptr};
#endif
};
}  // namespace query