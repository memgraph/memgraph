// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "glue/query_user.hpp"

#include "glue/auth_checker.hpp"

namespace memgraph::glue {

bool QueryUserOrRole::IsAuthorized(const std::vector<query::AuthQuery::Privilege> &privileges, std::string_view db_name,
                                   query::UserPolicy *policy) const {
  auto locked_auth = auth_->Lock();
  // Check policy and update if behind (and policy permits it)
  if (policy->DoUpdate() && !locked_auth->UpToDate(auth_epoch_)) {
    if (user_) user_ = locked_auth->GetUser(user_->username());
    if (roles_) {
      // For backward compatibility, update the first role
      auto rolenames = roles_->rolenames();
      if (!rolenames.empty()) {
        auto role = locked_auth->GetRole(rolenames[0]);
        if (role) {
          roles_ = auth::Roles({*role});
        }
      }
    }
  }

  if (user_) return AuthChecker::IsUserAuthorized(*user_, privileges, db_name);
  if (roles_) return AuthChecker::IsRoleAuthorized(*roles_, privileges, db_name);

  return !policy->DoUpdate() || !locked_auth->AccessControlled();
}

#ifdef MG_ENTERPRISE
bool QueryUserOrRole::CanImpersonate(const std::string &target, query::UserPolicy *policy) const {
  auto locked_auth = auth_->Lock();
  // Check policy and update if behind (and policy permits it)
  if (policy->DoUpdate() && !locked_auth->UpToDate(auth_epoch_)) {
    if (user_) user_ = locked_auth->GetUser(user_->username());
    if (roles_) {
      // For backward compatibility, update the first role
      auto rolenames = roles_->rolenames();
      if (!rolenames.empty()) {
        auto role = locked_auth->GetRole(rolenames[0]);
        if (role) {
          roles_ = auth::Roles({*role});
        }
      }
    }
  }

  auto user_to_impersonate = locked_auth->GetUser(target);
  if (!user_to_impersonate) {
    return false;
  }

  if (user_) return AuthChecker::CanImpersonate(*user_, *user_to_impersonate);
  if (roles_) return AuthChecker::CanImpersonate(*roles_, *user_to_impersonate);
  return false;
}

std::string QueryUserOrRole::GetDefaultDB() const {
  if (user_) return user_->GetMain();
  if (roles_) return roles_->GetMain();
  return std::string{dbms::kDefaultDB};
}
#endif

QueryUserOrRole::QueryUserOrRole(auth::SynchedAuth *auth, auth::UserOrRole user_or_role)
    : query::QueryUserOrRole{std::visit(
                                 utils::Overloaded{[](const auto &user_or_role) { return user_or_role.username(); }},
                                 user_or_role),
                             std::visit(
                                 utils::Overloaded{[&](const auth::User &) -> std::vector<std::string> { return {}; },
                                                   [&](const auth::Roles &roles) -> std::vector<std::string> {
                                                     return roles.rolenames();
                                                   }},
                                 user_or_role)},
      auth_{auth} {
  std::visit(utils::Overloaded{[&](auth::User &&user) { user_.emplace(std::move(user)); },
                               [&](auth::RoleWUsername &&roles) { roles_.emplace(std::move(roles)); }},
             std::move(user_or_role));
}

}  // namespace memgraph::glue
