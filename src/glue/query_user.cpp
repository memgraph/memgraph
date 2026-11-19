// Copyright 2026 Memgraph Ltd.
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
#include <unordered_set>

#include "glue/auth_checker.hpp"

namespace memgraph::glue {

void QueryUserOrRole::RefreshPrincipal(const auth::Auth &auth) const {
  if (dropped_) return;  // latched: a same-named user created later must not revive the session
  if (user_) {
    auto user = auth.GetUser(user_->username());
    if (!user) {
      dropped_ = true;
      return;
    }
    user_ = std::move(*user);
  }
  if (roles_) {
    std::unordered_set<auth::Role> updated_roles;
    for (const auto &rolename : roles_->rolenames()) {
      if (auto role = auth.GetRole(rolename)) updated_roles.insert(std::move(*role));
    }
    if (updated_roles.empty()) {
      dropped_ = true;
      return;
    }
    roles_ = auth::Roles(std::move(updated_roles));
  }
}

bool QueryUserOrRole::IsAuthorized(const std::vector<query::AuthQuery::Privilege> &privileges,
                                   std::optional<std::string_view> db_name, query::UserPolicy *policy) const {
  if (internal_system_user_) return true;
  // The session-long policy only consults the cached principal; a dropped session only checks if auth is on.
  if (!policy->DoUpdate()) {
    if (dropped_) return !auth_->ReadLock()->AccessControlled();
    if (user_) return AuthChecker::IsUserAuthorized(*user_, privileges, db_name);
    if (roles_) return AuthChecker::IsRoleAuthorized(*roles_, privileges, db_name);
    return true;
  }
  auto locked_auth = auth_->ReadLock();
  if (!locked_auth->UpToDate(auth_epoch_)) RefreshPrincipal(*locked_auth);
  if (dropped_) return !locked_auth->AccessControlled();  // auth off: same access as a fresh connection

  if (user_) return AuthChecker::IsUserAuthorized(*user_, privileges, db_name);
  if (roles_) return AuthChecker::IsRoleAuthorized(*roles_, privileges, db_name);

  return !locked_auth->AccessControlled();
}

std::shared_ptr<query::QueryUserOrRole> QueryUserOrRole::clone() const {
  return std::make_shared<QueryUserOrRole>(*this);
}

std::vector<std::string> QueryUserOrRole::GetRolenames(std::optional<std::string> db_name) const {
#ifdef MG_ENTERPRISE
  std::unordered_set<auth::Role> roles;
  if (user_) roles = user_->GetRoles(db_name);
  if (roles_) roles = roles_->GetFilteredRoles(db_name);

  std::vector<std::string> rolenames;
  rolenames.reserve(roles.size());
  for (const auto &role : roles) {
    rolenames.push_back(role.rolename());
  }
  return rolenames;
#else
  if (user_) return user_->rolenames();
  if (roles_) return roles_->rolenames();
  return {};
#endif
}

#ifdef MG_ENTERPRISE
bool QueryUserOrRole::CanImpersonate(const std::string &target, query::UserPolicy *policy,
                                     std::optional<std::string_view> db_name) const {
  if (internal_system_user_) return false;
  auto locked_auth = auth_->ReadLock();
  if (policy->DoUpdate() && !locked_auth->UpToDate(auth_epoch_)) RefreshPrincipal(*locked_auth);
  if (dropped_) return false;

  auto user_to_impersonate = locked_auth->GetUser(target);
  if (!user_to_impersonate) {
    return false;
  }

  if (user_) return AuthChecker::CanImpersonate(*user_, *user_to_impersonate, db_name);
  if (roles_) return AuthChecker::CanImpersonate(*roles_, *user_to_impersonate, db_name);
  return false;
}

std::string QueryUserOrRole::GetDefaultDB() const {
  if (user_) return user_->GetMain();
  if (roles_) return roles_->GetMain();
  return std::string{dbms::kDefaultDB};
}

bool QueryUserOrRole::HasFineGrainedConfiguration() const {
  // Read straight from the stored handlers: the merged getters answer for a database, while this
  // question is about whether anything is configured at all.
  auto const configured = [](auto const &fine_grained, auto const &properties) {
    return !fine_grained.label_permissions().IsUnconfigured() ||
           !fine_grained.edge_type_permissions().IsUnconfigured() || !properties.label_properties().IsUnconfigured() ||
           !properties.edge_type_properties().IsUnconfigured();
  };

  if (user_ && configured(user_->fine_grained_access_handler(), user_->property_access_handler())) return true;
  if (roles_) {
    for (auto const &role : *roles_) {
      if (configured(role.fine_grained_access_handler(), role.property_access_handler())) return true;
    }
  }
  if (user_) {
    for (auto const &role : user_->roles()) {
      if (configured(role.fine_grained_access_handler(), role.property_access_handler())) return true;
    }
  }
  return false;
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
