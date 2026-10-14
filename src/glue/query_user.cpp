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

#include <spdlog/spdlog.h>

#include "auth/exceptions.hpp"
#include "glue/auth_checker.hpp"

namespace memgraph::glue {

void QueryUserOrRole::RefreshPrincipal(const auth::Auth &auth) const {
  if (dropped_) return;  // latched: a same-named user created later must not revive the session
  if (user_) {
    auto user = auth.GetUser(user_->username());
    // A same-named user with a different uuid is a different identity: treat as dropped.
    if (!user || user->uuid() != user_->uuid()) {
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

void QueryUserOrRole::EnsureFresh() const {
  if (epoch_src_->load(std::memory_order_acquire) == auth_epoch_) return;
  auto locked = auth_->ReadLock();
  auto const e = locked->CurrentEpoch();
  RefreshPrincipal(*locked);
  access_controlled_ = locked->AccessControlled();
  auth_epoch_ = e;  // only after success: a failed refresh is retried on the next check
}

bool QueryUserOrRole::IsAuthorized(const std::vector<query::AuthQuery::Privilege> &privileges,
                                   std::optional<std::string_view> db_name) const {
  try {
    EnsureFresh();
  } catch (const auth::AuthException &e) {
    spdlog::warn("Denying access: failed to refresh the principal's permissions: {}", e.what());
    return false;
  }
  if (dropped_) return !access_controlled_;  // auth off: same access as a fresh connection

  if (user_) return AuthChecker::IsUserAuthorized(*user_, privileges, db_name);
  if (roles_) return AuthChecker::IsRoleAuthorized(*roles_, privileges, db_name);

  return !access_controlled_;
}

std::shared_ptr<query::QueryUserOrRole> QueryUserOrRole::clone() const {
  return std::make_shared<QueryUserOrRole>(*this);
}

uint64_t QueryUserOrRole::PrincipalVersion() const {
  EnsureFresh();
  return auth_epoch_;
}

std::vector<std::string> QueryUserOrRole::GetRolenames(std::optional<std::string> db_name) const {
  EnsureFresh();  // a failed refresh throws (retryable TransientError) rather than reporting stale roles
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
bool QueryUserOrRole::CanImpersonate(const std::string &target, std::optional<std::string_view> db_name) const {
  try {
    EnsureFresh();
  } catch (const auth::AuthException &e) {
    spdlog::warn("Denying impersonation: failed to refresh the principal's permissions: {}", e.what());
    return false;
  }
  if (dropped_) return false;

  if (imp_ok_ && imp_ok_->epoch == auth_epoch_ && imp_ok_->target == target &&
      imp_ok_->db_name.has_value() == db_name.has_value() &&
      (!db_name || std::string_view{*imp_ok_->db_name} == *db_name)) {
    return true;
  }

  auto locked_auth = auth_->ReadLock();
  auto user_to_impersonate = locked_auth->GetUser(target);
  bool allowed = false;
  if (user_to_impersonate) {
    if (user_) {
      allowed = AuthChecker::CanImpersonate(*user_, *user_to_impersonate, db_name);
    } else if (roles_) {
      allowed = AuthChecker::CanImpersonate(*roles_, *user_to_impersonate, db_name);
    }
  }
  // Cache only when the principal and the target were read at the same epoch.
  if (allowed && locked_auth->CurrentEpoch() == auth_epoch_) {
    imp_ok_ = ImpersonationGrant{.target = target,
                                 .db_name = db_name ? std::optional<std::string>{std::string{*db_name}} : std::nullopt,
                                 .epoch = auth_epoch_};
  } else {
    imp_ok_.reset();
  }
  return allowed;
}

std::string QueryUserOrRole::GetDefaultDB() const {
  EnsureFresh();
  if (user_) return user_->GetMain();
  if (roles_) return roles_->GetMain();
  return std::string{dbms::kDefaultDB};
}
#endif

QueryUserOrRole::QueryUserOrRole(auth::SynchedAuth *auth)
    : query::QueryUserOrRole{{}, {}}, auth_{auth}, epoch_src_{&auth->ReadLock()->EpochSource()} {}

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
      auth_{auth},
      epoch_src_{&auth->ReadLock()->EpochSource()} {
  std::visit(utils::Overloaded{[&](auth::User &&user) { user_.emplace(std::move(user)); },
                               [&](auth::RoleWUsername &&roles) { roles_.emplace(std::move(roles)); }},
             std::move(user_or_role));
}

}  // namespace memgraph::glue
