// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "glue/auth_checker.hpp"

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "glue/auth.hpp"
#include "glue/query_user.hpp"
#include "license/license.hpp"
#include "query/auth_checker.hpp"
#include "query/constants.hpp"
#include "query/frontend/ast/ast.hpp"
#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

#ifdef MG_ENTERPRISE
namespace {
bool IsUserAuthorizedLabels(const memgraph::auth::UserOrRole &user_or_role, const memgraph::query::DbAccessor *dba,
                            const std::vector<memgraph::storage::LabelId> &labels,
                            const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return std::all_of(labels.begin(), labels.end(), [dba, &user_or_role, fine_grained_privilege](const auto &label) {
    return std::visit(memgraph::utils::Overloaded{[&](auto &user_or_role) {
                        return user_or_role.GetFineGrainedAccessLabelPermissions().Has(
                                   dba->LabelToName(label), memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(
                                                                fine_grained_privilege)) ==
                               memgraph::auth::PermissionLevel::GRANT;
                      }},
                      user_or_role);
  });
}

bool IsUserAuthorizedGloballyLabels(const memgraph::auth::UserOrRole &user_or_role,
                                    const memgraph::auth::FineGrainedPermission fine_grained_permission) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return std::visit(memgraph::utils::Overloaded{[&](auto &user_or_role) {
                      return user_or_role.GetFineGrainedAccessLabelPermissions().Has(memgraph::query::kAsterisk,
                                                                                     fine_grained_permission) ==
                             memgraph::auth::PermissionLevel::GRANT;
                    }},
                    user_or_role);
}

bool IsUserAuthorizedGloballyEdges(const memgraph::auth::UserOrRole &user_or_role,
                                   const memgraph::auth::FineGrainedPermission fine_grained_permission) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return std::visit(memgraph::utils::Overloaded{[&](auto &user_or_role) {
                      return user_or_role.GetFineGrainedAccessEdgeTypePermissions().Has(memgraph::query::kAsterisk,
                                                                                        fine_grained_permission) ==
                             memgraph::auth::PermissionLevel::GRANT;
                    }},
                    user_or_role);
}

bool IsUserAuthorizedEdgeType(const memgraph::auth::UserOrRole &user_or_role, const memgraph::query::DbAccessor *dba,
                              const memgraph::storage::EdgeTypeId &edgeType,
                              const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return std::visit(memgraph::utils::Overloaded{[&](auto &user_or_role) {
                      return user_or_role.GetFineGrainedAccessEdgeTypePermissions().Has(
                                 dba->EdgeTypeToName(edgeType),
                                 memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege)) ==
                             memgraph::auth::PermissionLevel::GRANT;
                    }},
                    user_or_role);
}
}  // namespace
#endif
namespace memgraph::glue {

AuthChecker::AuthChecker(memgraph::auth::SynchedAuth *auth) : auth_(auth) {}

std::shared_ptr<query::QueryUser> AuthChecker::GenQueryUser(const std::optional<std::string> &name) const {
  if (name) {
    auto locked_auth = auth_->ReadLock();
    const auto user = locked_auth->GetUser(*name);
    if (user) return std::make_shared<QueryUser>(auth_, std::move(*user));
    const auto role = locked_auth->GetRole(*name);
    if (role) return std::make_shared<QueryUser>(auth_, std::move(*role));
  }
  // No user or role
  return std::make_shared<QueryUser>(auth_);
}

std::unique_ptr<query::QueryUser> AuthChecker::GenQueryUser(auth::SynchedAuth *auth,
                                                            const std::optional<auth::UserOrRole> &user_or_role) {
  if (user_or_role) {
    return std::visit(
        utils::Overloaded{[&](auto &user_or_role) { return std::make_unique<QueryUser>(auth, user_or_role); }},
        *user_or_role);
  }
  // No user or role
  return std::make_unique<QueryUser>(auth);
}

#ifdef MG_ENTERPRISE
std::unique_ptr<memgraph::query::FineGrainedAuthChecker> AuthChecker::GetFineGrainedAuthChecker(
    const std::string &name, const memgraph::query::DbAccessor *dba) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  try {
    auto user_or_role = user_or_role_.Lock();
    std::string current_name = std::visit(utils::Overloaded{[](auth::User &user) { return user.username(); },
                                                            [](auth::Role &role) { return role.rolename(); }},
                                          *user_or_role);
    if (name != current_name) {
      auto locked_auth = auth_->ReadLock();
      auto maybe_user = locked_auth->GetUser(name);
      if (maybe_user) {
        *user_or_role = std::move(*maybe_user);
        return std::make_unique<memgraph::glue::FineGrainedAuthChecker>(*maybe_user, dba);
      }
      auto maybe_role = locked_auth->GetRole(name);
      if (maybe_role) {
        *user_or_role = std::move(*maybe_role);
        return std::make_unique<memgraph::glue::FineGrainedAuthChecker>(*maybe_role, dba);
      }
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist .", name);
    }
    return std::visit(utils::Overloaded{[dba](auto &user_or_role) {
                        return std::make_unique<memgraph::glue::FineGrainedAuthChecker>(user_or_role, dba);
                      }},
                      *user_or_role);

  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthChecker::ClearCache() const {
  user_or_role_.WithLock([](auto &user_or_role) mutable { user_or_role = {}; });
}
#endif

bool AuthChecker::IsUserAuthorized(const memgraph::auth::User &user,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                   const std::string &db_name) {  // NOLINT
#ifdef MG_ENTERPRISE
  if (!db_name.empty() && !user.db_access().Contains(db_name)) {
    return false;
  }
#endif
  const auto user_permissions = user.GetPermissions();
  return std::all_of(privileges.begin(), privileges.end(), [&user_permissions](const auto privilege) {
    return user_permissions.Has(memgraph::glue::PrivilegeToPermission(privilege)) ==
           memgraph::auth::PermissionLevel::GRANT;
  });
}

bool AuthChecker::IsRoleAuthorized(const memgraph::auth::Role &role,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                   const std::string &db_name) {  // NOLINT
#ifdef MG_ENTERPRISE
  if (!db_name.empty() && !role.db_access().Contains(db_name)) {
    return false;
  }
#endif
  const auto user_permissions = role.permissions();
  return std::all_of(privileges.begin(), privileges.end(), [&user_permissions](const auto privilege) {
    return user_permissions.Has(memgraph::glue::PrivilegeToPermission(privilege)) ==
           memgraph::auth::PermissionLevel::GRANT;
  });
}

bool AuthChecker::IsUserOrRoleAuthorized(const memgraph::auth::UserOrRole &user_or_role,
                                         const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                         const std::string &db_name) {
  return std::visit(
      utils::Overloaded{
          [&](const auth::User &user) -> bool { return AuthChecker::IsUserAuthorized(user, privileges, db_name); },
          [&](const auth::Role &role) -> bool { return AuthChecker::IsRoleAuthorized(role, privileges, db_name); }},
      user_or_role);
}

#ifdef MG_ENTERPRISE
FineGrainedAuthChecker::FineGrainedAuthChecker(auth::User user, const memgraph::query::DbAccessor *dba)
    : user_or_role_{std::move(user)}, dba_(dba){};
FineGrainedAuthChecker::FineGrainedAuthChecker(auth::Role role, const memgraph::query::DbAccessor *dba)
    : user_or_role_{std::move(role)}, dba_(dba){};

bool FineGrainedAuthChecker::Has(const memgraph::query::VertexAccessor &vertex, const memgraph::storage::View view,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  auto maybe_labels = vertex.Labels(view);
  if (maybe_labels.HasError()) {
    switch (maybe_labels.GetError()) {
      case memgraph::storage::Error::DELETED_OBJECT:
        throw memgraph::query::QueryRuntimeException("Trying to get labels from a deleted node.");
      case memgraph::storage::Error::NONEXISTENT_OBJECT:
        throw memgraph::query::QueryRuntimeException("Trying to get labels from a node that doesn't exist.");
      case memgraph::storage::Error::SERIALIZATION_ERROR:
      case memgraph::storage::Error::VERTEX_HAS_EDGES:
      case memgraph::storage::Error::PROPERTIES_DISABLED:
        throw memgraph::query::QueryRuntimeException("Unexpected error when getting labels.");
    }
  }

  return IsUserAuthorizedLabels(user_or_role_, dba_, *maybe_labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::query::EdgeAccessor &edge,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsUserAuthorizedEdgeType(user_or_role_, dba_, edge.EdgeType(), fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const std::vector<memgraph::storage::LabelId> &labels,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsUserAuthorizedLabels(user_or_role_, dba_, labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::storage::EdgeTypeId &edge_type,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsUserAuthorizedEdgeType(user_or_role_, dba_, edge_type, fine_grained_privilege);
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnVertices(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsUserAuthorizedGloballyLabels(user_or_role_,
                                        FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege));
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnEdges(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsUserAuthorizedGloballyEdges(user_or_role_,
                                       FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege));
};
#endif
}  // namespace memgraph::glue
