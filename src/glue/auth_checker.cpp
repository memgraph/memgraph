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
#include "query/query_user.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

#ifdef MG_ENTERPRISE
namespace {
bool IsAuthorizedLabels(const memgraph::auth::UserOrRole &user_or_role, const memgraph::query::DbAccessor *dba,
                        std::span<memgraph::storage::LabelId const> labels,
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

bool IsAuthorizedGloballyLabels(const memgraph::auth::UserOrRole &user_or_role,
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

bool IsAuthorizedGloballyEdges(const memgraph::auth::UserOrRole &user_or_role,
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

bool IsAuthorizedEdgeType(const memgraph::auth::UserOrRole &user_or_role, const memgraph::query::DbAccessor *dba,
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

std::shared_ptr<query::QueryUserOrRole> AuthChecker::GenQueryUser(const std::optional<std::string> &username,
                                                                  const std::optional<std::string> &rolename) const {
  const auto user_or_role = auth_->ReadLock()->GetUserOrRole(username, rolename);
  if (user_or_role) {
    return std::make_shared<QueryUserOrRole>(auth_, *user_or_role);
  }
  // No user or role
  return std::make_shared<QueryUserOrRole>(auth_);
}

std::unique_ptr<query::QueryUserOrRole> AuthChecker::GenQueryUser(auth::SynchedAuth *auth,
                                                                  const std::optional<auth::UserOrRole> &user_or_role) {
  if (user_or_role) {
    return std::visit(
        utils::Overloaded{[&](auto &user_or_role) { return std::make_unique<QueryUserOrRole>(auth, user_or_role); }},
        *user_or_role);
  }
  // No user or role
  return std::make_unique<QueryUserOrRole>(auth);
}

#ifdef MG_ENTERPRISE
std::unique_ptr<memgraph::query::FineGrainedAuthChecker> AuthChecker::GetFineGrainedAuthChecker(
    std::shared_ptr<query::QueryUserOrRole> user_or_role, const memgraph::query::DbAccessor *dba) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  if (!user_or_role || !*user_or_role) {
    throw query::QueryRuntimeException("No user specified for fine grained authorization!");
  }

  // Convert from query user to auth user or role
  try {
    auto glue_user = dynamic_cast<glue::QueryUserOrRole &>(*user_or_role);
    if (glue_user.user_) {
      return std::make_unique<glue::FineGrainedAuthChecker>(std::move(*glue_user.user_), dba);
    }
    if (glue_user.role_) {
      return std::make_unique<glue::FineGrainedAuthChecker>(
          auth::RoleWUsername{*glue_user.username(), std::move(*glue_user.role_)}, dba);
    }
    DMG_ASSERT(false, "Glue user has neither user not role");
  } catch (std::bad_cast &e) {
    DMG_ASSERT(false, "Using a non-glue user in glue...");
  }

  // Should never get here
  return {};
}
#endif

bool AuthChecker::IsUserAuthorized(const memgraph::auth::User &user,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                   const std::string &db_name) {  // NOLINT
#ifdef MG_ENTERPRISE
  if (!db_name.empty() && !user.HasAccess(db_name)) {
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
  if (!db_name.empty() && !role.HasAccess(db_name)) {
    return false;
  }
#endif
  const auto role_permissions = role.permissions();
  return std::all_of(privileges.begin(), privileges.end(), [&role_permissions](const auto privilege) {
    return role_permissions.Has(memgraph::glue::PrivilegeToPermission(privilege)) ==
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
FineGrainedAuthChecker::FineGrainedAuthChecker(auth::UserOrRole user_or_role, const memgraph::query::DbAccessor *dba)
    : user_or_role_{std::move(user_or_role)}, dba_(dba){};

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

  return IsAuthorizedLabels(user_or_role_, dba_, *maybe_labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::query::EdgeAccessor &edge,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsAuthorizedEdgeType(user_or_role_, dba_, edge.EdgeType(), fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const std::vector<memgraph::storage::LabelId> &labels,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsAuthorizedLabels(user_or_role_, dba_, labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::storage::EdgeTypeId &edge_type,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsAuthorizedEdgeType(user_or_role_, dba_, edge_type, fine_grained_privilege);
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnVertices(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsAuthorizedGloballyLabels(user_or_role_, FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege));
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnEdges(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsAuthorizedGloballyEdges(user_or_role_, FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege));
};
#endif
}  // namespace memgraph::glue
