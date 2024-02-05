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
#include "license/license.hpp"
#include "query/constants.hpp"
#include "query/frontend/ast/ast.hpp"
#include "utils/synchronized.hpp"

#ifdef MG_ENTERPRISE
namespace {
bool IsUserAuthorizedLabels(const memgraph::auth::User &user, const memgraph::query::DbAccessor *dba,
                            const std::vector<memgraph::storage::LabelId> &labels,
                            const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return std::all_of(labels.begin(), labels.end(), [dba, &user, fine_grained_privilege](const auto &label) {
    return user.GetFineGrainedAccessLabelPermissions().Has(
               dba->LabelToName(label), memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(
                                            fine_grained_privilege)) == memgraph::auth::PermissionLevel::GRANT;
  });
}

bool IsUserAuthorizedGloballyLabels(const memgraph::auth::User &user,
                                    const memgraph::auth::FineGrainedPermission fine_grained_permission) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return user.GetFineGrainedAccessLabelPermissions().Has(memgraph::query::kAsterisk, fine_grained_permission) ==
         memgraph::auth::PermissionLevel::GRANT;
}

bool IsUserAuthorizedGloballyEdges(const memgraph::auth::User &user,
                                   const memgraph::auth::FineGrainedPermission fine_grained_permission) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return user.GetFineGrainedAccessEdgeTypePermissions().Has(memgraph::query::kAsterisk, fine_grained_permission) ==
         memgraph::auth::PermissionLevel::GRANT;
}

bool IsUserAuthorizedEdgeType(const memgraph::auth::User &user, const memgraph::query::DbAccessor *dba,
                              const memgraph::storage::EdgeTypeId &edgeType,
                              const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return user.GetFineGrainedAccessEdgeTypePermissions().Has(
             dba->EdgeTypeToName(edgeType), memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(
                                                fine_grained_privilege)) == memgraph::auth::PermissionLevel::GRANT;
}
}  // namespace
#endif
namespace memgraph::glue {

AuthChecker::AuthChecker(memgraph::auth::SynchedAuth *auth) : auth_(auth) {}

bool AuthChecker::IsUserAuthorized(const std::optional<std::string> &username,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                   const std::string &db_name) const {
  std::optional<memgraph::auth::User> maybe_user;
  {
    auto locked_auth = auth_->ReadLock();
    if (!locked_auth->AccessControlled()) {
      return true;
    }
    if (username.has_value()) {
      maybe_user = locked_auth->GetUser(*username);
    }
  }

  return maybe_user.has_value() && IsUserAuthorized(*maybe_user, privileges, db_name);
}

#ifdef MG_ENTERPRISE
std::unique_ptr<memgraph::query::FineGrainedAuthChecker> AuthChecker::GetFineGrainedAuthChecker(
    const std::string &username, const memgraph::query::DbAccessor *dba) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  try {
    auto user = user_.Lock();
    if (username != user->username()) {
      auto maybe_user = auth_->ReadLock()->GetUser(username);
      if (!maybe_user) {
        throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist .", username);
      }
      *user = std::move(*maybe_user);
    }
    return std::make_unique<memgraph::glue::FineGrainedAuthChecker>(*user, dba);

  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthChecker::ClearCache() const {
  user_.WithLock([](auto &user) mutable { user = {}; });
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

#ifdef MG_ENTERPRISE
FineGrainedAuthChecker::FineGrainedAuthChecker(auth::User user, const memgraph::query::DbAccessor *dba)
    : user_{std::move(user)}, dba_(dba){};

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

  return IsUserAuthorizedLabels(user_, dba_, *maybe_labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::query::EdgeAccessor &edge,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsUserAuthorizedEdgeType(user_, dba_, edge.EdgeType(), fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const std::vector<memgraph::storage::LabelId> &labels,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsUserAuthorizedLabels(user_, dba_, labels, fine_grained_privilege);
}

bool FineGrainedAuthChecker::Has(const memgraph::storage::EdgeTypeId &edge_type,
                                 const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  return IsUserAuthorizedEdgeType(user_, dba_, edge_type, fine_grained_privilege);
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnVertices(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsUserAuthorizedGloballyLabels(user_, FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege));
}

bool FineGrainedAuthChecker::HasGlobalPrivilegeOnEdges(
    const memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return true;
  }
  return IsUserAuthorizedGloballyEdges(user_, FineGrainedPrivilegeToFineGrainedPermission(fine_grained_privilege));
};
#endif
}  // namespace memgraph::glue
