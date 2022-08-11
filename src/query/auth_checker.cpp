// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#include "query/auth_checker.hpp"
#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "glue/auth.hpp"
#include "query/frontend/ast/ast.hpp"

namespace memgraph::query {

AuthChecker::AuthChecker() {}

AuthChecker::AuthChecker(
    memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth)
    : auth_(auth) {}

bool AuthChecker::IsUserAuthorized(const memgraph::auth::User &user,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges) const {
  const auto user_permissions = user.GetPermissions();
  return std::all_of(privileges.begin(), privileges.end(), [&user_permissions](const auto privilege) {
    return user_permissions.Has(memgraph::glue::PrivilegeToPermission(privilege)) ==
           memgraph::auth::PermissionLevel::GRANT;
  });
}

bool AuthChecker::IsUserAuthorized(const std::optional<std::string> &username,
                                   const std::vector<memgraph::query::AuthQuery::Privilege> &privileges) const {
  std::optional<memgraph::auth::User> maybe_user;
  {
    auto locked_auth = auth_->ReadLock();
    if (!locked_auth->HasUsers()) {
      return true;
    }
    if (username.has_value()) {
      maybe_user = locked_auth->GetUser(*username);
    }
  }

  return maybe_user.has_value() && AuthChecker::IsUserAuthorized(*maybe_user, privileges);
}

bool AuthChecker::Accept(const memgraph::auth::User &user, const memgraph::query::DbAccessor &dba,
                         const memgraph::query::VertexAccessor &vertex, const memgraph::storage::View &view) const {
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

  return IsUserAuthorizedLabels(user, dba, *maybe_labels);
}

bool AuthChecker::Accept(const memgraph::auth::User &user, const memgraph::query::DbAccessor &dba,
                         const memgraph::query::EdgeAccessor &edge) const {
  return IsUserAuthorizedEdgeType(user, dba, edge.EdgeType());
}

bool AuthChecker::IsUserAuthorizedLabels(const memgraph::auth::User &user, const memgraph::query::DbAccessor &dba,
                                         const std::vector<memgraph::storage::LabelId> &labels) const {
  return std::all_of(labels.begin(), labels.end(), [dba, user](const auto label) {
    return user.GetFineGrainedAccessLabelPermissions().Has(dba.LabelToName(label)) ==
           memgraph::auth::PermissionLevel::GRANT;
  });
}

bool AuthChecker::IsUserAuthorizedEdgeType(const memgraph::auth::User &user, const memgraph::query::DbAccessor &dba,
                                           const memgraph::storage::EdgeTypeId &edgeType) const {
  return user.GetFineGrainedAccessEdgeTypePermissions().Has(dba.EdgeTypeToName(edgeType)) ==
         memgraph::auth::PermissionLevel::GRANT;
}
}  // namespace memgraph::query
