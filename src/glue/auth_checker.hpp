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

#pragma once

#include "auth/auth.hpp"
#include "glue/auth.hpp"
#include "query/auth_checker.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::glue {

class AuthChecker : public query::AuthChecker {
 public:
  explicit AuthChecker(auth::SynchedAuth *auth);

  std::shared_ptr<query::QueryUserOrRole> GenQueryUser(const std::optional<std::string> &username,
                                                       const std::optional<std::string> &rolename) const override;

  static std::unique_ptr<query::QueryUserOrRole> GenQueryUser(auth::SynchedAuth *auth,
                                                              const std::optional<auth::UserOrRole> &user_or_role);

#ifdef MG_ENTERPRISE
  std::unique_ptr<query::FineGrainedAuthChecker> GetFineGrainedAuthChecker(std::shared_ptr<query::QueryUserOrRole> user,
                                                                           const query::DbAccessor *dba) const override;
#endif

  [[nodiscard]] static bool IsUserAuthorized(const auth::User &user,
                                             const std::vector<query::AuthQuery::Privilege> &privileges,
                                             std::string_view db_name = "");

  [[nodiscard]] static bool IsRoleAuthorized(const auth::Role &role,
                                             const std::vector<query::AuthQuery::Privilege> &privileges,
                                             std::string_view db_name = "");

  [[nodiscard]] static bool IsUserOrRoleAuthorized(const auth::UserOrRole &user_or_role,
                                                   const std::vector<query::AuthQuery::Privilege> &privileges,
                                                   std::string_view db_name = "");

 private:
  auth::SynchedAuth *auth_;
  mutable utils::Synchronized<auth::UserOrRole, utils::SpinLock> user_or_role_;  // cached user
};
#ifdef MG_ENTERPRISE
class FineGrainedAuthChecker : public query::FineGrainedAuthChecker {
 public:
  explicit FineGrainedAuthChecker(auth::UserOrRole user, const query::DbAccessor *dba);

  bool Has(const query::VertexAccessor &vertex, storage::View view,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool Has(const query::EdgeAccessor &edge,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool Has(const std::vector<storage::LabelId> &labels,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool Has(const storage::EdgeTypeId &edge_type,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool HasGlobalPrivilegeOnVertices(query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool HasGlobalPrivilegeOnEdges(query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

 private:
  auth::UserOrRole user_or_role_;
  const query::DbAccessor *dba_;
};
#endif
}  // namespace memgraph::glue
