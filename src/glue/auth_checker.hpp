// Copyright 2023 Memgraph Ltd.
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
  explicit AuthChecker(
      memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth);

  bool IsUserAuthorized(const std::optional<std::string> &username,
                        const std::vector<query::AuthQuery::Privilege> &privileges,
                        const std::string &db_name) const override;

#ifdef MG_ENTERPRISE
  std::unique_ptr<memgraph::query::FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const std::string &username, const memgraph::query::DbAccessor *dba) const override;

  void ClearCache() const override;

#endif
  [[nodiscard]] static bool IsUserAuthorized(const memgraph::auth::User &user,
                                             const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                             const std::string &db_name = "");

 private:
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
  mutable memgraph::utils::Synchronized<auth::User, memgraph::utils::SpinLock> user_;  // cached user
};
#ifdef MG_ENTERPRISE
class FineGrainedAuthChecker : public query::FineGrainedAuthChecker {
 public:
  explicit FineGrainedAuthChecker(auth::User user, const memgraph::query::DbAccessor *dba);

  bool Has(const query::VertexAccessor &vertex, memgraph::storage::View view,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool Has(const query::EdgeAccessor &edge,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool Has(const std::vector<memgraph::storage::LabelId> &labels,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool Has(const memgraph::storage::EdgeTypeId &edge_type,
           query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool HasGlobalPrivilegeOnVertices(
      memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

  bool HasGlobalPrivilegeOnEdges(
      memgraph::query::AuthQuery::FineGrainedPrivilege fine_grained_privilege) const override;

 private:
  auth::User user_;
  const memgraph::query::DbAccessor *dba_;
};
#endif
}  // namespace memgraph::glue
