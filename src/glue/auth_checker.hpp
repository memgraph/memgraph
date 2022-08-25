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

#pragma once

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "glue/auth.hpp"
#include "query/auth_checker.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"

namespace memgraph::glue {

class AuthChecker : public query::AuthChecker {
 public:
  explicit AuthChecker(
      memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth);

  bool IsUserAuthorized(const std::optional<std::string> &username,
                        const std::vector<query::AuthQuery::Privilege> &privileges) const override;

  std::unique_ptr<memgraph::query::FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const std::string &username) const override;

  [[nodiscard]] static bool IsUserAuthorized(const memgraph::auth::User &user,
                                             const std::vector<memgraph::query::AuthQuery::Privilege> &privileges);

 private:
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
};

class FineGrainedAuthChecker : public query::FineGrainedAuthChecker {
 public:
  explicit FineGrainedAuthChecker(auth::User user);

  virtual bool Accept(const memgraph::query::DbAccessor &dba, const query::VertexAccessor &vertex,
                      memgraph::storage::View view, memgraph::auth::FineGrainedPermission permission) const override;

  virtual bool Accept(const memgraph::query::DbAccessor &dba, const query::EdgeAccessor &edge,
                      memgraph::auth::FineGrainedPermission permission) const override;

 private:
  auth::User user_;
};

}  // namespace memgraph::glue
