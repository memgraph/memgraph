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

#include <optional>

#include "auth/auth.hpp"
#include "query/query_user.hpp"

namespace memgraph::glue {

struct QueryUser : public query::QueryUser {
  bool IsAuthorized(const std::vector<query::AuthQuery::Privilege> &privileges,
                    const std::string &db_name) const override;

#ifdef MG_ENTERPRISE
  std::string GetDefaultDB() const override;
#endif

  explicit QueryUser(auth::SynchedAuth *auth) : query::QueryUser{std::nullopt}, auth_{auth} {}

  QueryUser(auth::SynchedAuth *auth, auth::UserOrRole user_or_role)
      : query::QueryUser{std::visit(utils::Overloaded{[&](const auth::User &user) { return user.username(); },
                                                      [&](const auth::Role &role) { return role.rolename(); }},
                                    user_or_role)},
        auth_{auth} {
    std::visit(utils::Overloaded{[&](auth::User &&user) { user_.emplace(std::move(user)); },
                                 [&](auth::Role &&role) { role_.emplace(std::move(role)); }},
               std::move(user_or_role));
  }

 private:
  auth::SynchedAuth *auth_;
  mutable std::optional<auth::User> user_{};
  mutable std::optional<auth::Role> role_{};
};

}  // namespace memgraph::glue
