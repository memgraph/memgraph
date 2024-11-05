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
#include "utils/variant_helpers.hpp"

namespace memgraph::glue {

struct QueryUserOrRole : public query::QueryUserOrRole {
  bool IsAuthorized(const std::vector<query::AuthQuery::Privilege> &privileges, std::string_view db_name,
                    query::UserPolicy *policy) const override;

#ifdef MG_ENTERPRISE
  std::string GetDefaultDB() const override;
#endif

  explicit QueryUserOrRole(auth::SynchedAuth *auth) : query::QueryUserOrRole{std::nullopt, std::nullopt}, auth_{auth} {}

  QueryUserOrRole(auth::SynchedAuth *auth, auth::UserOrRole user_or_role)
      : query::QueryUserOrRole{std::visit(
                                   utils::Overloaded{[](const auto &user_or_role) { return user_or_role.username(); }},
                                   user_or_role),
                               std::visit(utils::Overloaded{[&](const auth::User &) -> std::optional<std::string> {
                                                              return std::nullopt;
                                                            },
                                                            [&](const auth::Role &role) -> std::optional<std::string> {
                                                              return role.rolename();
                                                            }},
                                          user_or_role)},
        auth_{auth} {
    std::visit(utils::Overloaded{[&](auth::User &&user) { user_.emplace(std::move(user)); },
                                 [&](auth::Role &&role) { role_.emplace(std::move(role)); }},
               std::move(user_or_role));
  }

 private:
  friend class AuthChecker;
  auth::SynchedAuth *auth_;
  mutable std::optional<auth::User> user_{};
  mutable std::optional<auth::Role> role_{};
  mutable auth::Auth::Epoch auth_epoch_{auth::Auth::kStartEpoch};
};

}  // namespace memgraph::glue
