// Copyright 2025 Memgraph Ltd.
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

  std::vector<std::string> GetRolenames(std::optional<std::string> db_name) const override;

#ifdef MG_ENTERPRISE
  bool CanImpersonate(const std::string &target, query::UserPolicy *policy,
                      std::optional<std::string_view> db_name = std::nullopt) const override;
  std::string GetDefaultDB() const override;
#endif

  explicit QueryUserOrRole(auth::SynchedAuth *auth) : query::QueryUserOrRole{{}, {}}, auth_{auth} {}

  QueryUserOrRole(auth::SynchedAuth *auth, auth::UserOrRole user_or_role);

  std::optional<auth::UserOrRole> GenAuthUserOrRole() const {
    if (user_) return {*user_};
    if (roles_) return auth::RoleWUsername{*username_, *roles_};
    return {};
  }

 private:
  friend class AuthChecker;
  auth::SynchedAuth *auth_;
  mutable std::optional<auth::User> user_{};
  mutable std::optional<auth::Roles> roles_{};
  mutable auth::Auth::Epoch auth_epoch_{auth::Auth::kStartEpoch};
};

}  // namespace memgraph::glue
