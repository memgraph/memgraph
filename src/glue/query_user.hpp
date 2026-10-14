// Copyright 2026 Memgraph Ltd.
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

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>

#include "auth/auth.hpp"
#include "query/query_user.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::glue {

struct QueryUserOrRole : public query::QueryUserOrRole {
  std::shared_ptr<query::QueryUserOrRole> clone() const override;

  bool IsAuthorized(const std::vector<query::AuthQuery::Privilege> &privileges,
                    std::optional<std::string_view> db_name) const override;

  std::vector<std::string> GetRolenames(std::optional<std::string> db_name) const override;

  std::optional<utils::UUID> UserUuid() const override { return user_uuid_; }

  uint64_t PrincipalVersion() const override;

#ifdef MG_ENTERPRISE
  bool CanImpersonate(const std::string &target, std::optional<std::string_view> db_name = std::nullopt) const override;
  std::string GetDefaultDB() const override;
#endif

  // True if the principal was dropped or replaced by a same-named user; refreshes first, so may throw.
  bool IsDropped() const {
    EnsureFresh();
    return dropped_;
  }

  explicit QueryUserOrRole(auth::SynchedAuth *auth);

  QueryUserOrRole(auth::SynchedAuth *auth, auth::UserOrRole user_or_role);

  std::optional<auth::UserOrRole> GenAuthUserOrRole() const {
    if (user_) return {*user_};
    if (roles_) return auth::RoleWUsername{*username_, *roles_};
    return {};
  }

 private:
  friend class AuthChecker;
  void RefreshPrincipal(const auth::Auth &auth) const;
  // Takes the read lock itself, so call it with none held. Throws auth::AuthException on a failed refresh.
  void EnsureFresh() const;

  auth::SynchedAuth *auth_;
  // Not written once the principal is shared (a uuid mismatch latches dropped_ instead): any thread may read it.
  std::optional<utils::UUID> user_uuid_{};
  // user_/roles_/dropped_/auth_epoch_ are a lazily refreshed cache that any authorization check may write:
  // a foreign thread may use only the base-class username(), rolenames() (not the refreshing virtual GetRolenames()),
  // operator==, operator bool; otherwise take its own clone().
  mutable std::optional<auth::User> user_{};
  mutable std::optional<auth::Roles> roles_{};
  mutable bool dropped_{false};
  // 0 = never refreshed; Auth starts at 1.
  mutable uint64_t auth_epoch_{0};
  std::atomic<uint64_t> const *epoch_src_;
  mutable bool access_controlled_{true};
};

}  // namespace memgraph::glue
