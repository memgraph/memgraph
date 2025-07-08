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
#include <string>
#include <vector>

#include "query/frontend/ast/query/auth_query.hpp"

namespace memgraph::query {

class UserPolicy {
 public:
  virtual bool DoUpdate() const = 0;
  virtual ~UserPolicy() = default;
};
extern struct SessionLongPolicy : UserPolicy {
 public:
  bool DoUpdate() const override { return false; }
} session_long_policy;
extern struct UpToDatePolicy : UserPolicy {
 public:
  bool DoUpdate() const override { return true; }
} up_to_date_policy;

struct QueryUserOrRole {
  QueryUserOrRole(std::optional<std::string> username, std::vector<std::string> rolenames)
      : username_{std::move(username)}, rolenames_{std::move(rolenames)} {}
  virtual ~QueryUserOrRole() = default;

  virtual bool IsAuthorized(const std::vector<AuthQuery::Privilege> &privileges, std::string_view db_name,
                            UserPolicy *policy) const = 0;

#ifdef MG_ENTERPRISE
  virtual bool CanImpersonate(const std::string &target, UserPolicy *policy,
                              std::optional<std::string_view> db_name = std::nullopt) const = 0;
  virtual std::string GetDefaultDB() const = 0;
#endif

  std::string key() const {
    // NOTE: Each role has an associated username, that's why we check it with higher priority
    return !rolenames_.empty() ? rolenames_[0] : (username_ ? *username_ : "");
  }
  const std::optional<std::string> &username() const { return username_; }
  const std::vector<std::string> &rolenames() const { return rolenames_; }

  bool operator==(const QueryUserOrRole &other) const = default;
  operator bool() const { return username_.has_value(); }

 protected:
  std::optional<std::string> username_;
  std::vector<std::string> rolenames_;
};

}  // namespace memgraph::query
