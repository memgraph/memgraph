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

#include <string>
#include <vector>

#include "query/frontend/ast/ast.hpp"

namespace memgraph::query {

class UserPolicy {
 public:
  virtual bool DoUpdate() const = 0;
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
  QueryUserOrRole(std::optional<std::string> username, std::optional<std::string> rolename)
      : username_{std::move(username)}, rolename_{std::move(rolename)} {}
  virtual ~QueryUserOrRole() = default;

  virtual bool IsAuthorized(const std::vector<AuthQuery::Privilege> &privileges, const std::string &db_name,
                            UserPolicy *policy) const = 0;

#ifdef MG_ENTERPRISE
  virtual std::string GetDefaultDB() const = 0;
#endif

  std::string key() const {
    // NOTE: Each role has an associated username, that's why we check it with higher priority
    return rolename_ ? *rolename_ : (username_ ? *username_ : "");
  }
  const std::optional<std::string> &username() const { return username_; }
  const std::optional<std::string> &rolename() const { return rolename_; }

  bool operator==(const QueryUserOrRole &other) const = default;
  operator bool() const { return username_.has_value(); }

 private:
  std::optional<std::string> username_;
  std::optional<std::string> rolename_;
};

}  // namespace memgraph::query
