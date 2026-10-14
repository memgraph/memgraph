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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "query/frontend/ast/query/auth_query.hpp"

namespace memgraph::query {

struct QueryUserOrRole {
  QueryUserOrRole(std::optional<std::string> username, std::vector<std::string> rolenames)
      : username_{std::move(username)}, rolenames_{std::move(rolenames)} {}

  virtual ~QueryUserOrRole() = default;

  virtual std::shared_ptr<QueryUserOrRole> clone() const = 0;

  virtual bool IsAuthorized(const std::vector<AuthQuery::Privilege> &privileges,
                            std::optional<std::string_view> db_name) const = 0;

  virtual std::vector<std::string> GetRolenames(std::optional<std::string> db_name) const = 0;

  // Owner-thread-only (may refresh the cached principal). Changes whenever the cached principal may have changed;
  // callers that cache anything derived from the principal rebuild when it differs. May throw on a failed refresh.
  virtual uint64_t PrincipalVersion() const { return 0; }

#ifdef MG_ENTERPRISE
  // Re-reads the impersonator first: false when it was dropped, revoked or its refresh failed.
  virtual bool CanImpersonate(const std::string &target,
                              std::optional<std::string_view> db_name = std::nullopt) const = 0;
  virtual std::string GetDefaultDB() const = 0;
#endif

  const std::optional<std::string> &username() const { return username_; }

  const std::vector<std::string> &rolenames() const { return rolenames_; }

  bool operator==(const QueryUserOrRole &other) const = default;

  operator bool() const { return username_.has_value(); }

 protected:
  QueryUserOrRole(const QueryUserOrRole &) = default;
  QueryUserOrRole(QueryUserOrRole &&) noexcept = default;
  QueryUserOrRole &operator=(const QueryUserOrRole &) = default;
  QueryUserOrRole &operator=(QueryUserOrRole &&) noexcept = default;

  std::optional<std::string> username_;
  std::vector<std::string> rolenames_;
};

}  // namespace memgraph::query
