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

#include <optional>
#include <string>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "license/license.hpp"

/// Test-only: drops the license for the duration of a scope, and re-enables the testing license
/// afterwards if one was active. Suites share one process and several turn the testing license on, so
/// leaving it off would decide the outcome of whatever runs next. It does not preserve a license set
/// from a real key: entering the scope clears the stored license info and any license memory limit,
/// and leaving it installs the testing license instead.
struct UnlicensedScope {
  UnlicensedScope() : was_valid_{memgraph::license::global_license_checker.IsEnterpriseValidFast()} {
    memgraph::license::global_license_checker.DisableTesting();
  }

  ~UnlicensedScope() {
    if (was_valid_) memgraph::license::global_license_checker.EnableTesting();
  }

  UnlicensedScope(const UnlicensedScope &) = delete;
  UnlicensedScope &operator=(const UnlicensedScope &) = delete;
  UnlicensedScope(UnlicensedScope &&) = delete;
  UnlicensedScope &operator=(UnlicensedScope &&) = delete;

 private:
  bool was_valid_;
};

namespace memgraph::auth {

/// Test-only: validates and creates a user the way CREATE USER does.
/// Returns nullopt if the user already exists; throws AuthException on invalid name or password.
inline std::optional<User> AddUser(Auth &auth, const std::string &username,
                                   const std::optional<std::string> &password = std::nullopt,
                                   system::Transaction *system_tx = nullptr) {
  auth.ValidateName(username);
  if (auth.GetUser(username)) return std::nullopt;
  auth.ValidatePassword(password);
  return auth.AddUserWithHash(username, Auth::ComputePasswordHash(password), system_tx);
}

}  // namespace memgraph::auth
