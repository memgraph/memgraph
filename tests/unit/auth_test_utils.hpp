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

namespace memgraph::auth {

/// Test-only: validates and creates a user the way CREATE USER does.
/// Returns nullopt if the user already exists; throws AuthException on invalid name or password.
inline std::optional<User> AddUser(Auth &auth, const std::string &username,
                                   const std::optional<std::string> &password = std::nullopt,
                                   system::Transaction *system_tx = nullptr) {
  auth.ValidateName(username);
  if (auth.GetUser(username)) return std::nullopt;
  if (!Auth::IsUserDefinedHash(password)) auth.ValidatePassword(password);
  return auth.AddUserWithHash(username, Auth::ComputePasswordHash(password), system_tx);
}

}  // namespace memgraph::auth
