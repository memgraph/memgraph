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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "auth/models.hpp"
#include "auth/profiles/user_profiles.hpp"

namespace memgraph::replication {

/// What a drop operation names. Mirrors DropAuthDataReq::DataType, which stays as it is for the single-drop RPC.
enum class AuthDataType : uint8_t { USER, ROLE, PROFILE, /* Leave at end */ N };

/// One record written by an auth transaction.
struct AuthUpdateOp {
  AuthUpdateOp() = default;

  explicit AuthUpdateOp(auth::User user) : user{std::move(user)} {}

  explicit AuthUpdateOp(auth::Role role) : role{std::move(role)} {}

  explicit AuthUpdateOp(auth::UserProfiles::Profile profile) : profile{std::move(profile)} {}

  std::optional<auth::User> user;
  std::optional<auth::Role> role;
  std::optional<auth::UserProfiles::Profile> profile;
};

/// One record removed by an auth transaction.
struct AuthDropOp {
  AuthDropOp() = default;

  AuthDropOp(AuthDataType type, std::string_view name) : type{type}, name{name} {}

  AuthDataType type{AuthDataType::USER};
  std::string name;
};

/// The operations of one auth transaction, in the order the transaction made them. Order is load-bearing: a
/// transaction may drop a name and recreate it, and applying those the other way round loses the record.
using AuthOp = std::variant<AuthUpdateOp, AuthDropOp>;

}  // namespace memgraph::replication
