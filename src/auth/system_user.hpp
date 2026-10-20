// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <filesystem>
#include <string>
#include <string_view>

#include "auth/crypto.hpp"

// The internal system user is the identity the Kubernetes operator (or any other automation that manages a cluster)
// logs in with. It is configured, not created: --system-user-internal-hash-file names a file holding its bcrypt
// hash, and while that flag is empty the user doesn't exist and every login behaves as before. It never lives in the
// auth kvstore and never reaches an auth module, so no Cypher query, replication of the auth store or SSO
// configuration can create, change, drop or shadow it, and SSO on a coordinator never closes it.
namespace memgraph::auth {

inline constexpr std::string_view kInternalSystemUsername{"memgraph_internal"};

/// Whether `name` is the internal system user's name, compared the way user and role names are stored (lowercased).
/// The name is reserved whether or not the user is configured, so nobody can claim it before it is turned on.
[[nodiscard]] bool IsInternalSystemUsername(std::string_view name);

/// Whether a user or role replicated from a main carries the reserved name, and so must not be stored. Only a main on
/// an older version can still hold one. Logs a warning when it does.
[[nodiscard]] bool IsReservedReplicatedName(std::string_view name);

/// Enables the internal system user with the hash read from `path`: a single pre-hashed bcrypt literal
/// (`bcrypt:$2a$...`) of a non-empty password, the same format CREATE USER ... IDENTIFIED BY accepts, surrounding
/// whitespace ignored. bcrypt
/// carries its own salt and cost. Called once at startup, before any session exists -- the hash is read without a lock
/// afterwards.
/// @throw AuthException if the file can't be read, holds anything else, or FIPS mode is on (bcrypt isn't approved).
void LoadInternalSystemUser(std::filesystem::path const &path);

enum class InternalSystemUserLogin : uint8_t {
  kNotInternal,  // Not the internal system user, or it isn't configured: authenticate the usual way.
  kAccepted,     // The internal system user with the right password.
  kRejected,     // The internal system user with a wrong password. Never falls through to another path.
};

[[nodiscard]] InternalSystemUserLogin AuthenticateInternalSystemUser(std::string_view username,
                                                                     std::string const &password);

}  // namespace memgraph::auth
