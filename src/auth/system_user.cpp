// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/system_user.hpp"

#include <fstream>
#include <iterator>
#include <optional>
#include <utility>

#include <spdlog/spdlog.h>

#include "auth/exceptions.hpp"
#include "utils/fips.hpp"
#include "utils/string.hpp"

namespace memgraph::auth {

namespace {
// Written once at startup, before the Bolt server accepts a connection, then only read.
std::optional<HashedPassword> internal_system_user_hash;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// bcrypt refuses a malformed hash only when asked to verify against it, so trying the empty password once at startup
// both means a login never throws and refuses a hash of the empty password.
bool IsUsable(HashedPassword hash) {
  try {
    return !hash.VerifyPassword({});
  } catch (AuthException const &) {
    return false;
  }
}
}  // namespace

bool IsInternalSystemUsername(std::string_view name) { return utils::ToLowerCase(name) == kInternalSystemUsername; }

bool IsReservedReplicatedName(std::string_view name) {
  if (!IsInternalSystemUsername(name)) return false;
  spdlog::warn("Not storing the replicated user or role {}: the name is reserved for the internal system user.", name);
  return true;
}

void LoadInternalSystemUser(std::filesystem::path const &path) {
  // Checked first: the bcrypt parser would also refuse under FIPS mode, but name pbkdf2 as the way out, which this
  // file doesn't accept.
  if (utils::FipsEnabled()) {
    throw AuthException(
        "The internal system user's hash file holds a bcrypt hash, which FIPS mode doesn't permit; leave "
        "--system-user-internal-hash-file empty when --fips-mode is on.");
  }
  std::ifstream file{path};
  if (!file) {
    throw AuthException("Couldn't read the internal system user's hash file {}.", path.string());
  }
  auto const contents = std::string{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
  if (file.bad()) {
    throw AuthException("Couldn't read the internal system user's hash file {}.", path.string());
  }
  auto hash = UserDefinedHash(utils::Trim(contents));
  if (!hash || hash->HashAlgo() != PasswordHashAlgorithm::BCRYPT || !IsUsable(*hash)) {
    // The contents are a credential: never echo them.
    throw AuthException(
        "The internal system user's hash file {} must hold a single bcrypt hash of a non-empty password in the form "
        "\"bcrypt:$2a$...\".",
        path.string());
  }
  internal_system_user_hash = std::move(*hash);
}

InternalSystemUserLogin AuthenticateInternalSystemUser(std::string_view username, std::string const &password) {
  if (!internal_system_user_hash || !IsInternalSystemUsername(username)) {
    return InternalSystemUserLogin::kNotInternal;
  }
  // VerifyPassword isn't const; a copy keeps concurrent logins off the shared value.
  auto hash = *internal_system_user_hash;
  if (!hash.VerifyPassword(password)) {
    spdlog::warn("Couldn't authenticate the internal system user {}: the password is not correct.", username);
    return InternalSystemUserLogin::kRejected;
  }
  spdlog::info("The internal system user {} logged in.", username);
  return InternalSystemUserLogin::kAccepted;
}

}  // namespace memgraph::auth
