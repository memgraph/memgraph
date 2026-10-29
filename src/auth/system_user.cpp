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

void LoadInternalSystemUser(std::filesystem::path const &path) {
  std::ifstream file{path};
  if (!file) {
    throw AuthException("Couldn't read the internal system user's hash file {}.", path.string());
  }
  auto const contents = std::string{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
  if (file.bad()) {
    throw AuthException("Couldn't read the internal system user's hash file {}.", path.string());
  }
  std::optional<HashedPassword> hash;
  try {
    hash = UserDefinedHash(utils::Trim(contents));
  } catch (AuthException const &) {
    // Only a hash of an algorithm FIPS mode doesn't approve gets here.
    throw AuthException(
        "The internal system user's hash file {} holds a hash FIPS mode doesn't permit; with --fips-mode on it must "
        "hold a pbkdf2-sha256 hash in the form \"pbkdf2-sha256:600000:<salt>:<key>\".",
        path.string());
  }
  if (!hash ||
      (hash->HashAlgo() != PasswordHashAlgorithm::BCRYPT && hash->HashAlgo() != PasswordHashAlgorithm::PBKDF2_SHA256) ||
      !IsUsable(*hash)) {
    // The contents are a credential: never echo them.
    throw AuthException(
        "The internal system user's hash file {} must hold a single bcrypt hash (\"bcrypt:$2a$...\") or "
        "pbkdf2-sha256 hash (\"pbkdf2-sha256:600000:<salt>:<key>\", salt and key in lowercase hex) of a non-empty "
        "password.",
        path.string());
  }
  internal_system_user_hash = std::move(*hash);
}

void ResetInternalSystemUser() { internal_system_user_hash.reset(); }

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
  spdlog::debug("The internal system user {} logged in.", username);
  return InternalSystemUserLogin::kAccepted;
}

}  // namespace memgraph::auth
