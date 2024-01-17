// Copyright 2024 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <json/json.hpp>
#include <string>

namespace memgraph::auth {
/// Need to be stable, auth durability depends on this
enum class PasswordHashAlgorithm : uint8_t { BCRYPT = 0, SHA256 = 1, SHA256_MULTIPLE = 2 };

void SetEncryptionAlgorithm(std::string const &algo);

auto CurrentEncryptionAlgorithm() -> PasswordHashAlgorithm;

auto AsString(PasswordHashAlgorithm enc_algo) -> std::string_view;

struct HashedPassword {
  HashedPassword() = default;
  HashedPassword(PasswordHashAlgorithm hash_algo, std::string password_hash)
      : hash_algo{hash_algo}, password_hash{std::move(password_hash)} {}
  HashedPassword(HashedPassword const &) = default;
  HashedPassword(HashedPassword &&) = default;
  HashedPassword &operator=(HashedPassword const &) = default;
  HashedPassword &operator=(HashedPassword &&) = default;

  friend bool operator==(HashedPassword const &, HashedPassword const &) = default;

  bool VerifyPassword(const std::string &password);

 private:
  PasswordHashAlgorithm hash_algo{PasswordHashAlgorithm::BCRYPT};
  std::string password_hash{};

  friend void to_json(nlohmann::json &j, const HashedPassword &p);
  friend void from_json(const nlohmann::json &j, HashedPassword &p);
};

/// @throw AuthException if unable to encrypt the password.
HashedPassword EncryptPassword(const std::string &password);

/// @throw AuthException if unable to verify the password.
bool VerifyPassword(const std::string &password, const HashedPassword &hash);
}  // namespace memgraph::auth
