// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include <nlohmann/json_fwd.hpp>

namespace memgraph::auth {
/// Need to be stable, auth durability depends on this
enum class PasswordHashAlgorithm : uint8_t { BCRYPT = 0, SHA256 = 1, SHA256_MULTIPLE = 2 };

struct HashSize {
  size_t unsalted;  //!< size defined by the algorithm
  size_t salted;    //!< size after our salt has been added
};

void SetHashAlgorithm(std::string_view algo);

auto CurrentHashAlgorithm() -> PasswordHashAlgorithm;

/**
 * @brief Return algorithm name. Needs to be stable; auth queries depend on it.
 *
 * @param hash_algo
 * @return std::string_view
 */
auto AsString(PasswordHashAlgorithm hash_algo) -> std::string_view;

/**
 * @brief Hash size (unsalted - as given by the algo; and salted - size after our salt is added)
 *
 * @param hash_algo
 * @return struct HashSize
 */
auto HashSize(PasswordHashAlgorithm hash_algo) -> struct HashSize;

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

  bool IsSalted() const;

  auto HashAlgo() const -> PasswordHashAlgorithm { return hash_algo; }

  friend void to_json(nlohmann::json &j, const HashedPassword &p);
  friend void from_json(const nlohmann::json &j, HashedPassword &p);

 private:
  PasswordHashAlgorithm hash_algo{PasswordHashAlgorithm::BCRYPT};
  std::string password_hash{};
};

/// @throw AuthException if unable to hash the password.
HashedPassword HashPassword(const std::string &password, std::optional<PasswordHashAlgorithm> override_algo = {});

std::optional<HashedPassword> UserDefinedHash(std::string_view password);
}  // namespace memgraph::auth
