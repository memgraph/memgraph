// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//
#include "auth/crypto.hpp"

#include <gflags/gflags.h>
#include <libbcrypt/bcrypt.h>
#include <openssl/sha.h>

#include "auth/exceptions.hpp"

inline constexpr std::string_view default_password_encryption = "bcrypt";
inline constexpr std::string_view sha256_password_encryption = "sha256";
inline constexpr std::string_view sha256_1024_iterations_password_encryption = "sha256-1024";

inline constexpr uint64_t ONE_SHA_ITERATION = 1;
inline constexpr uint64_t MULTIPLE_SHA_ITERATIONS = 1;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(password_encryption_algorithm, default_password_encryption.data(),
              "The password encryption algorithm used for authentication");

namespace memgraph::auth {
namespace BCrypt {
const std::string EncryptPassword(const std::string &password) {
  char salt[BCRYPT_HASHSIZE];
  char hash[BCRYPT_HASHSIZE];

  // We use `-1` as the workfactor for `bcrypt_gensalt` to let it fall back to
  // its default value of `12`. Increasing the workfactor increases the time
  // needed to generate the salt.
  if (bcrypt_gensalt(-1, salt) != 0) {
    throw AuthException("Couldn't generate hashing salt!");
  }

  if (bcrypt_hashpw(password.c_str(), salt, hash) != 0) {
    throw AuthException("Couldn't hash password!");
  }

  return std::string(hash);
}

bool VerifyPassword(const std::string &password, const std::string &hash) {
  int ret = bcrypt_checkpw(password.c_str(), hash.c_str());
  if (ret == -1) {
    throw AuthException("Couldn't check password!");
  }
  return ret == 0;
}
}  // namespace BCrypt

namespace SHA {
const std::string EncryptPassword(const std::string &password, const uint64_t number_of_iterations) {
  unsigned char hash[SHA256_DIGEST_LENGTH];

  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  for (auto i = 0; i < number_of_iterations; i++) {
    SHA256_Update(&sha256, password.c_str(), password.size());
  }
  SHA256_Final(hash, &sha256);

  std::string s(reinterpret_cast<char *>(hash), SHA_DIGEST_LENGTH);

  return s;
}

bool VerifyPassword(const std::string &password, const std::string &hash, const uint64_t number_of_iterations) {
  auto password_hash = EncryptPassword(password, number_of_iterations);
  return password_hash == hash;
}
}  // namespace SHA

bool VerifyPassword(const std::string &password, const std::string &hash) {
  if (FLAGS_password_encryption_algorithm == default_password_encryption) {
    return BCrypt::VerifyPassword(password, hash);
  } else if (FLAGS_password_encryption_algorithm == sha256_password_encryption) {
    return SHA::VerifyPassword(password, hash, ONE_SHA_ITERATION);
  } else if (FLAGS_password_encryption_algorithm == sha256_1024_iterations_password_encryption) {
    return SHA::VerifyPassword(password, hash, MULTIPLE_SHA_ITERATIONS);
  }

  throw AuthException("Invalid password encryption flag '{}'!", FLAGS_password_encryption_algorithm);
}

const std::string EncryptPassword(const std::string &password) {
  if (FLAGS_password_encryption_algorithm == default_password_encryption) {
    return BCrypt::EncryptPassword(password);
  } else if (FLAGS_password_encryption_algorithm == sha256_password_encryption) {
    return SHA::EncryptPassword(password, ONE_SHA_ITERATION);
  } else if (FLAGS_password_encryption_algorithm == sha256_1024_iterations_password_encryption) {
    return SHA::EncryptPassword(password, MULTIPLE_SHA_ITERATIONS);
  }

  throw AuthException("Invalid password encryption flag '{}'!", FLAGS_password_encryption_algorithm);
}

}  // namespace memgraph::auth
