// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//
#include "auth/crypto.hpp"

#include <iomanip>
#include <sstream>

#include <gflags/gflags.h>
#include <libbcrypt/bcrypt.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>
#include <openssl/sha.h>

#include "auth/exceptions.hpp"
#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"

namespace {
using namespace std::literals;
inline constexpr std::array password_encryption_mappings{
    std::pair{"bcrypt"sv, memgraph::auth::PasswordEncryptionAlgorithm::BCRYPT},
    std::pair{"sha256"sv, memgraph::auth::PasswordEncryptionAlgorithm::SHA256},
    std::pair{"sha256-multiple"sv, memgraph::auth::PasswordEncryptionAlgorithm::SHA256_MULTIPLE}};

inline constexpr uint64_t ONE_SHA_ITERATION = 1;
inline constexpr uint64_t MULTIPLE_SHA_ITERATIONS = 1024;
}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,misc-unused-parameters)
DEFINE_VALIDATED_string(password_encryption_algorithm, "bcrypt",
                        "The password encryption algorithm used for authentication.", {
                          if (const auto result =
                                  memgraph::utils::IsValidEnumValueString(value, password_encryption_mappings);
                              result.HasError()) {
                            const auto error = result.GetError();
                            switch (error) {
                              case memgraph::utils::ValidationError::EmptyValue: {
                                std::cout << "Password encryption algorithm cannot be empty." << std::endl;
                                break;
                              }
                              case memgraph::utils::ValidationError::InvalidValue: {
                                std::cout << "Invalid value for password encryption algorithm. Allowed values: "
                                          << memgraph::utils::GetAllowedEnumValuesString(password_encryption_mappings)
                                          << std::endl;
                                break;
                              }
                            }
                            return false;
                          }

                          return true;
                        });

namespace memgraph::auth {
namespace BCrypt {
std::string EncryptPassword(const std::string &password) {
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

  return {hash};
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
#if OPENSSL_VERSION_MAJOR >= 3
std::string EncryptPasswordOpenSSL3(const std::string &password, const uint64_t number_of_iterations) {
  unsigned char hash[SHA256_DIGEST_LENGTH];

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  EVP_MD *md = EVP_MD_fetch(nullptr, "SHA2-256", nullptr);

  EVP_DigestInit_ex(ctx, md, nullptr);
  for (auto i = 0; i < number_of_iterations; i++) {
    EVP_DigestUpdate(ctx, password.c_str(), password.size());
  }
  EVP_DigestFinal_ex(ctx, hash, nullptr);

  EVP_MD_free(md);
  EVP_MD_CTX_free(ctx);

  std::stringstream result_stream;
  for (auto hash_char : hash) {
    result_stream << std::hex << std::setw(2) << std::setfill('0') << (int)hash_char;
  }

  return result_stream.str();
}
#else
std::string EncryptPasswordOpenSSL1_1(const std::string &password, const uint64_t number_of_iterations) {
  unsigned char hash[SHA256_DIGEST_LENGTH];

  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  for (auto i = 0; i < number_of_iterations; i++) {
    SHA256_Update(&sha256, password.c_str(), password.size());
  }
  SHA256_Final(hash, &sha256);

  std::stringstream ss;
  for (auto hash_char : hash) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash_char;
  }

  return ss.str();
}
#endif

std::string EncryptPassword(const std::string &password, const uint64_t number_of_iterations) {
#if OPENSSL_VERSION_MAJOR >= 3
  return EncryptPasswordOpenSSL3(password, number_of_iterations);
#else
  return EncryptPasswordOpenSSL1_1(password, number_of_iterations);
#endif
}

bool VerifyPassword(const std::string &password, const std::string &hash, const uint64_t number_of_iterations) {
  auto password_hash = EncryptPassword(password, number_of_iterations);
  return password_hash == hash;
}
}  // namespace SHA

bool VerifyPassword(const std::string &password, const std::string &hash) {
  const auto password_encryption_algorithm = utils::StringToEnum<PasswordEncryptionAlgorithm>(
      FLAGS_password_encryption_algorithm, password_encryption_mappings);

  if (!password_encryption_algorithm.has_value()) {
    throw AuthException("Invalid password encryption flag '{}'!", FLAGS_password_encryption_algorithm);
  }

  switch (password_encryption_algorithm.value()) {
    case PasswordEncryptionAlgorithm::BCRYPT:
      return BCrypt::VerifyPassword(password, hash);
    case PasswordEncryptionAlgorithm::SHA256:
      return SHA::VerifyPassword(password, hash, ONE_SHA_ITERATION);
    case PasswordEncryptionAlgorithm::SHA256_MULTIPLE:
      return SHA::VerifyPassword(password, hash, MULTIPLE_SHA_ITERATIONS);
  }

  throw AuthException("Invalid password encryption flag '{}'!", FLAGS_password_encryption_algorithm);
}

std::string EncryptPassword(const std::string &password) {
  const auto password_encryption_algorithm = utils::StringToEnum<PasswordEncryptionAlgorithm>(
      FLAGS_password_encryption_algorithm, password_encryption_mappings);

  if (!password_encryption_algorithm.has_value()) {
    throw AuthException("Invalid password encryption flag '{}'!", FLAGS_password_encryption_algorithm);
  }

  switch (password_encryption_algorithm.value()) {
    case PasswordEncryptionAlgorithm::BCRYPT:
      return BCrypt::EncryptPassword(password);
    case PasswordEncryptionAlgorithm::SHA256:
      return SHA::EncryptPassword(password, ONE_SHA_ITERATION);
    case PasswordEncryptionAlgorithm::SHA256_MULTIPLE:
      return SHA::EncryptPassword(password, MULTIPLE_SHA_ITERATIONS);
  }
}

}  // namespace memgraph::auth
