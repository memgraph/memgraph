// Copyright 2024 Memgraph Ltd.
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

constexpr auto kHashAlgo = "hash_algo";
constexpr auto kPasswordHash = "password_hash";

inline constexpr std::array password_encryption_mappings{
    std::pair{"bcrypt"sv, memgraph::auth::PasswordHashAlgorithm::BCRYPT},
    std::pair{"sha256"sv, memgraph::auth::PasswordHashAlgorithm::SHA256},
    std::pair{"sha256-multiple"sv, memgraph::auth::PasswordHashAlgorithm::SHA256_MULTIPLE}};

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

HashedPassword EncryptPassword(const std::string &password, std::optional<PasswordHashAlgorithm> override_algo) {
  auto const hash_algo = override_algo.value_or(CurrentEncryptionAlgorithm());
  auto password_hash = std::invoke([&] {
    switch (hash_algo) {
      case PasswordHashAlgorithm::BCRYPT:
        return BCrypt::EncryptPassword(password);
      case PasswordHashAlgorithm::SHA256:
        return SHA::EncryptPassword(password, ONE_SHA_ITERATION);
      case PasswordHashAlgorithm::SHA256_MULTIPLE:
        return SHA::EncryptPassword(password, MULTIPLE_SHA_ITERATIONS);
    }
  });
  return HashedPassword{hash_algo, std::move(password_hash)};
};

namespace {

auto InternalParseEncryptionAlgorithm(std::string const &algo) -> PasswordHashAlgorithm {
  auto maybe_parsed = utils::StringToEnum<PasswordHashAlgorithm>(algo, password_encryption_mappings);
  if (!maybe_parsed) {
    throw AuthException("Invalid password encryption '{}'!", algo);
  }
  return *maybe_parsed;
}

PasswordHashAlgorithm &InternalCurrentEncryptionAlgorithm() {
  static auto current = PasswordHashAlgorithm::BCRYPT;
  static std::once_flag flag;
  std::call_once(flag, [] { current = InternalParseEncryptionAlgorithm(FLAGS_password_encryption_algorithm); });
  return current;
}
}  // namespace

auto CurrentEncryptionAlgorithm() -> PasswordHashAlgorithm { return InternalCurrentEncryptionAlgorithm(); }

void SetEncryptionAlgorithm(const std::string &algo) {
  auto &current = InternalCurrentEncryptionAlgorithm();
  current = InternalParseEncryptionAlgorithm(algo);
}

auto AsString(PasswordHashAlgorithm enc_algo) -> std::string_view {
  return *utils::EnumToString<PasswordHashAlgorithm>(enc_algo, password_encryption_mappings);
}

bool HashedPassword::VerifyPassword(const std::string &password) {
  switch (hash_algo) {
    case PasswordHashAlgorithm::BCRYPT:
      return BCrypt::VerifyPassword(password, password_hash);
    case PasswordHashAlgorithm::SHA256:
      return SHA::VerifyPassword(password, password_hash, ONE_SHA_ITERATION);
    case PasswordHashAlgorithm::SHA256_MULTIPLE:
      return SHA::VerifyPassword(password, password_hash, MULTIPLE_SHA_ITERATIONS);
  }
}

void to_json(nlohmann::json &j, const HashedPassword &p) {
  j = nlohmann::json{{kHashAlgo, p.hash_algo}, {kPasswordHash, p.password_hash}};
}

void from_json(const nlohmann::json &j, HashedPassword &p) {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  PasswordHashAlgorithm hash_algo;
  j.at(kHashAlgo).get_to(hash_algo);
  auto password_hash = j.value(kPasswordHash, std::string());
  p = HashedPassword{hash_algo, std::move(password_hash)};
}

}  // namespace memgraph::auth
