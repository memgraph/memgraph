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

#include <iosfwd>
#include "auth/exceptions.hpp"
#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"

namespace {
using namespace std::literals;

constexpr auto kHashAlgo = "hash_algo";
constexpr auto kPasswordHash = "password_hash";

inline constexpr std::array password_hash_mappings{
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
                                  memgraph::utils::IsValidEnumValueString(value, password_hash_mappings);
                              result.HasError()) {
                            const auto error = result.GetError();
                            switch (error) {
                              case memgraph::utils::ValidationError::EmptyValue: {
                                std::cout << "Password encryption algorithm cannot be empty." << std::endl;
                                break;
                              }
                              case memgraph::utils::ValidationError::InvalidValue: {
                                std::cout << "Invalid value for password encryption algorithm. Allowed values: "
                                          << memgraph::utils::GetAllowedEnumValuesString(password_hash_mappings)
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
std::string HashPassword(const std::string &password) {
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

namespace {

constexpr auto SHA_LENGTH = 64U;
constexpr auto SALT_SIZE = 16U;
constexpr auto SALT_SIZE_DURABLE = SALT_SIZE * 2;

#if OPENSSL_VERSION_MAJOR >= 3
std::string HashPasswordOpenSSL3(std::string_view password, const uint64_t number_of_iterations,
                                 std::string_view salt) {
  unsigned char hash[SHA256_DIGEST_LENGTH];

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  EVP_MD *md = EVP_MD_fetch(nullptr, "SHA2-256", nullptr);

  EVP_DigestInit_ex(ctx, md, nullptr);

  if (!salt.empty()) {
    DMG_ASSERT(salt.size() == SALT_SIZE);
    EVP_DigestUpdate(ctx, salt.data(), salt.size());
  }

  for (auto i = 0; i < number_of_iterations; i++) {
    EVP_DigestUpdate(ctx, password.data(), password.size());
  }
  EVP_DigestFinal_ex(ctx, hash, nullptr);

  EVP_MD_free(md);
  EVP_MD_CTX_free(ctx);

  std::stringstream result_stream;

  for (unsigned char salt_char : salt) {
    result_stream << std::hex << std::setw(2) << std::setfill('0') << (((unsigned int)salt_char) & 0xFFU);
  }

  for (auto hash_char : hash) {
    result_stream << std::hex << std::setw(2) << std::setfill('0') << (int)hash_char;
  }

  return result_stream.str();
}
#else
std::string HashPasswordOpenSSL1_1(std::string_view password, const uint64_t number_of_iterations,
                                   std::string_view salt) {
  unsigned char hash[SHA256_DIGEST_LENGTH];

  SHA256_CTX sha256;
  SHA256_Init(&sha256);

  if (!salt.empty()) {
    DMG_ASSERT(salt.size() == SALT_SIZE);
    SHA256_Update(&sha256, salt.data(), salt.size());
  }

  for (auto i = 0; i < number_of_iterations; i++) {
    SHA256_Update(&sha256, password.data(), password.size());
  }
  SHA256_Final(hash, &sha256);

  std::stringstream ss;
  for (unsigned char salt_char : salt) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (((unsigned int)salt_char) & 0xFFU);
  }
  for (auto hash_char : hash) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash_char;
  }

  return ss.str();
}
#endif

std::string HashPassword(std::string_view password, const uint64_t number_of_iterations, std::string_view salt) {
#if OPENSSL_VERSION_MAJOR >= 3
  return HashPasswordOpenSSL3(password, number_of_iterations, salt);
#else
  return HashPasswordOpenSSL1_1(password, number_of_iterations, salt);
#endif
}

auto ExtractSalt(std::string_view salt_durable) -> std::array<char, SALT_SIZE> {
  static_assert(SALT_SIZE_DURABLE % 2 == 0);
  static_assert(SALT_SIZE_DURABLE / 2 == SALT_SIZE);

  MG_ASSERT(salt_durable.size() == SALT_SIZE_DURABLE);
  auto const *b = salt_durable.cbegin();
  auto const *const e = salt_durable.cend();

  auto salt = std::array<char, SALT_SIZE>{};
  auto *inserter = salt.begin();

  auto const toval = [](char a) -> uint8_t {
    if ('0' <= a && a <= '9') {
      return a - '0';
    }
    if ('a' <= a && a <= 'f') {
      return 10 + (a - 'a');
    }
    MG_ASSERT(false, "Currupt hash, can't extract salt");
    __builtin_unreachable();
  };

  for (; b != e; b += 2, ++inserter) {
    *inserter = static_cast<char>(static_cast<uint8_t>(toval(b[0]) << 4U) | toval(b[1]));
  }
  return salt;
}

bool IsSalted(std::string_view hash) { return hash.size() == SHA_LENGTH + SALT_SIZE_DURABLE; }

bool VerifyPassword(std::string_view password, std::string_view hash, const uint64_t number_of_iterations) {
  auto password_hash = std::invoke([&] {
    if (hash.size() == SHA_LENGTH) [[unlikely]] {
      // Just SHA256
      return HashPassword(password, number_of_iterations, {});
    } else {
      // SHA256 + SALT
      MG_ASSERT(IsSalted(hash));
      auto const salt_durable = std::string_view{hash.data(), SALT_SIZE_DURABLE};
      std::array<char, SALT_SIZE> salt = ExtractSalt(salt_durable);
      return HashPassword(password, number_of_iterations, {salt.data(), salt.size()});
    }
  });
  return password_hash == hash;
}

}  // namespace

}  // namespace SHA

HashedPassword HashPassword(const std::string &password, std::optional<PasswordHashAlgorithm> override_algo) {
  auto const hash_algo = override_algo.value_or(CurrentHashAlgorithm());
  auto password_hash = std::invoke([&] {
    switch (hash_algo) {
      case PasswordHashAlgorithm::BCRYPT: {
        return BCrypt::HashPassword(password);
      }
      case PasswordHashAlgorithm::SHA256:
      case PasswordHashAlgorithm::SHA256_MULTIPLE: {
        auto gen = std::mt19937(std::random_device{}());
        auto salt = std::array<char, SHA::SALT_SIZE>{};
        auto dis = std::uniform_int_distribution<unsigned char>(0, 255);
        std::generate(salt.begin(), salt.end(), [&]() { return dis(gen); });
        auto iterations = (hash_algo == PasswordHashAlgorithm::SHA256) ? ONE_SHA_ITERATION : MULTIPLE_SHA_ITERATIONS;
        return SHA::HashPassword(password, iterations, {salt.data(), salt.size()});
      }
    }
  });
  return HashedPassword{hash_algo, std::move(password_hash)};
};

namespace {

auto InternalParseHashAlgorithm(std::string_view algo) -> PasswordHashAlgorithm {
  auto maybe_parsed = utils::StringToEnum<PasswordHashAlgorithm>(algo, password_hash_mappings);
  if (!maybe_parsed) {
    throw AuthException("Invalid password encryption '{}'!", algo);
  }
  return *maybe_parsed;
}

PasswordHashAlgorithm &InternalCurrentHashAlgorithm() {
  static auto current = PasswordHashAlgorithm::BCRYPT;
  static std::once_flag flag;
  std::call_once(flag, [] { current = InternalParseHashAlgorithm(FLAGS_password_encryption_algorithm); });
  return current;
}
}  // namespace

auto CurrentHashAlgorithm() -> PasswordHashAlgorithm { return InternalCurrentHashAlgorithm(); }

void SetHashAlgorithm(std::string_view algo) {
  auto &current = InternalCurrentHashAlgorithm();
  current = InternalParseHashAlgorithm(algo);
}

auto AsString(PasswordHashAlgorithm hash_algo) -> std::string_view {
  return *utils::EnumToString<PasswordHashAlgorithm>(hash_algo, password_hash_mappings);
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

bool HashedPassword::IsSalted() const {
  switch (hash_algo) {
    case PasswordHashAlgorithm::BCRYPT:
      return true;
    case PasswordHashAlgorithm::SHA256:
    case PasswordHashAlgorithm::SHA256_MULTIPLE:
      return SHA::IsSalted(password_hash);
  }
}

}  // namespace memgraph::auth
