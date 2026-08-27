// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//
#include "auth/crypto.hpp"

#include <bcrypt.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/opensslv.h>
#include <openssl/params.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/types.h>
#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <expected>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <utility>

#include "auth/exceptions.hpp"
#include "utils/enum.hpp"
#include "utils/exit_codes.hpp"
#include "utils/fips.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"
#include "utils/startup_failure.hpp"

namespace {
using namespace std::literals;

constexpr auto kHashAlgo = "hash_algo";
constexpr auto kPasswordHash = "password_hash";

// Needs to be stable user queries depend on this
inline constexpr std::array password_hash_mappings{
    std::pair{"bcrypt"sv, memgraph::auth::PasswordHashAlgorithm::BCRYPT},
    std::pair{"sha256"sv, memgraph::auth::PasswordHashAlgorithm::SHA256},
    std::pair{"sha256-multiple"sv, memgraph::auth::PasswordHashAlgorithm::SHA256_MULTIPLE},
    std::pair{"pbkdf2-sha256"sv, memgraph::auth::PasswordHashAlgorithm::PBKDF2_SHA256}};

inline constexpr uint64_t ONE_SHA_ITERATION = 1;
inline constexpr uint64_t MULTIPLE_SHA_ITERATIONS = 1024;
}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,misc-unused-parameters)
DEFINE_VALIDATED_string(password_encryption_algorithm, "bcrypt",
                        "The password encryption algorithm used for authentication.", {
                          if (const auto result =
                                  memgraph::utils::IsValidEnumValueString(value, password_hash_mappings);
                              !result.has_value()) {
                            const auto error = result.error();
                            switch (error) {
                              case memgraph::utils::ValidationError::EmptyValue: {
                                std::cerr << "Password encryption algorithm cannot be empty." << std::endl;
                                break;
                              }
                              case memgraph::utils::ValidationError::InvalidValue: {
                                std::cerr << "Invalid value for password encryption algorithm. Allowed values: "
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

namespace {
/// Salt from OpenSSL's DRBG, which SP 800-132 requires for an approved KDF.
/// The legacy SHA algorithms still salt from `std::mt19937`; they are not
/// FIPS-approvable regardless, so that path is deliberately left alone.
template <std::size_t N>
auto GenerateSalt() -> std::array<char, N> {
  auto salt = std::array<char, N>{};
  static_assert(N <= std::numeric_limits<int>::max());
  if (RAND_bytes(reinterpret_cast<unsigned char *>(salt.data()), static_cast<int>(N)) != 1) {
    throw AuthException("Couldn't generate a password salt!");
  }
  return salt;
}

auto ToHex(std::span<const unsigned char> bytes) -> std::string {
  auto out = std::string{};
  out.reserve(bytes.size() * 2);
  for (auto const byte : bytes) {
    fmt::format_to(std::back_inserter(out), "{:02x}", byte);
  }
  return out;
}

auto AsBytes(std::string_view sv) -> std::span<const unsigned char> {
  return {reinterpret_cast<const unsigned char *>(sv.data()), sv.size()};
}

/// Refuse a non-approved algorithm while in FIPS mode. Applied at every entry
/// point that hashes or verifies, because none of the legacy algorithms fail
/// on their own: bcrypt bypasses OpenSSL entirely, and the sha256 variants use
/// an approved digest in an unapproved construction.
void EnsureFipsApproved(PasswordHashAlgorithm hash_algo) {
  if (utils::FipsEnabled() && !IsFipsApproved(hash_algo)) {
    throw AuthException("The '{}' password hash algorithm is not permitted in FIPS mode; use '{}'.",
                        AsString(hash_algo),
                        AsString(PasswordHashAlgorithm::PBKDF2_SHA256));
  }
}
}  // namespace

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
    std::unreachable();
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

namespace PBKDF2 {

namespace {

// SP 800-132 floors, as OpenSSL enforces them: >= 1000 iterations,
// >= 128-bit salt, >= 112-bit derived key.
constexpr auto ITERATIONS = 600'000U;
constexpr auto SALT_SIZE = 16U;         // 128-bit
constexpr auto DERIVED_KEY_SIZE = 32U;  // 256-bit
constexpr auto SALT_SIZE_DURABLE = SALT_SIZE * 2;
constexpr auto HASH_LENGTH = DERIVED_KEY_SIZE * 2;

// The iteration count is not stored in the hash, so it cannot be raised
// without invalidating every existing one. If OWASP guidance moves past 600k,
// add a new PasswordHashAlgorithm rather than editing ITERATIONS -- the same
// way sha256 and sha256-multiple are separate algorithms.
static_assert(ITERATIONS >= 1000);
static_assert(SALT_SIZE * 8 >= 128);
static_assert(DERIVED_KEY_SIZE * 8 >= 112);

// Hex decoding is shared with the SHA path, which hardcodes its own salt size.
static_assert(SALT_SIZE == SHA::SALT_SIZE);
static_assert(SALT_SIZE_DURABLE == SHA::SALT_SIZE_DURABLE);

struct KdfDeleter {
  void operator()(EVP_KDF *kdf) const noexcept { EVP_KDF_free(kdf); }
};

struct KdfCtxDeleter {
  void operator()(EVP_KDF_CTX *ctx) const noexcept { EVP_KDF_CTX_free(ctx); }
};

auto Derive(std::string_view password, std::string_view salt) -> std::array<unsigned char, DERIVED_KEY_SIZE> {
  auto const kdf = std::unique_ptr<EVP_KDF, KdfDeleter>{EVP_KDF_fetch(nullptr, "PBKDF2", nullptr)};
  if (!kdf) {
    throw AuthException("Couldn't fetch the PBKDF2 implementation!");
  }
  auto const ctx = std::unique_ptr<EVP_KDF_CTX, KdfCtxDeleter>{EVP_KDF_CTX_new(kdf.get())};
  if (!ctx) {
    throw AuthException("Couldn't create a PBKDF2 context!");
  }

  // Copied so that `.data()` is never null, which an empty string_view allows
  // but OpenSSL does not accept.
  auto password_buffer = std::string{password};
  auto salt_buffer = std::string{salt};
  auto digest = std::string{"SHA2-256"};
  auto iterations = ITERATIONS;
  // 0 keeps the SP 800-132 lower-bound checks on. Stated explicitly so the
  // intent is visible rather than inherited from an OpenSSL default.
  auto legacy_pkcs5_mode = 0;

  auto params = std::array{
      OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_PASSWORD, password_buffer.data(), password_buffer.size()),
      OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, salt_buffer.data(), salt_buffer.size()),
      OSSL_PARAM_construct_uint(OSSL_KDF_PARAM_ITER, &iterations),
      OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, digest.data(), 0),
      OSSL_PARAM_construct_int(OSSL_KDF_PARAM_PKCS5, &legacy_pkcs5_mode),
      OSSL_PARAM_construct_end(),
  };

  auto derived = std::array<unsigned char, DERIVED_KEY_SIZE>{};
  if (EVP_KDF_derive(ctx.get(), derived.data(), derived.size(), params.data()) != 1) {
    throw AuthException("Couldn't hash the password!");
  }
  return derived;
}

}  // namespace

/// Stored as hex(salt) || hex(derived key), matching the shape of the salted
/// SHA hashes so the durable form stays fixed-width.
std::string HashPassword(std::string_view password, std::string_view salt) {
  MG_ASSERT(salt.size() == SALT_SIZE);
  return ToHex(AsBytes(salt)) + ToHex(Derive(password, salt));
}

bool VerifyPassword(std::string_view password, std::string_view hash) {
  // Always salted, unlike SHA256 which has a legacy unsalted form.
  if (hash.size() != SALT_SIZE_DURABLE + HASH_LENGTH) {
    return false;
  }
  auto const salt = SHA::ExtractSalt(hash.substr(0, SALT_SIZE_DURABLE));
  return HashPassword(password, {salt.data(), salt.size()}) == hash;
}

}  // namespace PBKDF2

HashedPassword HashPassword(const std::string &password, std::optional<PasswordHashAlgorithm> override_algo) {
  auto const hash_algo = override_algo.value_or(CurrentHashAlgorithm());
  EnsureFipsApproved(hash_algo);
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
      case PasswordHashAlgorithm::PBKDF2_SHA256: {
        auto const salt = GenerateSalt<PBKDF2::SALT_SIZE>();
        return PBKDF2::HashPassword(password, {salt.data(), salt.size()});
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

std::optional<std::string_view> UsesAlgo(std::string_view str, PasswordHashAlgorithm algo) {
  // header = algo name + :
  const auto header = std::string{AsString(algo)} + ":";
  const auto hash_size = HashSize(algo).unsalted;  // Support only unsalted hashes
  if (str.size() == header.size() + hash_size) {
    int i = 0;
    if (std::ranges::all_of(header, [&](const auto ch) { return tolower(ch) == str[i++]; })) {
      return str.substr(header.size());
    }
  }
  return {};
}
}  // namespace

// NOTE: Deliberately no pbkdf2-sha256 branch. A user-supplied hash carries no
// iteration count, so accepting one would mean assuming it was derived with
// ITERATIONS when it may have used the SP 800-132 floor of 1000 -- we would
// verify a far weaker hash as though it were strong. Set a pbkdf2 password
// through the plaintext path instead.
std::optional<HashedPassword> UserDefinedHash(std::string_view password) {
  for (auto const algo :
       {PasswordHashAlgorithm::BCRYPT, PasswordHashAlgorithm::SHA256, PasswordHashAlgorithm::SHA256_MULTIPLE}) {
    if (const auto hash = UsesAlgo(password, algo)) {
      // Must throw, not fall through to `{}`: Auth::UpdatePassword treats an
      // empty result as a plaintext password, which would silently store the
      // literal "bcrypt:$2a$..." string as the user's password.
      EnsureFipsApproved(algo);
      return HashedPassword{algo, std::string{*hash}};
    }
  }
  return {};
}

auto CurrentHashAlgorithm() -> PasswordHashAlgorithm { return InternalCurrentHashAlgorithm(); }

auto IsFipsApproved(PasswordHashAlgorithm hash_algo) -> bool {
  switch (hash_algo) {
    case PasswordHashAlgorithm::BCRYPT:
    case PasswordHashAlgorithm::SHA256:
    case PasswordHashAlgorithm::SHA256_MULTIPLE:
      return false;
    case PasswordHashAlgorithm::PBKDF2_SHA256:
      return true;
  }
}

void EnableFipsMode() {
  utils::SetFipsStatus({.enabled = true});

  // Fail here rather than at the first login. bcrypt does not go through EVP,
  // so without this check it would keep hashing happily under an active FIPS
  // provider and ship a silent compliance violation.
  auto const configured = CurrentHashAlgorithm();
  if (!IsFipsApproved(configured)) {
    utils::FailStartup(
        utils::ExitCode::FipsModeUnsupportedPasswordAlgorithm,
        fmt::format("--fips-mode=true is incompatible with --password-encryption-algorithm={}. Only '{}' is approved.",
                    AsString(configured),
                    AsString(PasswordHashAlgorithm::PBKDF2_SHA256)));
  }
}

void SetHashAlgorithm(std::string_view algo) {
  auto &current = InternalCurrentHashAlgorithm();
  current = InternalParseHashAlgorithm(algo);
}

auto AsString(PasswordHashAlgorithm hash_algo) -> std::string_view {
  return *utils::EnumToString<PasswordHashAlgorithm>(hash_algo, password_hash_mappings);
}

auto HashSize(PasswordHashAlgorithm hash_algo) -> struct HashSize {
  switch (hash_algo) {
    case PasswordHashAlgorithm::BCRYPT:
      return {60, 60};  // NOTE: BCRYPT_HASHSIZE is 64, but the result is actually 60B
    case PasswordHashAlgorithm::SHA256:
    case PasswordHashAlgorithm::SHA256_MULTIPLE:
      return {SHA::SHA_LENGTH, SHA::SHA_LENGTH + SHA::SALT_SIZE_DURABLE};
    case PasswordHashAlgorithm::PBKDF2_SHA256:
      return {PBKDF2::HASH_LENGTH, PBKDF2::HASH_LENGTH + PBKDF2::SALT_SIZE_DURABLE};
  }

}

bool HashedPassword::VerifyPassword(const std::string &password) {
  // Throws rather than returning false: a legacy hash under FIPS mode is a
  // migration problem the operator has to see, not a wrong password.
  EnsureFipsApproved(hash_algo);
  switch (hash_algo) {
    case PasswordHashAlgorithm::BCRYPT:
      return BCrypt::VerifyPassword(password, password_hash);
    case PasswordHashAlgorithm::SHA256:
      return SHA::VerifyPassword(password, password_hash, ONE_SHA_ITERATION);
    case PasswordHashAlgorithm::SHA256_MULTIPLE:
      return SHA::VerifyPassword(password, password_hash, MULTIPLE_SHA_ITERATIONS);
    case PasswordHashAlgorithm::PBKDF2_SHA256:
      return PBKDF2::VerifyPassword(password, password_hash);
  }
}

void to_json(nlohmann::json &j, const HashedPassword &p) {
  j = nlohmann::json{{kHashAlgo, p.hash_algo}, {kPasswordHash, p.password_hash}};
}

void from_json(const nlohmann::json &j, HashedPassword &p) {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  PasswordHashAlgorithm hash_algo;
  j.at(kHashAlgo).get_to(hash_algo);
  std::string password_hash = j.at(kPasswordHash);
  p = HashedPassword{hash_algo, std::move(password_hash)};
}

bool HashedPassword::IsSalted() const {
  switch (hash_algo) {
    case PasswordHashAlgorithm::BCRYPT:
      return true;
    case PasswordHashAlgorithm::SHA256:
    case PasswordHashAlgorithm::SHA256_MULTIPLE:
      return SHA::IsSalted(password_hash);
    case PasswordHashAlgorithm::PBKDF2_SHA256:
      return true;
  }
}

}  // namespace memgraph::auth
