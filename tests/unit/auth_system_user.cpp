// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include "auth/exceptions.hpp"
#include "auth/system_user.hpp"
#include "utils/fips.hpp"

using memgraph::auth::AuthenticateInternalSystemUser;
using memgraph::auth::AuthException;
using memgraph::auth::InternalSystemUserLogin;
using memgraph::auth::IsInternalSystemUsername;
using memgraph::auth::LoadInternalSystemUser;

namespace {
constexpr std::string_view kPassword = "word";
// bcrypt hash of kPassword.
constexpr std::string_view kHash = "bcrypt:$2a$12$ueWpo7FfYrBwoFwBhaCD1ucO4hbwKtOtr9MvxCELJaNq746xhvqYy";
// Right prefix and length, so it parses, but bcrypt can't verify against it.
constexpr std::string_view kMalformedBcryptHash = "bcrypt:$2a$12$!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
// bcrypt hash of the empty password.
constexpr std::string_view kEmptyPasswordHash = "bcrypt:$2a$04$bjGN6kb5Vx/nN3KMfP7Uue1.Qni74z198xOVB7WBYjIdWmXfYlfCK";
constexpr std::string_view kSha256Hash = "sha256:b06a8a6d5a2e2e8f86ed7c18e36dc4c42c1a6b8e4bc1ee6b2ad7c7fd3e1b8a4e";
}  // namespace

// The loaded hash is process-wide and can't be unloaded, so only InternalSystemUser.Authenticate loads one
// successfully; every other test leaves it untouched.
class InternalSystemUser : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::create_directories(dir_); }

  void TearDown() override {
    memgraph::utils::SetFipsStatus({});
    std::filesystem::remove_all(dir_);
  }

  std::filesystem::path WriteHashFile(std::string_view contents) const {
    auto path = dir_ / "hash";
    std::ofstream{path} << contents;
    return path;
  }

  std::filesystem::path dir_{std::filesystem::temp_directory_path() /
                             ("MG_test_unit_auth_system_user_" +
                              std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()})};
};

TEST(InternalSystemUserName, IsReservedCaseInsensitively) {
  EXPECT_TRUE(IsInternalSystemUsername("memgraph_internal"));
  EXPECT_TRUE(IsInternalSystemUsername("MEMGRAPH_INTERNAL"));
  EXPECT_TRUE(IsInternalSystemUsername("Memgraph_Internal"));
  EXPECT_FALSE(IsInternalSystemUsername("memgraph"));
  EXPECT_FALSE(IsInternalSystemUsername("memgraph_internal_"));
  EXPECT_FALSE(IsInternalSystemUsername(""));
}

TEST_F(InternalSystemUser, LoadRejectsMissingFile) {
  EXPECT_THROW(LoadInternalSystemUser(dir_ / "missing"), AuthException);
}

TEST_F(InternalSystemUser, LoadRejectsAnythingButABcryptHash) {
  auto const cases = std::array{std::string{},
                                std::string{kPassword},
                                std::string{kHash.substr(std::string_view{"bcrypt:"}.size())},
                                std::string{kHash} + "x",
                                std::string{kSha256Hash},
                                std::string{kMalformedBcryptHash},
                                std::string{kEmptyPasswordHash}};
  for (auto const &contents : cases) {
    EXPECT_THROW(LoadInternalSystemUser(WriteHashFile(contents)), AuthException) << contents;
  }
}

TEST_F(InternalSystemUser, LoadRejectsBcryptUnderFips) {
  memgraph::utils::SetFipsStatus({.enabled = true});
  EXPECT_THROW(LoadInternalSystemUser(WriteHashFile(kHash)), AuthException);
}

TEST_F(InternalSystemUser, Authenticate) {
  auto const password = std::string{kPassword};
  // Not configured yet: nobody is the internal system user, not even with the right password.
  ASSERT_EQ(AuthenticateInternalSystemUser("memgraph_internal", password), InternalSystemUserLogin::kNotInternal);

  ASSERT_NO_THROW(LoadInternalSystemUser(WriteHashFile("\n  " + std::string{kHash} + "  \n")));

  EXPECT_EQ(AuthenticateInternalSystemUser("memgraph_internal", password), InternalSystemUserLogin::kAccepted);
  EXPECT_EQ(AuthenticateInternalSystemUser("MEMGRAPH_INTERNAL", password), InternalSystemUserLogin::kAccepted);
  EXPECT_EQ(AuthenticateInternalSystemUser("memgraph_internal", "wrong"), InternalSystemUserLogin::kRejected);
  EXPECT_EQ(AuthenticateInternalSystemUser("memgraph_internal", ""), InternalSystemUserLogin::kRejected);
  EXPECT_EQ(AuthenticateInternalSystemUser("admin", password), InternalSystemUserLogin::kNotInternal);
}
