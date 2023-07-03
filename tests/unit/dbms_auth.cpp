// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#ifdef MG_ENTERPRISE

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>

#include "dbms/auth_handler.hpp"
#include "dbms/global.hpp"

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_auth"};

class DBMS_Auth : public ::testing::Test {
 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

 private:
  void Clear() {
    if (!std::filesystem::exists(storage_directory)) return;
    std::filesystem::remove_all(storage_directory);
  }
};

TEST_F(DBMS_Auth, New) {
  memgraph::dbms::AuthContextHandler ah;
  {
    // Clean new
    auto a1 = ah.New("auth1", storage_directory / "auth1");
    ASSERT_TRUE(a1.HasValue() && a1.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "auth1" / "auth"));
  }
  {
    // Try to reuse the directory
    auto a2 = ah.New("auth2", storage_directory / "auth1");
    ASSERT_TRUE(a2.HasError() && a2.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    // Try to reuse the name
    auto a3 = ah.New("auth1", storage_directory / "auth3");
    ASSERT_TRUE(a3.HasError() && a3.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    // Another clean
    auto a4 = ah.New("auth4", storage_directory / "auth4");
    ASSERT_TRUE(a4.HasValue() && a4.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "auth4" / "auth"));
  }
  {
    // Another clean with regex flags
    auto a5 = ah.New("auth5", storage_directory / "auth5", "[0-9]+");
    ASSERT_TRUE(a5.HasValue() && a5.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "auth5" / "auth"));
  }
}

TEST_F(DBMS_Auth, Get) {
  memgraph::dbms::AuthContextHandler ah;

  ASSERT_FALSE(ah.Get("auth1"));

  ASSERT_TRUE(ah.New("auth1", storage_directory / "auth1").HasValue());
  ASSERT_TRUE(ah.New("auth2", storage_directory / "auth2").HasValue());
  ASSERT_TRUE(ah.New("auth3", storage_directory / "three").HasValue());

  ASSERT_TRUE(ah.Get("auth1"));
  ASSERT_TRUE(ah.Get("auth2"));
  ASSERT_TRUE(ah.Get("auth3"));
}

TEST_F(DBMS_Auth, Delete) {
  memgraph::dbms::AuthContextHandler ah;

  ASSERT_FALSE(ah.Delete("auth1"));

  ASSERT_TRUE(ah.New("auth1", storage_directory / "auth1").HasValue());
  ASSERT_TRUE(ah.New("auth2", storage_directory / "auth2").HasValue());
  ASSERT_TRUE(ah.New("auth3", storage_directory / "three").HasValue());

  ASSERT_TRUE(ah.Delete("auth1"));
  ASSERT_FALSE(ah.Get("auth1"));
  ASSERT_TRUE(ah.New("auth1", storage_directory / "auth1").HasValue());
  ASSERT_TRUE(ah.Get("auth1"));
  ASSERT_TRUE(ah.Delete("auth1"));
  ASSERT_FALSE(ah.Get("auth1"));
  ASSERT_TRUE(ah.Get("auth3"));
}

#endif
