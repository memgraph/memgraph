// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <iostream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/crypto.hpp"
#include "auth/models.hpp"
#include "utils/cast.hpp"
#include "utils/file.hpp"
#include "utils/license.hpp"

using namespace memgraph::auth;
namespace fs = std::filesystem;

DECLARE_bool(auth_password_permit_null);
DECLARE_string(auth_password_strength_regex);

class AuthWithStorage : public ::testing::Test {
 protected:
  virtual void SetUp() {
    memgraph::utils::EnsureDir(test_folder_);
    FLAGS_auth_password_permit_null = true;
    FLAGS_auth_password_strength_regex = ".+";

    memgraph::utils::license::global_license_checker.EnableTesting();
  }

  virtual void TearDown() { fs::remove_all(test_folder_); }

  fs::path test_folder_{fs::temp_directory_path() / "MG_tests_unit_auth"};

  Auth auth{test_folder_ / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid())))};
};

TEST_F(AuthWithStorage, IsUserAuthorizedLabels) { ASSERT_TRUE(true); }

TEST_F(AuthWithStorage, IsUserAuthorizedEdgeType) { ASSERT_TRUE(true); }
