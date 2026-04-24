// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <unistd.h>

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "dbms/tenant_profiles.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace {

const std::string kLegacyDbMappingPrefix = "db_tenant_profile:";

class TenantProfilesTest : public ::testing::Test {
 protected:
  void SetUp() override { memgraph::utils::EnsureDir(test_folder_); }

  void TearDown() override { std::filesystem::remove_all(test_folder_); }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     ("unit_tenant_profiles_test_" + std::to_string(static_cast<int>(getpid())))};
};

}  // namespace

TEST_F(TenantProfilesTest, PersistsProfileDatabaseSetsAndRebuildsReverseMap) {
  const auto store_path = test_folder_ / "profiles";

  {
    memgraph::kvstore::KVStore durability{store_path};
    memgraph::dbms::TenantProfiles profiles{durability};

    ASSERT_TRUE(profiles.Create("analytics", 1024));
    ASSERT_TRUE(profiles.Create("reporting", 2048));

    ASSERT_EQ(profiles.AttachToDatabase("analytics", "db1"), 1024);
    ASSERT_EQ(profiles.AttachToDatabase("analytics", "db2"), 1024);
    ASSERT_EQ(profiles.AttachToDatabase("reporting", "db1"), 2048);
    ASSERT_TRUE(profiles.RenameDatabase("db2", "db3"));

    EXPECT_EQ(durability.Get(memgraph::dbms::TenantProfiles::kVersionKey), memgraph::dbms::TenantProfiles::kVersion);
    EXPECT_EQ(durability.Size(kLegacyDbMappingPrefix), 0);
  }

  {
    memgraph::kvstore::KVStore durability{store_path};
    memgraph::dbms::TenantProfiles profiles{durability};

    EXPECT_EQ(profiles.GetProfileForDatabase("db1"), "reporting");
    EXPECT_EQ(profiles.GetProfileForDatabase("db3"), "analytics");
    EXPECT_EQ(profiles.GetProfileForDatabase("db2"), std::nullopt);

    const auto analytics = profiles.Get("analytics");
    ASSERT_TRUE(analytics);
    EXPECT_EQ(analytics->memory_limit, 1024);
    EXPECT_EQ(analytics->databases.size(), 1);
    EXPECT_TRUE(analytics->databases.contains("db3"));

    const auto reporting = profiles.Get("reporting");
    ASSERT_TRUE(reporting);
    EXPECT_EQ(reporting->memory_limit, 2048);
    EXPECT_EQ(reporting->databases.size(), 1);
    EXPECT_TRUE(reporting->databases.contains("db1"));
    EXPECT_EQ(durability.Size(kLegacyDbMappingPrefix), 0);
  }
}

TEST_F(TenantProfilesTest, RemovesLegacyDatabaseMappingKeysOnStartup) {
  memgraph::kvstore::KVStore durability{test_folder_ / "legacy"};

  ASSERT_TRUE(durability.Put("db_tenant_profile:db1", "analytics"));
  ASSERT_EQ(durability.Size(kLegacyDbMappingPrefix), 1);

  memgraph::dbms::TenantProfiles profiles{durability};
  EXPECT_EQ(durability.Size(kLegacyDbMappingPrefix), 0);
}
