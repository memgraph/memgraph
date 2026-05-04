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
#include <unordered_set>

#include <gtest/gtest.h>

#include "dbms/tenant_profiles.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace {

class TenantProfilesTest : public ::testing::Test {
 protected:
  void SetUp() override { memgraph::utils::EnsureDir(test_folder_); }

  void TearDown() override { std::filesystem::remove_all(test_folder_); }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     ("unit_tenant_profiles_test_" + std::to_string(static_cast<int>(getpid())))};
};

}  // namespace

// This test verifies that created profiles and their database mappings are correctly persisted to the KVStore and
// can be read back after a restart.
TEST_F(TenantProfilesTest, PersistsProfileAndDatabaseMapping) {
  const auto store_path = test_folder_ / "profiles";
  const auto db_prefix = std::string{memgraph::dbms::TenantProfiles::kDbMappingPrefix};

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
    // db1 → reporting, db3 → analytics. db2 was renamed away, so 2 mappings remain.
    EXPECT_EQ(durability.Size(db_prefix), 2);
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
    EXPECT_EQ(durability.Size(db_prefix), 2);
  }
}

// Alter changes the persisted memory_limit and returns the set of attached databases — the
// dbms_handler relies on that set to push the new limit to every attached DB's tracker.
TEST_F(TenantProfilesTest, AlterReturnsAttachedDatabasesAndUpdatesLimit) {
  memgraph::kvstore::KVStore durability{test_folder_ / "alter"};
  memgraph::dbms::TenantProfiles profiles{durability};

  ASSERT_TRUE(profiles.Create("p", 1024));
  ASSERT_EQ(profiles.AttachToDatabase("p", "db1"), 1024);
  ASSERT_EQ(profiles.AttachToDatabase("p", "db2"), 1024);

  auto altered = profiles.Alter("p", 4096);
  ASSERT_TRUE(altered);
  EXPECT_EQ(*altered, std::unordered_set<std::string>({"db1", "db2"}));
  EXPECT_EQ(profiles.Get("p")->memory_limit, 4096);

  auto missing = profiles.Alter("ghost", 1);
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error(), memgraph::dbms::TenantProfiles::AlterError::NOT_FOUND);
}

// Drop is blocked while databases are attached. Detach must clean up both the reverse mapping
// and the forward `databases` set, must be non-idempotent, and must let a subsequent Drop succeed.
TEST_F(TenantProfilesTest, AttachDetachDropLifecycle) {
  memgraph::kvstore::KVStore durability{test_folder_ / "lifecycle"};
  memgraph::dbms::TenantProfiles profiles{durability};
  const auto db_prefix = std::string{memgraph::dbms::TenantProfiles::kDbMappingPrefix};

  ASSERT_TRUE(profiles.Create("p", 1024));
  ASSERT_EQ(profiles.AttachToDatabase("p", "db1"), 1024);

  auto drop_blocked = profiles.Drop("p");
  ASSERT_FALSE(drop_blocked);
  EXPECT_EQ(drop_blocked.error(), memgraph::dbms::TenantProfiles::DropError::HAS_ATTACHED_DATABASES);

  ASSERT_TRUE(profiles.DetachFromDatabase("db1"));
  EXPECT_EQ(profiles.GetProfileForDatabase("db1"), std::nullopt);
  EXPECT_TRUE(profiles.Get("p")->databases.empty());
  EXPECT_EQ(durability.Size(db_prefix), 0);

  auto detach_again = profiles.DetachFromDatabase("db1");
  ASSERT_FALSE(detach_again);
  EXPECT_EQ(detach_again.error(), memgraph::dbms::TenantProfiles::DetachError::NOT_ATTACHED);

  ASSERT_TRUE(profiles.Drop("p"));
  EXPECT_TRUE(profiles.GetAll().empty());
}
