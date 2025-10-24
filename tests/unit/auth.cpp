// Copyright 2025 Memgraph Ltd.
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
#include <optional>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/crypto.hpp"
#include "auth/models.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "glue/auth_global.hpp"
#include "license/license.hpp"
#include "utils/cast.hpp"
#include "utils/file.hpp"

#include <boost/dll/runtime_symbol_info.hpp>
#include <nlohmann/json.hpp>

using namespace memgraph::auth;
using namespace std::literals;
namespace fs = std::filesystem;

// @TODO extend fine-grained permission tests with multi-label and
// MATCHING ANY / MATCHING EXACTLY logic.

DECLARE_string(password_encryption_algorithm);

class AuthWithStorage : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder);
    memgraph::license::global_license_checker.EnableTesting();
    auth.emplace(test_folder / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid()))), auth_config);
  }

  void TearDown() override { fs::remove_all(test_folder); }

  fs::path test_folder{fs::temp_directory_path() / "MG_tests_unit_auth"};
  Auth::Config auth_config{};
  std::optional<Auth> auth{};
};

class V1Auth : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder);
    memgraph::license::global_license_checker.EnableTesting();
    auth.emplace(test_folder, auth_config);
  }

  void TearDown() override {}

  fs::path test_folder{fs::path{boost::dll::program_location().parent_path().string()} / "auth_kvstore/v1"};
  Auth::Config auth_config{};
  std::optional<Auth> auth{};
};

TEST_F(AuthWithStorage, AddRole) {
  ASSERT_TRUE(auth->AddRole("admin"));
  ASSERT_TRUE(auth->AddRole("user"));
  ASSERT_FALSE(auth->AddRole("admin"));
}

TEST_F(AuthWithStorage, RemoveRole) {
  ASSERT_TRUE(auth->AddRole("admin"));
  ASSERT_TRUE(auth->RemoveRole("admin"));
  ASSERT_FALSE(auth->HasUsers());
  ASSERT_FALSE(auth->RemoveUser("test2"));
  ASSERT_FALSE(auth->RemoveUser("test"));
  ASSERT_FALSE(auth->HasUsers());
}

TEST_F(AuthWithStorage, Authenticate) {
  ASSERT_FALSE(auth->HasUsers());

  auto user = auth->AddUser("test");
  ASSERT_NE(user, std::nullopt);
  ASSERT_TRUE(auth->HasUsers());

  ASSERT_TRUE(auth->Authenticate("test", "123"));
  ASSERT_TRUE(auth->Authenticate("test", ""));

  user->UpdatePassword("123");
  auth->SaveUser(*user);

  ASSERT_NE(auth->Authenticate("test", "123"), std::nullopt);

  ASSERT_EQ(auth->Authenticate("test", "456"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "123"), std::nullopt);

  const auto bcrpyt_hash = HashPassword("456", PasswordHashAlgorithm::BCRYPT);
  user->UpdateHash(bcrpyt_hash);
  auth->SaveUser(*user);

  ASSERT_NE(auth->Authenticate("test", "456"), std::nullopt);

  ASSERT_EQ(auth->Authenticate("test", "123"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "456"), std::nullopt);

  const auto sha256_hash = HashPassword("789", PasswordHashAlgorithm::SHA256);
  user->UpdateHash(sha256_hash);
  auth->SaveUser(*user);

  ASSERT_NE(auth->Authenticate("test", "789"), std::nullopt);

  ASSERT_EQ(auth->Authenticate("test", "456"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "789"), std::nullopt);

  const auto sha256_mul_hash = HashPassword("012", PasswordHashAlgorithm::SHA256_MULTIPLE);
  user->UpdateHash(sha256_mul_hash);
  auth->SaveUser(*user);

  ASSERT_NE(auth->Authenticate("test", "012"), std::nullopt);

  ASSERT_EQ(auth->Authenticate("test", "567"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "012"), std::nullopt);

  user->UpdatePassword();
  auth->SaveUser(*user);

  ASSERT_NE(auth->Authenticate("test", "123"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "456"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "789"), std::nullopt);
  ASSERT_NE(auth->Authenticate("test", "012"), std::nullopt);

  ASSERT_EQ(auth->Authenticate("nonexistant", "123"), std::nullopt);
}

TEST_F(AuthWithStorage, UserRolePermissions) {
  ASSERT_FALSE(auth->HasUsers());
  ASSERT_TRUE(auth->AddUser("test"));
  ASSERT_TRUE(auth->HasUsers());

  auto user = auth->GetUser("test");
  ASSERT_NE(user, std::nullopt);

  // Test initial user permissions.
  ASSERT_EQ(user->permissions().Has(Permission::MATCH), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions().Has(Permission::CREATE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions().Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions().Has(Permission::DELETE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions(), user->GetPermissions());

  // Change one user permission.
  user->permissions().Grant(Permission::MATCH);

  // Check permissions.
  ASSERT_EQ(user->permissions().Has(Permission::MATCH), PermissionLevel::GRANT);
  ASSERT_EQ(user->permissions().Has(Permission::CREATE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions().Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions().Has(Permission::DELETE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(user->permissions(), user->GetPermissions());

  // Create role.
  ASSERT_TRUE(auth->AddRole("admin"));
  auto role = auth->GetRole("admin");
  ASSERT_NE(role, std::nullopt);

  // Assign permissions to role and role to user.
  role->permissions().Grant(Permission::DELETE);
  user->ClearAllRoles();
  user->AddRole(*role);

  // Check permissions.
  {
    auto permissions = user->GetPermissions();
    ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
    ASSERT_EQ(permissions.Has(Permission::DELETE), PermissionLevel::GRANT);
    ASSERT_EQ(permissions.Has(Permission::CREATE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  }

  // Add explicit deny to role.
  role->permissions().Deny(Permission::MATCH);
  user->ClearAllRoles();
  user->AddRole(*role);

  // Check permissions.
  {
    auto permissions = user->GetPermissions();
    ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::DENY);
    ASSERT_EQ(permissions.Has(Permission::DELETE), PermissionLevel::GRANT);
    ASSERT_EQ(permissions.Has(Permission::CREATE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  }
}

#ifdef MG_ENTERPRISE
TEST_F(AuthWithStorage, UserRoleFineGrainedAccessHandler) {
  ASSERT_FALSE(auth->HasUsers());
  ASSERT_TRUE(auth->AddUser("test"));
  ASSERT_TRUE(auth->HasUsers());

  auto user = auth->GetUser("test");
  ASSERT_NE(user, std::nullopt);

  // Test initial user fine grained access permissions.
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions(), FineGrainedAccessPermissions{});
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions(), FineGrainedAccessPermissions{});
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions(), user->GetFineGrainedAccessLabelPermissions());
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions(),
            user->GetFineGrainedAccessEdgeTypePermissions());

  // Grant one label to user .
  user->fine_grained_access_handler().label_permissions().Grant({"labelTest"s}, FineGrainedPermission::CREATE);
  user->fine_grained_access_handler().label_permissions().Grant({"labelTest"s}, FineGrainedPermission::UPDATE);
  // Grant one edge type to user .
  user->fine_grained_access_handler().edge_type_permissions().Grant({"edgeTypeTest"s}, FineGrainedPermission::READ);
  user->fine_grained_access_handler().edge_type_permissions().Grant({"edgeTypeTest"s}, FineGrainedPermission::DELETE);

  // Check permissions.
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest"s},
                                                                        FineGrainedPermission::CREATE),
            PermissionLevel::GRANT);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest"s},
                                                                        FineGrainedPermission::READ),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest"s},
                                                                        FineGrainedPermission::UPDATE),
            PermissionLevel::GRANT);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest"s},
                                                                        FineGrainedPermission::DELETE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest"s},
                                                                            FineGrainedPermission::CREATE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest"s},
                                                                            FineGrainedPermission::READ),
            PermissionLevel::GRANT);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest"s},
                                                                            FineGrainedPermission::UPDATE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest"s},
                                                                            FineGrainedPermission::DELETE),
            PermissionLevel::GRANT);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions(), user->GetFineGrainedAccessLabelPermissions());
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions(),
            user->GetFineGrainedAccessEdgeTypePermissions());

  // Check permissions on labels and edge types to which the user has no privileges
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest1"s},
                                                                        FineGrainedPermission::CREATE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest1"s},
                                                                        FineGrainedPermission::READ),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest1"s},
                                                                        FineGrainedPermission::UPDATE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions().Has(std::array{"labelTest1"s},
                                                                        FineGrainedPermission::DELETE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest1"s},
                                                                            FineGrainedPermission::CREATE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest1"s},
                                                                            FineGrainedPermission::READ),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest1"s},
                                                                            FineGrainedPermission::UPDATE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions().Has(std::array{"edgeTypeTest1"s},
                                                                            FineGrainedPermission::DELETE),
            PermissionLevel::DENY);
  ASSERT_EQ(user->fine_grained_access_handler().label_permissions(), user->GetFineGrainedAccessLabelPermissions());
  ASSERT_EQ(user->fine_grained_access_handler().edge_type_permissions(),
            user->GetFineGrainedAccessEdgeTypePermissions());

  // Create role.
  ASSERT_TRUE(auth->AddRole("admin"));
  auto role = auth->GetRole("admin");
  ASSERT_NE(role, std::nullopt);

  // Grant label and edge type to role, and role to user.
  role->fine_grained_access_handler().label_permissions().Grant({"roleLabelTest"s}, FineGrainedPermission::CREATE);
  role->fine_grained_access_handler().label_permissions().Grant({"roleLabelTest"s}, FineGrainedPermission::READ);
  role->fine_grained_access_handler().edge_type_permissions().Grant({"roleEdgeTypeTest"s},
                                                                    FineGrainedPermission::UPDATE);
  role->fine_grained_access_handler().edge_type_permissions().Grant({"roleEdgeTypeTest"s},
                                                                    FineGrainedPermission::DELETE);
  user->AddRole(*role);

  // Check permissions.
  {
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest"s}, FineGrainedPermission::CREATE),
        PermissionLevel::GRANT);
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest"s}, FineGrainedPermission::READ),
        PermissionLevel::GRANT);
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest"s}, FineGrainedPermission::UPDATE),
        PermissionLevel::DENY);
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest"s}, FineGrainedPermission::DELETE),
        PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest"s},
                                                                  FineGrainedPermission::CREATE),
              PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest"s},
                                                                  FineGrainedPermission::READ),
              PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest"s},
                                                                  FineGrainedPermission::UPDATE),
              PermissionLevel::GRANT);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest"s},
                                                                  FineGrainedPermission::DELETE),
              PermissionLevel::GRANT);
  }

  user->ClearAllRoles();
  user->AddRole(*role);

  // Check silent deny permissions against labels to which the role has no privileges.
  {
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest1"s}, FineGrainedPermission::CREATE),
        PermissionLevel::DENY);
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest1"s}, FineGrainedPermission::READ),
        PermissionLevel::DENY);
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest1"s}, FineGrainedPermission::UPDATE),
        PermissionLevel::DENY);
    ASSERT_EQ(
        user->GetFineGrainedAccessLabelPermissions().Has(std::array{"roleLabelTest1"s}, FineGrainedPermission::DELETE),
        PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest1"s},
                                                                  FineGrainedPermission::CREATE),
              PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest1"s},
                                                                  FineGrainedPermission::READ),
              PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest1"s},
                                                                  FineGrainedPermission::UPDATE),
              PermissionLevel::DENY);
    ASSERT_EQ(user->GetFineGrainedAccessEdgeTypePermissions().Has(std::array{"roleEdgeTypeTest1"s},
                                                                  FineGrainedPermission::DELETE),
              PermissionLevel::DENY);
  }
}

TEST_F(AuthWithStorage, DatabaseSpecificAccess) {
  // Create a user and multiple roles with different database access
  auto user = auth->AddUser("user");
  ASSERT_TRUE(user);
  auto role1 = auth->AddRole("role1");
  auto role2 = auth->AddRole("role2");
  auto role3 = auth->AddRole("role3");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  ASSERT_TRUE(role3);

  // Grant different fine-grained permissions to each role
  role1->fine_grained_access_handler().label_permissions().Grant({"label1"s}, FineGrainedPermission::CREATE);
  role1->fine_grained_access_handler().label_permissions().Grant({"label1"s}, FineGrainedPermission::READ);
  role1->fine_grained_access_handler().label_permissions().Grant({"label1"s}, FineGrainedPermission::UPDATE);
  role1->fine_grained_access_handler().label_permissions().Grant({"label1"s}, FineGrainedPermission::DELETE);
  role1->fine_grained_access_handler().edge_type_permissions().Grant({"edge1"s}, FineGrainedPermission::READ);

  role2->fine_grained_access_handler().label_permissions().Grant({"label2"s}, FineGrainedPermission::READ);
  role2->fine_grained_access_handler().label_permissions().Grant({"label2"s}, FineGrainedPermission::UPDATE);
  role2->fine_grained_access_handler().edge_type_permissions().Grant({"edge2"s}, FineGrainedPermission::CREATE);
  role2->fine_grained_access_handler().edge_type_permissions().Grant({"edge2"s}, FineGrainedPermission::READ);
  role2->fine_grained_access_handler().edge_type_permissions().Grant({"edge2"s}, FineGrainedPermission::UPDATE);
  role2->fine_grained_access_handler().edge_type_permissions().Grant({"edge2"s}, FineGrainedPermission::DELETE);

  role3->fine_grained_access_handler().label_permissions().Grant({"label3"s}, FineGrainedPermission::READ);
  role3->fine_grained_access_handler().edge_type_permissions().Grant({"edge3"s}, FineGrainedPermission::READ);
  role3->fine_grained_access_handler().edge_type_permissions().Grant({"edge3"s}, FineGrainedPermission::UPDATE);

  // Grant database access to roles
  role1->db_access().Grant("db1");
  role2->db_access().Grant("db2");
  role3->db_access().Grant("db3");
  role3->db_access().Deny("db4");

  // Add all roles to user
  user->AddRole(*role1);
  user->AddRole(*role2);
  user->AddRole(*role3);

  // Update storage
  auth->SaveRole(*role1);
  auth->SaveRole(*role2);
  auth->SaveRole(*role3);
  auth->SaveUser(*user);
  user = auth->GetUser("user");

  // Test -1: check that correct roles are returned
  {
    auto roles = user->GetRoles();
    ASSERT_EQ(roles.size(), 3);
    ASSERT_EQ(roles.count(*role1), 1);
    ASSERT_EQ(roles.count(*role2), 1);
    ASSERT_EQ(roles.count(*role3), 1);

    auto roles_db1 = user->GetRoles("db1");
    ASSERT_EQ(roles_db1.size(), 1);
    ASSERT_EQ(roles_db1.count(*role1), 1);

    auto roles_db2 = user->GetRoles("db2");
    ASSERT_EQ(roles_db2.size(), 1);
    ASSERT_EQ(roles_db2.count(*role2), 1);

    auto roles_db3 = user->GetRoles("db3");
    ASSERT_EQ(roles_db3.size(), 1);
    ASSERT_EQ(roles_db3.count(*role3), 1);

    auto roles_db4 = user->GetRoles("db4");
    ASSERT_EQ(roles_db4.size(), 0);
  }

  // Test 0: Generic access check
  {
    ASSERT_TRUE(user->HasAccess("db1"));
    ASSERT_TRUE(user->HasAccess("db2"));
    ASSERT_TRUE(user->HasAccess("db3"));
    ASSERT_FALSE(user->HasAccess("db4"));

    ASSERT_FALSE(user->has_access("db1"));
    ASSERT_FALSE(user->has_access("db2"));
    ASSERT_FALSE(user->has_access("db3"));
    ASSERT_FALSE(user->has_access("db4"));
    user->db_access().Grant("db4");
    ASSERT_TRUE(user->has_access("db4"));
    ASSERT_FALSE(user->HasAccess("db4"));

    role3->db_access().Grant("db4");

    // Update linkage
    auth->SaveRole(*role3);
    auth->SaveUser(*user);
    user = auth->GetUser("user");

    ASSERT_TRUE(user->HasAccess("db4"));
  }

  // Test 1: No database filter - should include all roles
  {
    auto all_label_perms = user->GetFineGrainedAccessLabelPermissions();
    auto all_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions();

    // Should have permissions from all roles
    ASSERT_EQ(all_label_perms.Has(std::array{"label1"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(all_label_perms.Has(std::array{"label2"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(all_label_perms.Has(std::array{"label3"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);

    ASSERT_EQ(all_edge_perms.Has(std::array{"edge1"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(all_edge_perms.Has(std::array{"edge2"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(all_edge_perms.Has(std::array{"edge3"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
  }

  // Test 2: Filter by db1 - should only include role1
  {
    auto db1_label_perms = user->GetFineGrainedAccessLabelPermissions("db1");
    auto db1_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions("db1");

    // Should have permissions from role1 only
    ASSERT_EQ(db1_label_perms.Has(std::array{"label1"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db1_label_perms.Has(std::array{"label2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db1_label_perms.Has(std::array{"label3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);

    ASSERT_EQ(db1_edge_perms.Has(std::array{"edge1"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db1_edge_perms.Has(std::array{"edge2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db1_edge_perms.Has(std::array{"edge3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
  }

  // Test 3: Filter by db2 - should only include role2
  {
    auto db2_label_perms = user->GetFineGrainedAccessLabelPermissions("db2");
    auto db2_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions("db2");

    // Should have permissions from role2 only
    ASSERT_EQ(db2_label_perms.Has(std::array{"label1"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db2_label_perms.Has(std::array{"label2"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db2_label_perms.Has(std::array{"label3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);

    ASSERT_EQ(db2_edge_perms.Has(std::array{"edge1"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db2_edge_perms.Has(std::array{"edge2"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db2_edge_perms.Has(std::array{"edge3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
  }

  // Test 4: Filter by db3 - should only include role3
  {
    auto db3_label_perms = user->GetFineGrainedAccessLabelPermissions("db3");
    auto db3_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions("db3");

    // Should have permissions from role3 only
    ASSERT_EQ(db3_label_perms.Has(std::array{"label1"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db3_label_perms.Has(std::array{"label2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db3_label_perms.Has(std::array{"label3"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);

    ASSERT_EQ(db3_edge_perms.Has(std::array{"edge1"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db3_edge_perms.Has(std::array{"edge2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db3_edge_perms.Has(std::array{"edge3"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
  }

  // Test 5: Filter by non-existent database - should have no permissions
  {
    auto none_label_perms = user->GetFineGrainedAccessLabelPermissions("nonexistent");
    auto none_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions("nonexistent");

    // Should have no permissions since no roles grant access to this database
    ASSERT_EQ(none_label_perms.Has(std::array{"label1"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(none_label_perms.Has(std::array{"label2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(none_label_perms.Has(std::array{"label3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);

    ASSERT_EQ(none_edge_perms.Has(std::array{"edge1"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(none_edge_perms.Has(std::array{"edge2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(none_edge_perms.Has(std::array{"edge3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
  }

  // Test 6: Test with user's own fine-grained permissions
  {
    // Grant user's own permissions
    user->fine_grained_access_handler().label_permissions().Grant({"user_label"s}, FineGrainedPermission::READ);
    user->fine_grained_access_handler().edge_type_permissions().Grant({"user_edge"s}, FineGrainedPermission::UPDATE);

    // Test that user's own permissions are always included regardless of database filter
    auto db1_perms = user->GetFineGrainedAccessLabelPermissions("db1");
    auto db1_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions("db1");

    ASSERT_EQ(db1_perms.Has(std::array{"user_label"s}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(db1_perms.Has(std::array{"user_label"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db1_perms.Has(std::array{"user_label"s}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(db1_perms.Has(std::array{"user_label"s}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
    ASSERT_EQ(db1_edge_perms.Has(std::array{"user_edge"s}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(db1_edge_perms.Has(std::array{"user_edge"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db1_edge_perms.Has(std::array{"user_edge"s}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(db1_edge_perms.Has(std::array{"user_edge"s}, FineGrainedPermission::DELETE), PermissionLevel::DENY);

    // Test with non-existent database - user permissions should still be included
    auto none_perms = user->GetFineGrainedAccessLabelPermissions("nonexistent");
    auto none_edge_perms = user->GetFineGrainedAccessEdgeTypePermissions("nonexistent");

    ASSERT_EQ(none_perms.Has(std::array{"user_label"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(none_edge_perms.Has(std::array{"user_edge"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
  }

  // Test 7: Test role-specific methods with database filtering
  {
    auto db1_role_label_perms = user->GetRoleFineGrainedAccessLabelPermissions("db1");
    auto db1_role_edge_perms = user->GetRoleFineGrainedAccessEdgeTypePermissions("db1");

    // Should only include role1 permissions
    ASSERT_EQ(db1_role_label_perms.Has(std::array{"label1"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db1_role_label_perms.Has(std::array{"label2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db1_role_label_perms.Has(std::array{"label3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);

    ASSERT_EQ(db1_role_edge_perms.Has(std::array{"edge1"s}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(db1_role_edge_perms.Has(std::array{"edge2"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(db1_role_edge_perms.Has(std::array{"edge3"s}, FineGrainedPermission::READ), PermissionLevel::DENY);
  }

  // Test 8: Test GetPermissions with database filtering
  {
    // Grant different permissions to each role
    role1->permissions().Grant(Permission::MATCH);
    role1->permissions().Deny(Permission::CREATE);

    role2->permissions().Grant(Permission::DELETE);
    role2->permissions().Grant(Permission::MERGE);

    role3->permissions().Grant(Permission::SET);
    role3->permissions().Grant(Permission::REMOVE);

    // Update storage
    auth->SaveUser(*user);
    auth->SaveRole(*role1);
    auth->SaveRole(*role2);
    auth->SaveRole(*role3);

    user = auth->GetUser("user");

    // Test GetPermissions without database filter - should include all roles
    auto all_permissions = user->GetPermissions();
    EXPECT_EQ(all_permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
    EXPECT_EQ(all_permissions.Has(Permission::CREATE), PermissionLevel::DENY);
    EXPECT_EQ(all_permissions.Has(Permission::DELETE), PermissionLevel::GRANT);
    EXPECT_EQ(all_permissions.Has(Permission::MERGE), PermissionLevel::GRANT);
    EXPECT_EQ(all_permissions.Has(Permission::SET), PermissionLevel::GRANT);
    EXPECT_EQ(all_permissions.Has(Permission::REMOVE), PermissionLevel::GRANT);

    // Test GetPermissions with db1 filter - should only include role1
    auto db1_permissions = user->GetPermissions("db1");
    EXPECT_EQ(db1_permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
    EXPECT_EQ(db1_permissions.Has(Permission::CREATE), PermissionLevel::DENY);
    EXPECT_EQ(db1_permissions.Has(Permission::DELETE), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::SET), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::REMOVE), PermissionLevel::NEUTRAL);

    // Test GetPermissions with db2 filter - should only include role2
    auto db2_permissions = user->GetPermissions("db2");
    ASSERT_EQ(db2_permissions.Has(Permission::MATCH), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db2_permissions.Has(Permission::CREATE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db2_permissions.Has(Permission::DELETE), PermissionLevel::GRANT);
    ASSERT_EQ(db2_permissions.Has(Permission::MERGE), PermissionLevel::GRANT);
    ASSERT_EQ(db2_permissions.Has(Permission::SET), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db2_permissions.Has(Permission::REMOVE), PermissionLevel::NEUTRAL);

    // Test GetPermissions with db3 filter - should only include role3
    auto db3_permissions = user->GetPermissions("db3");
    ASSERT_EQ(db3_permissions.Has(Permission::MATCH), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db3_permissions.Has(Permission::CREATE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db3_permissions.Has(Permission::DELETE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db3_permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(db3_permissions.Has(Permission::SET), PermissionLevel::GRANT);
    ASSERT_EQ(db3_permissions.Has(Permission::REMOVE), PermissionLevel::GRANT);

    // Test GetPermissions with non-existent database - should have no permissions
    auto none_permissions = user->GetPermissions("nonexistent");
    ASSERT_EQ(none_permissions.Has(Permission::MATCH), PermissionLevel::NEUTRAL);
    ASSERT_EQ(none_permissions.Has(Permission::CREATE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(none_permissions.Has(Permission::DELETE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(none_permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
    ASSERT_EQ(none_permissions.Has(Permission::SET), PermissionLevel::NEUTRAL);
    ASSERT_EQ(none_permissions.Has(Permission::REMOVE), PermissionLevel::NEUTRAL);
  }

  // Test 9: Test GetPermissions with user's own permissions
  {
    // Grant user's own permissions
    user->permissions().Grant(Permission::INDEX);
    user->permissions().Grant(Permission::STATS);
    user->permissions().Deny(Permission::MATCH);
    user->permissions().Grant(Permission::CREATE);

    auto db1_permissions = user->GetPermissions("db1");
    EXPECT_FALSE(user->has_access("db1"));
    EXPECT_TRUE(user->HasAccess("db1"));
    EXPECT_EQ(db1_permissions.Has(Permission::INDEX), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::STATS), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
    EXPECT_EQ(db1_permissions.Has(Permission::CREATE), PermissionLevel::DENY);
    EXPECT_EQ(db1_permissions.Has(Permission::DELETE), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::SET), PermissionLevel::NEUTRAL);
    EXPECT_EQ(db1_permissions.Has(Permission::REMOVE), PermissionLevel::NEUTRAL);

    auto db4_permissions = user->GetPermissions("db4");
    EXPECT_TRUE(user->has_access("db4"));
    EXPECT_TRUE(user->HasAccess("db4"));
    EXPECT_EQ(db4_permissions.Has(Permission::INDEX), PermissionLevel::GRANT);
    EXPECT_EQ(db4_permissions.Has(Permission::STATS), PermissionLevel::GRANT);
    EXPECT_EQ(db4_permissions.Has(Permission::MATCH), PermissionLevel::DENY);
    EXPECT_EQ(db4_permissions.Has(Permission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(db4_permissions.Has(Permission::SET), PermissionLevel::GRANT);
    ASSERT_EQ(db4_permissions.Has(Permission::REMOVE), PermissionLevel::GRANT);

    auto none_permissions = user->GetPermissions("nonexistent");
    EXPECT_FALSE(user->has_access("nonexistent"));
    EXPECT_FALSE(user->HasAccess("nonexistent"));
    EXPECT_EQ(none_permissions.Has(Permission::INDEX), PermissionLevel::NEUTRAL);
    EXPECT_EQ(none_permissions.Has(Permission::STATS), PermissionLevel::NEUTRAL);
    EXPECT_EQ(none_permissions.Has(Permission::MATCH), PermissionLevel::NEUTRAL);
    EXPECT_EQ(none_permissions.Has(Permission::CREATE), PermissionLevel::NEUTRAL);
  }

  // Test 10: Test CanImpersonate with database filtering
  {
    // Create a target user to impersonate
    auto target_user = auth->AddUser("target_user");
    ASSERT_TRUE(target_user);

    // Grant impersonation permissions to roles
    role1->permissions().Grant(Permission::IMPERSONATE_USER);
    role2->permissions().Grant(Permission::IMPERSONATE_USER);
    role3->permissions().Grant(Permission::IMPERSONATE_USER);

    // Grant impersonation access to roles
    role1->GrantUserImp({*target_user});
    role2->GrantUserImp({*target_user});
    role3->GrantUserImp({*target_user});

    // Update storage
    auth->SaveRole(*role1);
    auth->SaveRole(*role2);
    auth->SaveRole(*role3);
    auth->SaveUser(*user);
    user = auth->GetUser("user");

    // Test CanImpersonate without database filter - should include all roles
    ASSERT_TRUE(user->CanImpersonate(*target_user));

    // Test CanImpersonate with db1 filter - should only include role1
    ASSERT_TRUE(user->CanImpersonate(*target_user, "db1"));

    // Test CanImpersonate with db2 filter - should only include role2
    ASSERT_TRUE(user->CanImpersonate(*target_user, "db2"));

    // Test CanImpersonate with db3 filter - should only include role3
    ASSERT_TRUE(user->CanImpersonate(*target_user, "db3"));

    // Test CanImpersonate with non-existent database - should fail
    ASSERT_FALSE(user->CanImpersonate(*target_user, "nonexistent"));

    // Test with role that denies impersonation
    role1->DenyUserImp({*target_user});
    auth->SaveRole(*role1);
    user = auth->GetUser("user");

    // Should still work for other databases
    ASSERT_TRUE(user->CanImpersonate(*target_user, "db2"));
    ASSERT_TRUE(user->CanImpersonate(*target_user, "db3"));

    // But should fail for db1 due to deny
    ASSERT_FALSE(user->CanImpersonate(*target_user, "db1"));
  }

  // Test 11: Test CanImpersonate with different permission levels
  {
    auto target_user = auth->GetUser("target_user");
    ASSERT_TRUE(target_user);
    // Create additional target users
    auto neutral_target_user = auth->AddUser("neutral_target_user");
    auto denied_target_user = auth->AddUser("denied_target_user");
    auto ungranted_target_user = auth->AddUser("ungranted_target_user");
    ASSERT_TRUE(neutral_target_user);
    ASSERT_TRUE(denied_target_user);
    ASSERT_TRUE(ungranted_target_user);

    // Set up different permission levels for each role:
    // role1: Grant impersonation permission and access to target_user
    // role2: Neutral impersonation permission (no explicit grant/deny)
    // role3: Deny impersonation permission for target_user
    role1->permissions().Grant(Permission::IMPERSONATE_USER);
    role1->GrantUserImp({*target_user});

    // role2 has no impersonation permissions set (neutral)
    role2->permissions().Revoke(Permission::IMPERSONATE_USER);
    role2->GrantUserImp({*neutral_target_user});

    role3->permissions().Deny(Permission::IMPERSONATE_USER);
    role3->GrantUserImp({*denied_target_user});

    // Update storage
    auth->SaveRole(*role1);
    auth->SaveRole(*role2);
    auth->SaveRole(*role3);
    auth->SaveUser(*user);
    user = auth->GetUser("user");

    // Test CanImpersonate with neutral permission - should fail
    ASSERT_FALSE(user->CanImpersonate(*neutral_target_user, "db2"));

    // Test CanImpersonate with denied permission - should fail
    ASSERT_FALSE(user->CanImpersonate(*denied_target_user, "db3"));

    // Test CanImpersonate with ungranted user - should fail
    ASSERT_FALSE(user->CanImpersonate(*ungranted_target_user, "db1"));
    ASSERT_FALSE(user->CanImpersonate(*ungranted_target_user, "db2"));
    ASSERT_FALSE(user->CanImpersonate(*ungranted_target_user, "db3"));
    ASSERT_FALSE(user->CanImpersonate(*ungranted_target_user));
  }

  // Test 12: Test CanImpersonate with role that changes from grant to neutral
  {
    auto target_user = auth->GetUser("target_user");
    ASSERT_TRUE(target_user);
    // Reset role1 to grant permission
    role1->permissions().Grant(Permission::IMPERSONATE_USER);
    role1->GrantUserImp({*target_user});
    auth->SaveRole(*role1);
    user = auth->GetUser("user");

    // Should work initially
    ASSERT_TRUE(user->CanImpersonate(*target_user, "db1"));

    // Remove impersonation permission (neutral)
    role1->permissions().Revoke(Permission::IMPERSONATE_USER);
    auth->SaveRole(*role1);
    user = auth->GetUser("user");

    // Should now fail due to neutral permission
    ASSERT_FALSE(user->CanImpersonate(*target_user, "db1"));
  }
}

#endif  // MG_ENTERPRISE

TEST_F(AuthWithStorage, RoleManipulations) {
  {
    auto user1 = auth->AddUser("user1");
    ASSERT_TRUE(user1);
    auto role1 = auth->AddRole("role1");
    ASSERT_TRUE(role1);
    user1->ClearAllRoles();
    user1->AddRole(*role1);
    auth->SaveUser(*user1);

    auto user2 = auth->AddUser("user2");
    ASSERT_TRUE(user2);
    auto role2 = auth->AddRole("role2");
    ASSERT_TRUE(role2);
    user2->ClearAllRoles();
    user2->AddRole(*role2);
    auth->SaveUser(*user2);
  }

  {
    auto user1 = auth->GetUser("user1");
    ASSERT_TRUE(user1);
    const auto &roles = user1->roles();
    ASSERT_EQ(roles.size(), 1);
    ASSERT_EQ(roles.rolenames().front(), "role1");

    auto user2 = auth->GetUser("user2");
    ASSERT_TRUE(user2);
    const auto &roles2 = user2->roles();
    ASSERT_EQ(roles2.size(), 1);
    ASSERT_EQ(roles2.rolenames().front(), "role2");
  }

  ASSERT_TRUE(auth->RemoveRole("role1"));

  {
    auto user1 = auth->GetUser("user1");
    ASSERT_TRUE(user1);
    const auto &roles = user1->roles();
    ASSERT_EQ(roles.size(), 0);

    auto user2 = auth->GetUser("user2");
    ASSERT_TRUE(user2);
    const auto &roles2 = user2->roles();
    ASSERT_EQ(roles2.size(), 1);
    ASSERT_EQ(roles2.rolenames().front(), "role2");
  }

  {
    auto role1 = auth->AddRole("role1");
    ASSERT_TRUE(role1);
  }

  {
    auto user1 = auth->GetUser("user1");
    ASSERT_TRUE(user1);
    const auto &roles = user1->roles();
    ASSERT_EQ(roles.size(), 0);

    auto user2 = auth->GetUser("user2");
    ASSERT_TRUE(user2);
    const auto &roles2 = user2->roles();
    ASSERT_EQ(roles2.size(), 1);
    ASSERT_EQ(roles2.rolenames().front(), "role2");
  }

  {
    const auto all = auth->AllUsernames();
    for (const auto &user : all) std::cout << user << std::endl;
    auto users = auth->AllUsers();
    std::sort(users.begin(), users.end(), [](const User &a, const User &b) { return a.username() < b.username(); });
    ASSERT_EQ(users.size(), 2);
    ASSERT_EQ(users[0].username(), "user1");
    ASSERT_EQ(users[1].username(), "user2");
  }

  {
    auto roles = auth->AllRoles();
    std::sort(roles.begin(), roles.end(), [](const Role &a, const Role &b) { return a.rolename() < b.rolename(); });
    ASSERT_EQ(roles.size(), 2);
    ASSERT_EQ(roles[0].rolename(), "role1");
    ASSERT_EQ(roles[1].rolename(), "role2");
  }

  {
    auto users = auth->AllUsersForRole("role2");
    ASSERT_EQ(users.size(), 1);
    ASSERT_EQ(users[0].username(), "user2");
  }
}

TEST_F(AuthWithStorage, UserRoleLinkUnlink) {
  {
    auto user = auth->AddUser("user");
    ASSERT_TRUE(user);
    auto role = auth->AddRole("role");
    ASSERT_TRUE(role);
    user->ClearAllRoles();
    user->AddRole(*role);
    auth->SaveUser(*user);
  }

  {
    auto user = auth->GetUser("user");
    ASSERT_TRUE(user);
    const auto &roles = user->roles();
    ASSERT_EQ(roles.size(), 1);
    ASSERT_EQ(roles.rolenames().front(), "role");
  }

  {
    auto user = auth->GetUser("user");
    ASSERT_TRUE(user);
    user->ClearAllRoles();
    auth->SaveUser(*user);
  }

  {
    auto user = auth->GetUser("user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->roles().size(), 0);
  }
}

TEST_F(AuthWithStorage, UserPasswordCreation) {
  {
    auto user = auth->AddUser("test");
    ASSERT_TRUE(user);
    ASSERT_TRUE(auth->Authenticate("test", "123"));
    ASSERT_TRUE(auth->Authenticate("test", ""));
    ASSERT_TRUE(auth->RemoveUser(user->username()));
  }

  {
    auto user = auth->AddUser("test", "123");
    ASSERT_TRUE(user);
    ASSERT_TRUE(auth->Authenticate("test", "123"));
    ASSERT_FALSE(auth->Authenticate("test", "456"));
    ASSERT_TRUE(auth->RemoveUser(user->username()));
  }

  {
    auto user =
        auth->AddUser("test", "sha256:d74ff0ee8da3b9806b18c877dbf29bbde50b5bd8e4dad7a3a725000feb82e8f1" /* pass */);
    ASSERT_TRUE(user);
    ASSERT_TRUE(auth->Authenticate("test", "pass"));
    ASSERT_FALSE(auth->Authenticate("test", "word"));
    ASSERT_TRUE(auth->RemoveUser(user->username()));
  }

  {
    auto user = auth->AddUser("test", "bcrypt:$2a$12$laGNZfDIHu3t6jGr4Xm9i.siwQ78xfEb2VgXNqNGBV8FEbpHgNiQS" /* pass */);
    ASSERT_TRUE(user);
    ASSERT_TRUE(auth->Authenticate("test", "pass"));
    ASSERT_FALSE(auth->Authenticate("test", "word"));
    ASSERT_TRUE(auth->RemoveUser(user->username()));
  }
}

TEST_F(AuthWithStorage, PasswordStrength) {
  const std::string kWeakRegex = ".+";
  // https://stackoverflow.com/questions/5142103/regex-to-validate-password-strength
  const std::string kStrongRegex =
      "^(?=.*[A-Z].*[A-Z])(?=.*[!@#$&*])(?=.*[0-9].*[0-9])(?=.*[a-z].*[a-z].*["
      "a-z]).{8,}$";

  const std::string kWeakPassword = "weak";
  const std::string kAlmostStrongPassword = "ThisPasswordMeetsAllButOneCriterion1234";
  const std::string kStrongPassword = "ThisIsAVeryStrongPassword123$";

  {
    auth.reset();
    auth.emplace(test_folder / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid()))),
                 Auth::Config{std::string{memgraph::glue::kDefaultUserRoleRegex}, kWeakRegex, true});
    auto user = auth->AddUser("user1");
    ASSERT_TRUE(user);
    ASSERT_NO_THROW(auth->UpdatePassword(*user, std::nullopt));
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kWeakPassword));
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kAlmostStrongPassword));
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kStrongPassword));
  }

  {
    auth.reset();
    auth.emplace(test_folder / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid()))),
                 Auth::Config{std::string{memgraph::glue::kDefaultUserRoleRegex}, kWeakRegex, false});
    ASSERT_THROW(auth->AddUser("user2", std::nullopt), AuthException);
    auto user = auth->AddUser("user2", kWeakPassword);
    ASSERT_TRUE(user);
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kWeakPassword));
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kAlmostStrongPassword));
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kStrongPassword));
  }

  {
    auth.reset();
    auth.emplace(test_folder / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid()))),
                 Auth::Config{std::string{memgraph::glue::kDefaultUserRoleRegex}, kStrongRegex, true});
    auto user = auth->AddUser("user3");
    ASSERT_TRUE(user);
    ASSERT_NO_THROW(auth->UpdatePassword(*user, std::nullopt));
    ASSERT_THROW(auth->UpdatePassword(*user, kWeakPassword), AuthException);
    ASSERT_THROW(auth->UpdatePassword(*user, kAlmostStrongPassword), AuthException);
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kStrongPassword));
  }

  {
    auth.reset();
    auth.emplace(test_folder / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid()))),
                 Auth::Config{std::string{memgraph::glue::kDefaultUserRoleRegex}, kStrongRegex, false});
    ASSERT_THROW(auth->AddUser("user4", std::nullopt);, AuthException);
    ASSERT_THROW(auth->AddUser("user4", kWeakPassword);, AuthException);
    ASSERT_THROW(auth->AddUser("user4", kAlmostStrongPassword);, AuthException);
    auto user = auth->AddUser("user4", kStrongPassword);
    ASSERT_TRUE(user);
    ASSERT_NO_THROW(auth->UpdatePassword(*user, kStrongPassword));
  }
}

TEST(AuthWithoutStorage, Permissions) {
  Permissions permissions;
  ASSERT_EQ(permissions.grants(), 0);
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Grant(Permission::MATCH);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
  ASSERT_EQ(permissions.grants(), memgraph::utils::UnderlyingCast(Permission::MATCH));
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Revoke(Permission::MATCH);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::NEUTRAL);
  ASSERT_EQ(permissions.grants(), 0);
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Deny(Permission::MATCH);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::DENY);
  ASSERT_EQ(permissions.denies(), memgraph::utils::UnderlyingCast(Permission::MATCH));
  ASSERT_EQ(permissions.grants(), 0);

  permissions.Grant(Permission::MATCH);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
  ASSERT_EQ(permissions.grants(), memgraph::utils::UnderlyingCast(Permission::MATCH));
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Deny(Permission::CREATE);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
  ASSERT_EQ(permissions.Has(Permission::CREATE), PermissionLevel::DENY);
  ASSERT_EQ(permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(permissions.grants(), memgraph::utils::UnderlyingCast(Permission::MATCH));
  ASSERT_EQ(permissions.denies(), memgraph::utils::UnderlyingCast(Permission::CREATE));

  permissions.Grant(Permission::DELETE);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
  ASSERT_EQ(permissions.Has(Permission::CREATE), PermissionLevel::DENY);
  ASSERT_EQ(permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(permissions.Has(Permission::DELETE), PermissionLevel::GRANT);
  ASSERT_EQ(permissions.grants(),
            memgraph::utils::UnderlyingCast(Permission::MATCH) | memgraph::utils::UnderlyingCast(Permission::DELETE));
  ASSERT_EQ(permissions.denies(), memgraph::utils::UnderlyingCast(Permission::CREATE));

  permissions.Revoke(Permission::DELETE);
  ASSERT_EQ(permissions.Has(Permission::MATCH), PermissionLevel::GRANT);
  ASSERT_EQ(permissions.Has(Permission::CREATE), PermissionLevel::DENY);
  ASSERT_EQ(permissions.Has(Permission::MERGE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(permissions.Has(Permission::DELETE), PermissionLevel::NEUTRAL);
  ASSERT_EQ(permissions.grants(), memgraph::utils::UnderlyingCast(Permission::MATCH));
  ASSERT_EQ(permissions.denies(), memgraph::utils::UnderlyingCast(Permission::CREATE));
}

TEST(AuthWithoutStorage, PermissionsMaskTest) {
  Permissions p1(0, 0);
  ASSERT_EQ(p1.grants(), 0);
  ASSERT_EQ(p1.denies(), 0);

  Permissions p2(1, 0);
  ASSERT_EQ(p2.grants(), 1);
  ASSERT_EQ(p2.denies(), 0);

  Permissions p3(1, 1);
  ASSERT_EQ(p3.grants(), 0);
  ASSERT_EQ(p3.denies(), 1);

  Permissions p4(3, 2);
  ASSERT_EQ(p4.grants(), 1);
  ASSERT_EQ(p4.denies(), 2);
}

#ifdef MG_ENTERPRISE
TEST(AuthWithoutStorage, FineGrainedAccessPermissions) {
  const std::string any_label = "AnyString";
  const std::string check_label = "Label";
  const std::string non_check_label = "OtherLabel";
  const std::string asterisk = "*";

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    ASSERT_TRUE(fga_permissions1 == fga_permissions2);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    ASSERT_TRUE(fga_permissions.GetPermissions().empty());
    ASSERT_EQ(fga_permissions.GetGlobalPermission(), std::nullopt);

    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({any_label}, FineGrainedPermission::DELETE);

    ASSERT_EQ(fga_permissions.GetGlobalPermission(), std::nullopt);
    ASSERT_FALSE(fga_permissions.GetPermissions().empty());
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.GrantGlobal(FineGrainedPermission::CREATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions.GrantGlobal(FineGrainedPermission::UPDATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::DELETE);

    ASSERT_EQ(fga_permissions.GetGlobalPermission(), static_cast<uint64_t>(kLabelPermissionAll));
    ASSERT_TRUE(fga_permissions.GetPermissions().empty());
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.GrantGlobal(FineGrainedPermission::CREATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions.GrantGlobal(FineGrainedPermission::UPDATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::DELETE);
    // Test that revoking a label-specific permission doesn't affect global permissions
    fga_permissions.Revoke({any_label}, FineGrainedPermission::CREATE, MatchingMode::ANY);

    ASSERT_EQ(fga_permissions.GetGlobalPermission(), static_cast<uint64_t>(kLabelPermissionAll));
    ASSERT_TRUE(fga_permissions.GetPermissions().empty());
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({any_label}, FineGrainedPermission::DELETE);
    fga_permissions.Revoke({any_label}, FineGrainedPermission::DELETE, MatchingMode::ANY);

    ASSERT_EQ(fga_permissions.GetGlobalPermission(), std::nullopt);
    ASSERT_TRUE(fga_permissions.GetPermissions().empty());
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({any_label}, FineGrainedPermission::CREATE);
    fga_permissions.RevokeAll();

    ASSERT_EQ(fga_permissions.GetGlobalPermission(), std::nullopt);
    ASSERT_TRUE(fga_permissions.GetPermissions().empty());
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.GrantGlobal(FineGrainedPermission::CREATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::UPDATE);

    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.GrantGlobal(FineGrainedPermission::UPDATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::CREATE);

    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions.Grant({check_label}, FineGrainedPermission::READ);
    fga_permissions.Grant({check_label}, FineGrainedPermission::UPDATE);

    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{non_check_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{non_check_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{non_check_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{non_check_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({check_label}, FineGrainedPermission::READ);
    fga_permissions.Grant({check_label}, FineGrainedPermission::UPDATE);
    fga_permissions.Grant({check_label}, FineGrainedPermission::DELETE);

    fga_permissions.Revoke({check_label}, FineGrainedPermission::UPDATE);

    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::DELETE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions.GrantGlobal(FineGrainedPermission::UPDATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::CREATE);
    fga_permissions.GrantGlobal(FineGrainedPermission::DELETE);

    fga_permissions.RevokeGlobal(FineGrainedPermission::DELETE);

    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({check_label}, FineGrainedPermission::READ);

    fga_permissions.Revoke({check_label}, FineGrainedPermission::READ);

    ASSERT_TRUE(fga_permissions.GetPermissions().empty());
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({check_label}, FineGrainedPermission::READ);

    fga_permissions.Revoke({check_label}, FineGrainedPermission::UPDATE);

    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions;
    fga_permissions.Grant({check_label}, FineGrainedPermission::READ);
    fga_permissions.Grant({check_label}, FineGrainedPermission::UPDATE);
    fga_permissions.Grant({check_label}, FineGrainedPermission::DELETE);

    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::DELETE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions.Has(std::array{check_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
  }
}

TEST_F(AuthWithStorage, FineGrainedAccessCheckerMerge) {
  const std::string any_label = "AnyString";
  const std::string check_label = "Label";
  const std::string asterisk = "*";

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions1.GrantGlobal(FineGrainedPermission::READ);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions2.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions2.GrantGlobal(FineGrainedPermission::CREATE);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions1.GrantGlobal(FineGrainedPermission::CREATE);
    fga_permissions2.GrantGlobal(FineGrainedPermission::UPDATE);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions1.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions1.Grant({check_label}, FineGrainedPermission::UPDATE);
    fga_permissions2.GrantGlobal(FineGrainedPermission::DELETE);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions1.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions1.Grant({check_label}, FineGrainedPermission::CREATE);
    fga_permissions2.GrantGlobal(FineGrainedPermission::UPDATE);
    fga_permissions2.Grant({check_label}, FineGrainedPermission::READ);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::CREATE), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::GRANT);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions1.Grant({check_label}, FineGrainedPermission::READ);
    fga_permissions2.Grant({check_label}, FineGrainedPermission::NOTHING);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{check_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }

  {
    FineGrainedAccessPermissions fga_permissions1, fga_permissions2;
    fga_permissions1.GrantGlobal(FineGrainedPermission::READ);
    fga_permissions2.GrantGlobal(FineGrainedPermission::NOTHING);

    auto fga_permissions3 = memgraph::auth::Merge(fga_permissions1, fga_permissions2);

    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::CREATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::READ), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::UPDATE), PermissionLevel::DENY);
    ASSERT_EQ(fga_permissions3.Has(std::array{any_label}, FineGrainedPermission::DELETE), PermissionLevel::DENY);
  }
}
#endif  // MG_ENTERPRISE

TEST(AuthWithoutStorage, UserSerializeDeserialize) {
  auto user = User("test");
  user.permissions().Grant(Permission::MATCH);
  user.permissions().Deny(Permission::MERGE);
  user.UpdatePassword("world");

  auto data = user.Serialize();

  auto output = User::Deserialize(data);
  ASSERT_EQ(user, output);
}

TEST(AuthWithoutStorage, UserSerializeDeserializeWithOutPassword) {
  auto user = User("test");
  user.permissions().Grant(Permission::MATCH);
  user.permissions().Deny(Permission::MERGE);

  auto data = user.Serialize();

  auto output = User::Deserialize(data);
  ASSERT_EQ(user, output);
}

TEST(AuthWithoutStorage, RoleSerializeDeserialize) {
  auto role = Role("test");
  role.permissions().Grant(Permission::MATCH);
  role.permissions().Deny(Permission::MERGE);

  auto data = role.Serialize();

  auto output = Role::Deserialize(data);
  ASSERT_EQ(role, output);
}

TEST_F(AuthWithStorage, UserWithRoleSerializeDeserialize) {
  auto role = auth->AddRole("role");
  ASSERT_TRUE(role);
  role->permissions().Grant(Permission::MATCH);
  role->permissions().Deny(Permission::MERGE);
  auth->SaveRole(*role);

  auto user = auth->AddUser("user");
  ASSERT_TRUE(user);
  user->permissions().Grant(Permission::MATCH);
  user->permissions().Deny(Permission::MERGE);
  user->UpdatePassword("world");
  user->ClearAllRoles();
  user->AddRole(*role);
  auth->SaveUser(*user);

  auto new_user = auth->GetUser("user");
  ASSERT_TRUE(new_user);
  ASSERT_EQ(*user, *new_user);
}

TEST_F(AuthWithStorage, UserRoleUniqueName) {
  ASSERT_TRUE(auth->AddUser("user"));
  ASSERT_TRUE(auth->AddRole("role"));
  ASSERT_FALSE(auth->AddRole("user"));
  ASSERT_FALSE(auth->AddUser("role"));
}

TEST_F(AuthWithStorage, MultipleRoles) {
  // Create a user and multiple roles
  auto user = auth->AddUser("user");
  ASSERT_TRUE(user);
  auto role1 = auth->AddRole("role1");
  auto role2 = auth->AddRole("role2");
  auto role3 = auth->AddRole("role3");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  ASSERT_TRUE(role3);

  // Add roles to user
  user->AddRole(*role1);
  user->AddRole(*role2);
  user->AddRole(*role3);
  auth->SaveUser(*user);

  // Verify user has all roles
  auto retrieved_user = auth->GetUser("user");
  ASSERT_TRUE(retrieved_user);
  ASSERT_EQ(retrieved_user->roles().size(), 3);

  const auto &roles = retrieved_user->roles();
  ASSERT_EQ(roles.size(), 3);
  ASSERT_TRUE(std::find(roles.rolenames().begin(), roles.rolenames().end(), "role1") != roles.rolenames().end());
  ASSERT_TRUE(std::find(roles.rolenames().begin(), roles.rolenames().end(), "role2") != roles.rolenames().end());
  ASSERT_TRUE(std::find(roles.rolenames().begin(), roles.rolenames().end(), "role3") != roles.rolenames().end());

  // Test removing a specific role
  retrieved_user->RemoveRole("role2");
  auth->SaveUser(*retrieved_user);

  auto updated_user = auth->GetUser("user");
  ASSERT_TRUE(updated_user);
  ASSERT_EQ(updated_user->roles().size(), 2);

  // Verify role2 was removed
  bool has_role2 = std::find_if(updated_user->roles().begin(), updated_user->roles().end(), [](const auto &role) {
                     return role.rolename() == "role2";
                   }) != updated_user->roles().end();
  ASSERT_FALSE(has_role2);

  // Test adding a duplicate role (should be ignored)
  updated_user->AddRole(*role1);
  ASSERT_EQ(updated_user->roles().size(), 2);  // Should still be 2

  // Test clearing all roles
  updated_user->ClearAllRoles();
  auth->SaveUser(*updated_user);

  auto cleared_user = auth->GetUser("user");
  ASSERT_TRUE(cleared_user);
  ASSERT_EQ(cleared_user->roles().size(), 0);
}

TEST(AuthWithoutStorage, CaseInsensitivity) {
  {
    auto user1 = User("test");
    auto user2 = User("Test");
    ASSERT_EQ(user1, user2);
    ASSERT_EQ(user1.username(), user2.username());
    ASSERT_EQ(user1.username(), "test");
    ASSERT_EQ(user2.username(), "test");
  }
  {
    auto perms = Permissions();
    auto fine_grained_access_handler = FineGrainedAccessHandler();
    auto passwordHash = HashPassword("pw");
    auto user1 = User("test", passwordHash, perms, fine_grained_access_handler);
    auto user2 = User("Test", passwordHash, perms, fine_grained_access_handler);
    ASSERT_EQ(user1, user2);
    ASSERT_EQ(user1.username(), user2.username());
    ASSERT_EQ(user1.username(), "test");
    ASSERT_EQ(user2.username(), "test");
  }
  {
    auto role1 = Role("role");
    auto role2 = Role("Role");
    ASSERT_EQ(role1, role2);
    ASSERT_EQ(role1.rolename(), role2.rolename());
    ASSERT_EQ(role1.rolename(), "role");
    ASSERT_EQ(role2.rolename(), "role");
  }
  {
    auto perms = Permissions();
    auto fine_grained_access_handler = FineGrainedAccessHandler();
    auto role1 = Role("role", perms, fine_grained_access_handler);
    auto role2 = Role("Role", perms, fine_grained_access_handler);
    ASSERT_EQ(role1, role2);
    ASSERT_EQ(role1.rolename(), role2.rolename());
    ASSERT_EQ(role1.rolename(), "role");
    ASSERT_EQ(role2.rolename(), "role");
  }
}

TEST_F(AuthWithStorage, CaseInsensitivity) {
  // AddUser
  {
    auto user = auth->AddUser("Alice", "alice");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "alice");
    ASSERT_FALSE(auth->AddUser("alice"));
    ASSERT_FALSE(auth->AddUser("alicE"));
  }
  {
    auto user = auth->AddUser("BoB", "bob");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "bob");
    ASSERT_FALSE(auth->AddUser("bob"));
    ASSERT_FALSE(auth->AddUser("bOb"));
  }

  // Authenticate
  {
    auto user_or_role = auth->Authenticate("alice", "alice");
    ASSERT_TRUE(user_or_role);
    const auto &user = std::get<memgraph::auth::User>(*user_or_role);
    ASSERT_EQ(user.username(), "alice");
  }
  {
    auto user_or_role = auth->Authenticate("alICe", "alice");
    ASSERT_TRUE(user_or_role);
    const auto &user = std::get<memgraph::auth::User>(*user_or_role);
    ASSERT_EQ(user.username(), "alice");
  }

  // GetUser
  {
    auto user = auth->GetUser("alice");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "alice");
  }
  {
    auto user = auth->GetUser("aLicE");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "alice");
  }
  ASSERT_FALSE(auth->GetUser("carol"));

  // RemoveUser
  {
    auto user = auth->AddUser("caRol", "carol");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "carol");
    ASSERT_TRUE(auth->RemoveUser("cAROl"));
    ASSERT_FALSE(auth->RemoveUser("carol"));
    ASSERT_FALSE(auth->GetUser("CAROL"));
  }

  // AllUsers
  {
    const auto all = auth->AllUsernames();
    for (const auto &user : all) std::cout << user << std::endl;
    auto users = auth->AllUsers();
    ASSERT_EQ(users.size(), 2);
    std::sort(users.begin(), users.end(), [](const auto &a, const auto &b) { return a.username() < b.username(); });
    ASSERT_EQ(users[0].username(), "alice");
    ASSERT_EQ(users[1].username(), "bob");
  }

  // AddRole
  {
    auto role = auth->AddRole("Moderator");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "moderator");
    ASSERT_FALSE(auth->AddRole("moderator"));
    ASSERT_FALSE(auth->AddRole("MODERATOR"));
  }
  {
    auto role = auth->AddRole("adMIN");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "admin");
    ASSERT_FALSE(auth->AddRole("Admin"));
    ASSERT_FALSE(auth->AddRole("ADMIn"));
  }
  ASSERT_FALSE(auth->AddRole("ALICE"));
  ASSERT_FALSE(auth->AddUser("ModeRAtor"));

  // GetRole
  {
    auto role = auth->GetRole("moderator");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "moderator");
  }
  {
    auto role = auth->GetRole("MoDERATOR");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "moderator");
  }
  ASSERT_FALSE(auth->GetRole("root"));

  // RemoveRole
  {
    auto role = auth->AddRole("RooT");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "root");
    ASSERT_TRUE(auth->RemoveRole("rOOt"));
    ASSERT_FALSE(auth->RemoveRole("RoOt"));
    ASSERT_FALSE(auth->GetRole("RoOt"));
  }

  // AllRoles
  {
    auto roles = auth->AllRoles();
    ASSERT_EQ(roles.size(), 2);
    std::sort(roles.begin(), roles.end(), [](const auto &a, const auto &b) { return a.rolename() < b.rolename(); });
    ASSERT_EQ(roles[0].rolename(), "admin");
    ASSERT_EQ(roles[1].rolename(), "moderator");
  }

  // SaveRole
  {
    auto role = auth->GetRole("MODErator");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "moderator");
    role->permissions().Grant(memgraph::auth::Permission::MATCH);
    auth->SaveRole(*role);
  }
  {
    auto role = auth->GetRole("modeRATOR");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "moderator");
    ASSERT_EQ(role->permissions().Has(memgraph::auth::Permission::MATCH), memgraph::auth::PermissionLevel::GRANT);
  }

  // SaveUser
  {
    auto user = auth->GetUser("aLice");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "alice");
    auto role = auth->GetRole("moderAtor");
    ASSERT_TRUE(role);
    ASSERT_EQ(role->rolename(), "moderator");
    user->ClearAllRoles();
    user->AddRole(*role);
    auth->SaveUser(*user);
  }
  {
    auto user = auth->GetUser("aLIce");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->username(), "alice");
    const auto &roles = user->roles();
    ASSERT_EQ(roles.size(), 1);
    ASSERT_EQ(roles.rolenames().front(), "moderator");
  }

  // AllUsersForRole
  {
    auto carol = auth->AddUser("caROl");
    ASSERT_TRUE(carol);
    ASSERT_EQ(carol->username(), "carol");
    auto dave = auth->AddUser("daVe");
    ASSERT_TRUE(dave);
    ASSERT_EQ(dave->username(), "dave");
    auto admin = auth->GetRole("aDMin");
    ASSERT_TRUE(admin);
    ASSERT_EQ(admin->rolename(), "admin");
    carol->ClearAllRoles();
    carol->AddRole(*admin);
    auth->SaveUser(*carol);
    dave->ClearAllRoles();
    dave->AddRole(*admin);
    auth->SaveUser(*dave);
  }
  {
    auto users = auth->AllUsersForRole("modeRAtoR");
    ASSERT_EQ(users.size(), 1);
    ASSERT_EQ(users[0].username(), "alice");
  }
  {
    auto users = auth->AllUsersForRole("AdmiN");
    ASSERT_EQ(users.size(), 2);
    std::sort(users.begin(), users.end(), [](const auto &a, const auto &b) { return a.username() < b.username(); });
    ASSERT_EQ(users[0].username(), "carol");
    ASSERT_EQ(users[1].username(), "dave");
  }
}

TEST(AuthWithoutStorage, Crypto) {
  auto hash = HashPassword("hello");
  ASSERT_TRUE(hash.VerifyPassword("hello"));
  ASSERT_FALSE(hash.VerifyPassword("hello1"));
}

class AuthWithVariousEncryptionAlgorithms : public ::testing::Test {
 protected:
  void SetUp() override { SetHashAlgorithm("bcrypt"); }
};

TEST_F(AuthWithVariousEncryptionAlgorithms, VerifyPasswordDefault) {
  auto hash = HashPassword("hello");
  ASSERT_TRUE(hash.VerifyPassword("hello"));
  ASSERT_FALSE(hash.VerifyPassword("hello1"));
}

TEST_F(AuthWithVariousEncryptionAlgorithms, VerifyPasswordSHA256) {
  SetHashAlgorithm("sha256");
  auto hash = HashPassword("hello");
  ASSERT_TRUE(hash.VerifyPassword("hello"));
  ASSERT_FALSE(hash.VerifyPassword("hello1"));
}

TEST_F(AuthWithVariousEncryptionAlgorithms, VerifyPasswordSHA256_1024) {
  SetHashAlgorithm("sha256-multiple");
  auto hash = HashPassword("hello");
  ASSERT_TRUE(hash.VerifyPassword("hello"));
  ASSERT_FALSE(hash.VerifyPassword("hello1"));
}

TEST_F(AuthWithVariousEncryptionAlgorithms, SetEncryptionAlgorithmNonsenseThrow) {
  ASSERT_THROW(SetHashAlgorithm("abcd"), AuthException);
}

TEST_F(AuthWithVariousEncryptionAlgorithms, SetEncryptionAlgorithmEmptyThrow) {
  ASSERT_THROW(SetHashAlgorithm(""), AuthException);
}

class AuthWithStorageWithVariousEncryptionAlgorithms : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder);
    SetHashAlgorithm("bcrypt");

    memgraph::license::global_license_checker.EnableTesting();
  }

  void TearDown() override { fs::remove_all(test_folder); }

  fs::path test_folder{fs::temp_directory_path() / "MG_tests_unit_auth"};
  Auth::Config auth_config{};
  Auth auth{test_folder / ("unit_auth_test_" + std::to_string(static_cast<int>(getpid()))), auth_config};
};

TEST_F(AuthWithStorageWithVariousEncryptionAlgorithms, AddUserDefault) {
  auto user = auth.AddUser("Alice", "alice");
  ASSERT_TRUE(user);
  ASSERT_EQ(user->username(), "alice");
}

TEST_F(AuthWithStorageWithVariousEncryptionAlgorithms, AddUserSha256) {
  SetHashAlgorithm("sha256");
  auto user = auth.AddUser("Alice", "alice");
  ASSERT_TRUE(user);
  ASSERT_EQ(user->username(), "alice");
}

TEST_F(AuthWithStorageWithVariousEncryptionAlgorithms, AddUserSha256_1024) {
  SetHashAlgorithm("sha256-multiple");
  auto user = auth.AddUser("Alice", "alice");
  ASSERT_TRUE(user);
  ASSERT_EQ(user->username(), "alice");
}

TEST(Serialize, HashedPassword) {
  for (auto algo :
       {PasswordHashAlgorithm::BCRYPT, PasswordHashAlgorithm::SHA256, PasswordHashAlgorithm::SHA256_MULTIPLE}) {
    auto sut = HashPassword("password", algo);
    nlohmann::json j = sut;
    auto ret = j.get<HashedPassword>();
    ASSERT_EQ(sut, ret);
    ASSERT_TRUE(ret.VerifyPassword("password"));
  }
}

#ifdef MG_ENTERPRISE

TEST_F(AuthWithStorage, UserImpersonationWUser) {
  ASSERT_TRUE(auth->AddUser("admin"));
  ASSERT_TRUE(auth->AddUser("user"));
  ASSERT_TRUE(auth->AddUser("another_user"));

  // User has no permissions by deafult; add some
  auto user = auth->GetUser("user");
  auto another_user = auth->GetUser("another_user");
  ASSERT_TRUE(user);
  ASSERT_TRUE(another_user);

  // it's not enough to have permission, you need to specify who you are allowed to impersonate
  auto admin = auth->GetUser("admin");
  ASSERT_TRUE(admin);
  admin->permissions().Grant(Permission::IMPERSONATE_USER);
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_FALSE(admin->CanImpersonate(*another_user));

  // allow impersonation of only "another_user"
  admin->GrantUserImp({*another_user});
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // allow admin to impersonate anyone
  admin->GrantUserImp();
  ASSERT_TRUE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // deny impersonation of only "user"
  admin->DenyUserImp({*user});
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // Reload user
  auth->SaveUser(*admin);
  admin = auth->GetUser("admin");
  ASSERT_TRUE(admin);
  // deny impersonation of only "user"
  admin->DenyUserImp({*user});
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));
}

TEST_F(AuthWithStorage, UserImpersonationWRole) {
  ASSERT_TRUE(auth->AddRole("admin"));
  ASSERT_TRUE(auth->AddUser("user"));
  ASSERT_TRUE(auth->AddUser("another_user"));

  // User has no permissions by deafult; add some
  auto user = auth->GetUser("user");
  auto another_user = auth->GetUser("another_user");
  ASSERT_TRUE(user);
  ASSERT_TRUE(another_user);
  user->permissions().Grant(Permission::MATCH);

  // even the admin can impersonate by default
  auto admin = auth->GetRole("admin");
  ASSERT_TRUE(admin);
  admin->permissions().Grant(Permission::IMPERSONATE_USER);
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_FALSE(admin->CanImpersonate(*another_user));

  // allow impersonation of only "another_user"
  admin->GrantUserImp({*another_user});
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // allow admin to impersonate anyone
  admin->GrantUserImp();
  ASSERT_TRUE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // deny impersonation of only "user"
  admin->DenyUserImp({*user});
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // user should have the same provoledges as its role
  user->ClearAllRoles();
  user->AddRole(*admin);
  ASSERT_FALSE(user->CanImpersonate(*user));
  ASSERT_TRUE(user->CanImpersonate(*another_user));

  // Reload role
  auth->SaveRole(*admin);
  admin = auth->GetRole("admin");
  ASSERT_TRUE(admin);
  ASSERT_FALSE(user->CanImpersonate(*user));
  ASSERT_TRUE(user->CanImpersonate(*another_user));
}

TEST_F(AuthWithStorage, UserImpersonationWUserAndRole) {
  ASSERT_TRUE(auth->AddUser("admin"));
  ASSERT_TRUE(auth->AddRole("admin_role"));
  ASSERT_TRUE(auth->AddUser("user"));
  ASSERT_TRUE(auth->AddUser("another_user"));

  // User has no permissions by default; add some
  auto user = auth->GetUser("user");
  auto another_user = auth->GetUser("another_user");
  ASSERT_TRUE(user);
  ASSERT_TRUE(another_user);
  user->permissions().Grant(Permission::MATCH);

  // even the admin can impersonate by default
  auto admin_role = auth->GetRole("admin_role");
  ASSERT_TRUE(admin_role);
  auto admin = auth->GetUser("admin");
  ASSERT_TRUE(admin);
  admin->permissions().Grant(Permission::IMPERSONATE_USER);
  admin->ClearAllRoles();
  admin->AddRole(*admin_role);
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_FALSE(admin->CanImpersonate(*another_user));

  // allow impersonation of only "another_user"
  admin->GrantUserImp({*another_user});
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // allow role to impersonate "user"
  admin_role->GrantUserImp({*user});
  admin->ClearAllRoles();
  admin->AddRole(*admin_role);  // update role
  ASSERT_TRUE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // allow admin to impersonate anyone
  admin->GrantUserImp();
  ASSERT_TRUE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // deny role to impersonate "user"
  admin_role->DenyUserImp({*user});
  admin->ClearAllRoles();
  admin->AddRole(*admin_role);  // update role
  ASSERT_FALSE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // allow role to impersonate anyone
  admin_role->GrantUserImp();
  admin->ClearAllRoles();
  admin->AddRole(*admin_role);  // update role
  ASSERT_TRUE(admin->CanImpersonate(*user));
  ASSERT_TRUE(admin->CanImpersonate(*another_user));

  // deny admin to impersonate "another_user"
  admin->DenyUserImp({*another_user});
  ASSERT_TRUE(admin->CanImpersonate(*user));
  ASSERT_FALSE(admin->CanImpersonate(*another_user));
}

TEST_F(V1Auth, MigrationTest) {
  // Check if migration was successful
  ASSERT_TRUE(auth->HasUsers());
  ASSERT_FALSE(auth->AllRoles().empty());

  // Check for specific users
  auto user1 = auth->GetUser("user1");
  auto user2 = auth->GetUser("user2");
  ASSERT_TRUE(user1);
  ASSERT_TRUE(user2);

  // Check for specific roles
  auto role1 = auth->GetRole("role1");
  auto role2 = auth->GetRole("role2");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);

  // Check that each user is connected to a role
  ASSERT_EQ(user1->roles().size(), 1);
  ASSERT_EQ(user2->roles().size(), 1);

  // Verify the role assignments
  ASSERT_EQ(user1->roles().rolenames().front(), "role1");
  ASSERT_EQ(user2->roles().rolenames().front(), "role2");
}

#endif

#ifdef MG_ENTERPRISE
TEST_F(AuthWithStorage, MultiTenantRoleManagement) {
  // Create roles with database access
  ASSERT_TRUE(auth->AddRole("role1"));
  ASSERT_TRUE(auth->AddRole("role2"));

  auto role1 = auth->GetRole("role1");
  auto role2 = auth->GetRole("role2");
  ASSERT_NE(role1, std::nullopt);
  ASSERT_NE(role2, std::nullopt);

  role1->db_access().Grant("db1");
  role1->db_access().Grant("db2");
  role2->db_access().Grant("db2");
  role2->db_access().Grant("db3");

  auth->SaveRole(*role1);
  auth->SaveRole(*role2);

  // Create a user
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  // Set multi-tenant roles
  user->AddMultiTenantRole(*role1, "db1");
  user->AddMultiTenantRole(*role1, "db2");
  user->AddMultiTenantRole(*role2, "db2");
  auth->SaveUser(*user);

  // Verify roles are set correctly
  {
    auto roles_db1 = user->GetMultiTenantRoles("db1");
    ASSERT_EQ(roles_db1.size(), 1);
    ASSERT_EQ(roles_db1.begin()->rolename(), "role1");

    auto roles_db2 = user->GetMultiTenantRoles("db2");
    ASSERT_EQ(roles_db2.size(), 2);  // Both role1 and role2 have access to db2
    std::set<std::string> role_names;
    for (const auto &role : roles_db2) {
      role_names.insert(role.rolename());
    }
    ASSERT_EQ(role_names.size(), 2);
    ASSERT_TRUE(role_names.count("role1"));
    ASSERT_TRUE(role_names.count("role2"));

    auto roles_db3 = user->GetMultiTenantRoles("db3");
    ASSERT_EQ(roles_db3.size(), 0);

    // Trying to add a role that doesn't have access to the database should throw
    ASSERT_THROW(user->AddMultiTenantRole(*role1, "db3"), AuthException);
  }

  // Verify durability
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);

    auto roles_db1 = updated_user->GetMultiTenantRoles("db1");
    ASSERT_EQ(roles_db1.size(), 1);
    ASSERT_EQ(roles_db1.begin()->rolename(), "role1");

    auto roles_db2 = updated_user->GetMultiTenantRoles("db2");
    ASSERT_EQ(roles_db2.size(), 2);  // Both role1 and role2 have access to db2
    std::set<std::string> role_names;
    for (const auto &role : roles_db2) {
      role_names.insert(role.rolename());
    }
    ASSERT_EQ(role_names.size(), 2);
    ASSERT_TRUE(role_names.count("role1"));
    ASSERT_TRUE(role_names.count("role2"));

    auto roles_db3 = updated_user->GetMultiTenantRoles("db3");
    ASSERT_EQ(roles_db3.size(), 0);

    // Trying to add a role that doesn't have access to the database should throw
    ASSERT_THROW(updated_user->AddMultiTenantRole(*role1, "db3"), AuthException);
  }
}

TEST_F(AuthWithStorage, MultiTenantRoleClearRole) {
  // Create a role with database access
  ASSERT_TRUE(auth->AddRole("test_role"));
  auto role = auth->GetRole("test_role");
  ASSERT_NE(role, std::nullopt);

  role->db_access().Grant("db1");
  role->db_access().Grant("db2");
  auth->SaveRole(*role);

  // Create a user and set multi-tenant roles
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  user->AddMultiTenantRole(*role, "db1");
  user->AddMultiTenantRole(*role, "db2");
  auth->SaveUser(*user);

  // Verify roles are set
  auto updated_user = auth->GetUser("test_user");
  ASSERT_NE(updated_user, std::nullopt);
  ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
  ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 1);

  // Clear all roles (should clear multi-tenant roles too)
  updated_user->ClearAllRoles();
  auth->SaveUser(*updated_user);

  // Verify all roles are cleared
  auto final_user = auth->GetUser("test_user");
  ASSERT_NE(final_user, std::nullopt);
  ASSERT_EQ(final_user->GetMultiTenantRoles("db1").size(), 0);
  ASSERT_EQ(final_user->GetMultiTenantRoles("db2").size(), 0);
  ASSERT_TRUE(final_user->roles().empty());
}

TEST_F(AuthWithStorage, MultiTenantRoleRemoveUser) {
  // Create a role with database access
  ASSERT_TRUE(auth->AddRole("test_role"));
  auto role = auth->GetRole("test_role");
  ASSERT_NE(role, std::nullopt);

  role->db_access().Grant("db1");
  auth->SaveRole(*role);

  // Create a user and set multi-tenant role
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  user->AddMultiTenantRole(*role, "db1");
  auth->SaveUser(*user);

  // Verify user exists with multi-tenant role
  auto existing_user = auth->GetUser("test_user");
  ASSERT_NE(existing_user, std::nullopt);
  ASSERT_EQ(existing_user->GetMultiTenantRoles("db1").size(), 1);

  // Remove the user
  ASSERT_TRUE(auth->RemoveUser("test_user", nullptr));

  // Verify user is removed
  auto removed_user = auth->GetUser("test_user");
  ASSERT_EQ(removed_user, std::nullopt);
}

TEST_F(AuthWithStorage, MultiTenantRoleWithGlobalRoles) {
  // Create roles
  ASSERT_TRUE(auth->AddRole("global_role"));
  ASSERT_TRUE(auth->AddRole("mt_role"));

  auto global_role = auth->GetRole("global_role");
  auto mt_role = auth->GetRole("mt_role");
  ASSERT_NE(global_role, std::nullopt);
  ASSERT_NE(mt_role, std::nullopt);

  global_role->db_access().Grant("db1");
  global_role->db_access().Grant("db2");
  mt_role->db_access().Grant("db1");

  auth->SaveRole(*global_role);
  auth->SaveRole(*mt_role);

  // Create a user
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  // Set global role
  user->ClearAllRoles();
  user->AddRole(*global_role);
  auth->SaveUser(*user);

  // Try to set the same role as multi-tenant role (should fail)
  ASSERT_THROW(user->AddMultiTenantRole(*global_role, "db1"), AuthException);

  // Try to set different role as multi-tenant role (should succeed)
  ASSERT_NO_THROW(user->AddMultiTenantRole(*mt_role, "db1"));
  auth->SaveUser(*user);

  // Verify both roles are present
  auto updated_user = auth->GetUser("test_user");
  ASSERT_NE(updated_user, std::nullopt);
  ASSERT_EQ(updated_user->roles().size(), 2);  // global_role + mt_role
  ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
  ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").begin()->rolename(), "mt_role");

  // Clear global roles
  updated_user->ClearAllRoles();
  auth->SaveUser(*updated_user);

  // Now should be able to set global_role as multi-tenant role
  ASSERT_NO_THROW(updated_user->AddMultiTenantRole(*global_role, "db1"));
  auth->SaveUser(*updated_user);

  auto final_user = auth->GetUser("test_user");
  ASSERT_NE(final_user, std::nullopt);
  ASSERT_EQ(final_user->GetMultiTenantRoles("db1").size(), 1);
  ASSERT_EQ(final_user->GetMultiTenantRoles("db1").begin()->rolename(), "global_role");
}

TEST_F(AuthWithStorage, MultiTenantRoleEdgeCases) {
  // Create a user without any multi-tenant roles
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  // Test getting roles for non-existent database
  auto roles = user->GetMultiTenantRoles("non_existent_db");
  ASSERT_EQ(roles.size(), 0);

  // Test clearing roles for non-existent database
  ASSERT_NO_THROW(user->ClearMultiTenantRoles("non_existent_db"));

  // Test with role that has access to multiple databases
  ASSERT_TRUE(auth->AddRole("test_role"));
  auto role = auth->GetRole("test_role");
  ASSERT_NE(role, std::nullopt);

  role->db_access().Grant("db1");
  role->db_access().Grant("db2");
  auth->SaveRole(*role);

  user->AddMultiTenantRole(*role, "db1");
  auth->SaveUser(*user);

  // Verify role is set for db1 but not db2
  auto updated_user = auth->GetUser("test_user");
  ASSERT_NE(updated_user, std::nullopt);
  ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
  ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 0);

  // Test setting role for db2 as well
  updated_user->AddMultiTenantRole(*role, "db2");
  auth->SaveUser(*updated_user);

  auto final_user = auth->GetUser("test_user");
  ASSERT_NE(final_user, std::nullopt);
  ASSERT_EQ(final_user->GetMultiTenantRoles("db1").size(), 1);
  ASSERT_EQ(final_user->GetMultiTenantRoles("db2").size(), 1);

  // Test clearing one database
  final_user->ClearMultiTenantRoles("db1");
  auth->SaveUser(*final_user);

  auto cleared_user = auth->GetUser("test_user");
  ASSERT_NE(cleared_user, std::nullopt);
  ASSERT_EQ(cleared_user->GetMultiTenantRoles("db1").size(), 0);
  ASSERT_EQ(cleared_user->GetMultiTenantRoles("db2").size(), 1);
}

TEST_F(AuthWithStorage, MultiTenantRoleRemoveRole) {
  // Create a role with database access
  ASSERT_TRUE(auth->AddRole("test_role"));
  auto role = auth->GetRole("test_role");
  ASSERT_NE(role, std::nullopt);

  role->db_access().Grant("db1");
  auth->SaveRole(*role);

  // Create a user and set multi-tenant role
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  user->AddMultiTenantRole(*role, "db1");
  auth->SaveUser(*user);

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
    ASSERT_EQ(updated_user->roles().size(), 1);
  }

  // Remove the role
  ASSERT_TRUE(auth->RemoveRole("test_role"));

  // Verify role is removed
  auto removed_role = auth->GetRole("test_role");
  ASSERT_EQ(removed_role, std::nullopt);

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 0);
    ASSERT_EQ(updated_user->roles().size(), 0);
  }

  // Re-add the role
  ASSERT_TRUE(auth->AddRole("test_role"));
  role = auth->GetRole("test_role");
  ASSERT_NE(role, std::nullopt);

  role->db_access().Grant("db1");
  auth->SaveRole(*role);

  // Verify user still does not have the role
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 0);
    ASSERT_EQ(updated_user->roles().size(), 0);
  }

  user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);
  user->AddMultiTenantRole(*role, "db1");
  auth->SaveUser(*user);

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
    ASSERT_EQ(updated_user->roles().size(), 1);
  }
}

TEST_F(AuthWithStorage, MultiTenantRoleAtDifferentDBs) {
  // Create a role with database access
  ASSERT_TRUE(auth->AddRole("test_role"));
  auto role = auth->GetRole("test_role");
  ASSERT_NE(role, std::nullopt);

  role->db_access().Grant("db1");
  role->db_access().Grant("db2");
  auth->SaveRole(*role);

  // Create a user
  ASSERT_TRUE(auth->AddUser("test_user"));
  auto user = auth->GetUser("test_user");
  ASSERT_NE(user, std::nullopt);

  // Verify user has no access to db1 or db2
  ASSERT_FALSE(user->HasAccess("db1"));
  ASSERT_FALSE(user->HasAccess("db2"));

  // Set multi-tenant role
  user->AddMultiTenantRole(*role, "db1");
  auth->SaveUser(*user);

  // Verify user has access to db1 but not db2
  ASSERT_TRUE(user->HasAccess("db1"));
  ASSERT_FALSE(user->HasAccess("db2"));

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 0);
  }

  // Add role to db2
  user->AddMultiTenantRole(*role, "db2");
  auth->SaveUser(*user);

  // Verify user has access to db1 and db2
  ASSERT_TRUE(user->HasAccess("db1"));
  ASSERT_TRUE(user->HasAccess("db2"));

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 1);
  }

  // Remove role from db1
  user->ClearMultiTenantRoles("db1");
  auth->SaveUser(*user);

  // Verify user has no access to db1 or db2
  ASSERT_FALSE(user->HasAccess("db1"));
  ASSERT_TRUE(user->HasAccess("db2"));

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 0);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 1);
  }

  // Remove role from db2
  user->ClearMultiTenantRoles("db2");
  auth->SaveUser(*user);

  // Verify user has no access
  ASSERT_FALSE(user->HasAccess("db1"));
  ASSERT_FALSE(user->HasAccess("db2"));

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 0);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 0);
  }

  // Re-add role to db1
  user->AddMultiTenantRole(*role, "db1");
  auth->SaveUser(*user);

  // Verify user has access to db1 but not db2
  ASSERT_TRUE(user->HasAccess("db1"));
  ASSERT_FALSE(user->HasAccess("db2"));

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 0);
  }

  // Re-add role to db2
  user->AddMultiTenantRole(*role, "db2");
  auth->SaveUser(*user);

  // Verify user is updated
  {
    auto updated_user = auth->GetUser("test_user");
    ASSERT_NE(updated_user, std::nullopt);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db1").size(), 1);
    ASSERT_EQ(updated_user->GetMultiTenantRoles("db2").size(), 1);
  }

  // Verify user has access to both databases
  ASSERT_TRUE(user->HasAccess("db1"));
  ASSERT_TRUE(user->HasAccess("db2"));
}
TEST_F(AuthWithStorage, AddProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));
  ASSERT_FALSE(auth->CreateProfile("profile", {}));
}

TEST_F(AuthWithStorage, UpdateProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));
  ASSERT_TRUE(auth->UpdateProfile(
      "profile", {{memgraph::auth::UserProfiles::Limits::kSessions, memgraph::auth::UserProfiles::limit_t{1UL}}}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {}));
  ASSERT_FALSE(auth->UpdateProfile("non_profile", {}));
}

TEST_F(AuthWithStorage, DropProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));
  ASSERT_TRUE(auth->DropProfile("profile"));
  ASSERT_TRUE(auth->DropProfile("other_profile"));
  ASSERT_FALSE(auth->DropProfile("non_profile"));
}

TEST_F(AuthWithStorage, GetProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));
  {
    const auto profile = auth->GetProfile("profile");
    ASSERT_TRUE(profile);
    ASSERT_EQ(profile->name, "profile");
    ASSERT_EQ(profile->limits.size(), 0);
  }
  {
    const auto profile = auth->GetProfile("other_profile");
    ASSERT_TRUE(profile);
    ASSERT_EQ(profile->name, "other_profile");
    ASSERT_EQ(profile->limits.size(), 1);
  }
  ASSERT_TRUE(auth->DropProfile("profile"));
  ASSERT_FALSE(auth->GetProfile("profile"));
  ASSERT_TRUE(auth->DropProfile("other_profile"));
  ASSERT_FALSE(auth->GetProfile("other_profile"));
  ASSERT_FALSE(auth->GetProfile("non_profile"));
}

TEST_F(AuthWithStorage, AllProfiles) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));
  {
    const auto profiles = auth->AllProfiles();
    ASSERT_EQ(profiles.size(), 2);
    for (const auto &profile : profiles) {
      if (profile.name == "profile") {
        ASSERT_EQ(profile.limits.size(), 0);
      } else if (profile.name == "other_profile") {
        ASSERT_EQ(profile.limits.size(), 1);
      } else {
        FAIL() << "Unexpected profile name: " << profile.name;
      }
    }
  }
  ASSERT_TRUE(auth->DropProfile("profile"));
  {
    const auto profiles = auth->AllProfiles();
    ASSERT_EQ(profiles.size(), 1);
    for (const auto &profile : profiles) {
      if (profile.name == "other_profile") {
        ASSERT_EQ(profile.limits.size(), 1);
      } else {
        FAIL() << "Unexpected profile name: " << profile.name;
      }
    }
  }
  ASSERT_TRUE(auth->DropProfile("other_profile"));
  ASSERT_EQ(auth->AllProfiles().size(), 0);
}

TEST_F(AuthWithStorage, SetProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));

  ASSERT_TRUE(auth->AddUser("user"));

  ASSERT_NO_THROW(auth->SetProfile("profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "profile");
  }
  ASSERT_NO_THROW(auth->SetProfile("other_profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "other_profile");
  }
  ASSERT_THROW(auth->SetProfile("non_profile", "user"), memgraph::auth::AuthException);
  // NOTE: We don't check for non-existing profiles in the new architecture
  ASSERT_NO_THROW(auth->SetProfile("profile", "non_user"));
}

// Role-based profile management is no longer supported in the new architecture

TEST_F(AuthWithStorage, SetProfileUserWRole) {
  // In the new architecture, only users can have profiles, not roles
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{10UL}}}));

  ASSERT_TRUE(auth->AddUser("user"));
  ASSERT_TRUE(auth->AddRole("role"));

  // Set role for user
  auto user = auth->GetUser("user");
  user->ClearAllRoles();
  user->AddRole(*auth->GetRole("role"));
  auth->SaveUser(*user);

  // Set profile for user
  ASSERT_NO_THROW(auth->SetProfile("profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "profile");
  }

  // Change profile for user
  ASSERT_NO_THROW(auth->SetProfile("other_profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "other_profile");
  }

  // Update profile and set it again
  auth->UpdateProfile("profile",
                      {{memgraph::auth::UserProfiles::Limits::kSessions, memgraph::auth::UserProfiles::limit_t{1UL}}});
  ASSERT_NO_THROW(auth->SetProfile("profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "profile");
  }

  // Remove role and verify profile is still there
  auth->RemoveRole("role");
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "profile");
  }
}

TEST_F(AuthWithStorage, RevokeProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));

  ASSERT_TRUE(auth->AddUser("user"));

  ASSERT_NO_THROW(auth->SetProfile("profile", "user"));
  ASSERT_TRUE(auth->GetProfileForUsername("user"));
  ASSERT_NO_THROW(auth->RevokeProfile("user"));
  ASSERT_FALSE(auth->GetProfileForUsername("user"));
  ASSERT_NO_THROW(auth->RevokeProfile("user"));
  // NOTE: We don't check for non-existing users in the new architecture
  ASSERT_NO_THROW(auth->RevokeProfile("non_user"));

  ASSERT_NO_THROW(auth->SetProfile("profile", "user"));
  ASSERT_TRUE(auth->DropProfile("profile"));
  ASSERT_FALSE(auth->GetProfileForUsername("user"));
}

// Role-based profile management is no longer supported in the new architecture

TEST_F(AuthWithStorage, RevokeProfileUserWRole) {
  // In the new architecture, only users can have profiles, not roles
  ASSERT_TRUE(auth->CreateProfile(
      "profile", {{memgraph::auth::UserProfiles::Limits::kSessions, memgraph::auth::UserProfiles::limit_t{1UL}}}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{10UL}}}));

  ASSERT_TRUE(auth->AddUser("user"));
  ASSERT_TRUE(auth->AddRole("role"));

  // Set role for user
  auto user = auth->GetUser("user");
  user->ClearAllRoles();
  user->AddRole(*auth->GetRole("role"));
  auth->SaveUser(*user);

  // Set profile for user
  ASSERT_NO_THROW(auth->SetProfile("profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "profile");
  }

  // Change profile for user
  ASSERT_NO_THROW(auth->SetProfile("other_profile", "user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_TRUE(profile);
    ASSERT_EQ(*profile, "other_profile");
  }

  // Revoke profile from user
  ASSERT_NO_THROW(auth->RevokeProfile("user"));
  {
    const auto profile = auth->GetProfileForUsername("user");
    ASSERT_FALSE(profile);
  }
}

TEST_F(AuthWithStorage, GetUsersForProfile) {
  ASSERT_TRUE(auth->CreateProfile("profile", {}));
  ASSERT_TRUE(auth->CreateProfile("other_profile", {}));
  ASSERT_TRUE(auth->UpdateProfile("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                     memgraph::auth::UserProfiles::limit_t{1UL}}}));

  ASSERT_TRUE(auth->AddUser("user1"));
  ASSERT_TRUE(auth->AddUser("user2"));
  ASSERT_TRUE(auth->AddUser("user3"));
  ASSERT_TRUE(auth->AddUser("user4"));

  ASSERT_NO_THROW(auth->SetProfile("profile", "user1"));
  {
    const auto users = auth->GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 1);
    ASSERT_TRUE(users.find("user1") != users.end());
  }
  ASSERT_NO_THROW(auth->RevokeProfile("user1"));
  {
    const auto users = auth->GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 0);
  }

  ASSERT_NO_THROW(auth->SetProfile("profile", "user1"));
  ASSERT_NO_THROW(auth->SetProfile("profile", "user2"));
  ASSERT_NO_THROW(auth->SetProfile("profile", "user3"));
  {
    const auto users = auth->GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 3);
    for (const auto &user : users) {
      ASSERT_TRUE(user == "user1" || user == "user2" || user == "user3");
    }
  }
  ASSERT_NO_THROW(auth->RevokeProfile("user3"));
  {
    const auto users = auth->GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 2);
    for (const auto &user : users) {
      ASSERT_TRUE(user == "user1" || user == "user2");
    }
  }
  ASSERT_TRUE(auth->RemoveUser("user2"));
  {
    const auto users = auth->GetUsernamesForProfile("profile");
    // ASSERT_EQ(users.size(), 1);
    // ASSERT_EQ(users[0], "user1");
    // NOTE: We don't check for non-existing users in the new architecture
    ASSERT_EQ(users.size(), 2);
    for (const auto &user : users) {
      ASSERT_TRUE(user == "user1" || user == "user2");
    }
  }
  ASSERT_TRUE(auth->DropProfile("profile"));
  ASSERT_THROW(auth->GetUsernamesForProfile("profile"), memgraph::auth::AuthException);

  ASSERT_EQ(auth->GetUsernamesForProfile("other_profile").size(), 0);
}

// Role-based profile management is no longer supported in the new architecture

#endif  // MG_ENTERPRISE
