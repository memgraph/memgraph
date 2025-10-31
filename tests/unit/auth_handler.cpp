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

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <openssl/x509_vfy.h>
#include <atomic>
#include <chrono>
#include <latch>
#include <thread>
#include <variant>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "dbms/constants.hpp"
#include "exceptions.hpp"
#include "frontend/ast/ast_visitor.hpp"
#include "glue/auth_global.hpp"
#include "glue/auth_handler.hpp"
#include "query/typed_value.hpp"
#include "utils/file.hpp"
#include "utils/resource_monitoring.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

#include <nlohmann/json.hpp>

class AuthQueryHandlerFixture : public testing::Test {
 protected:
  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_auth_handler"};
  std::filesystem::path auth_dir_ =
      test_folder_ / ("unit_auth_handler_test_" + std::to_string(static_cast<int>(getpid())));

#ifdef MG_ENTERPRISE
  memgraph::utils::ResourceMonitoring resources;
  memgraph::auth::FineGrainedAccessHandler handler{};
#endif

  std::optional<memgraph::auth::SynchedAuth> auth{std::in_place, auth_dir_, memgraph::auth::Auth::Config{/* default */}
#ifdef MG_ENTERPRISE
                                                  ,
                                                  &resources
#endif
  };
  memgraph::glue::AuthQueryHandler auth_handler{&*auth};

  std::string user_name = "Mate";
  std::string edge_type_repr = "EdgeType1";
  std::string label_repr = "Label1";
  memgraph::auth::Permissions perms{};
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder_);
    memgraph::license::global_license_checker.EnableTesting();
  }

  void TearDown() override {
    std::filesystem::remove_all(test_folder_);
    perms = memgraph::auth::Permissions{};
#ifdef MG_ENTERPRISE
    handler = memgraph::auth::FineGrainedAccessHandler{};
#endif
  }
};

TEST_F(AuthQueryHandlerFixture, GivenAuthQueryHandlerWhenInitializedHaveNoUsernamesOrRolenames) {
  ASSERT_EQ(auth_handler.GetUsernames().size(), 0);
  ASSERT_EQ(auth_handler.GetRolenames().size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenNoDeniesOrGrantsThenNothingIsReturned) {
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth.value()->SaveUser(user);

  { ASSERT_EQ(auth_handler.GetUsernames().size(), 1); }

  {
    auto privileges =
        auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});

    ASSERT_EQ(privileges.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenAddedGrantPermissionThenItIsReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenAddedDenyPermissionThenItIsReturned) {
  perms.Deny(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "DENIED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenPrivilegeRevokedThenNothingIsReturned) {
  perms.Deny(memgraph::auth::Permission::MATCH);
  perms.Revoke(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenPrivilegeGrantedThenItIsReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth.value()->SaveRole(role);

  { ASSERT_EQ(auth_handler.GetRolenames().size(), 1); }

  {
    auto privileges =
        auth_handler.GetPrivileges("Mates_role", std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
    ASSERT_EQ(privileges.size(), 1);

    auto result = *privileges.begin();
    ASSERT_EQ(result.size(), 3);

    ASSERT_TRUE(result[0].IsString());
    ASSERT_EQ(result[0].ValueString(), "MATCH");

    ASSERT_TRUE(result[1].IsString());
    ASSERT_EQ(result[1].ValueString(), "GRANT");

    ASSERT_TRUE(result[2].IsString());
    ASSERT_EQ(result[2].ValueString(), "GRANTED TO ROLE");
  }
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenPrivilegeDeniedThenItIsReturned) {
  perms.Deny(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth.value()->SaveRole(role);

  auto privileges =
      auth_handler.GetPrivileges("Mates_role", std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "DENIED TO ROLE");
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenPrivilegeRevokedThenNothingIsReturned) {
  perms.Deny(memgraph::auth::Permission::MATCH);
  perms.Revoke(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth.value()->SaveRole(role);

  auto privileges =
      auth_handler.GetPrivileges("Mates_role", std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedTwoPrivilegesThenBothAreReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 2);
}

TEST_F(AuthQueryHandlerFixture, GivenUserAndRoleWhenOneGrantedAndOtherGrantedThenBothArePrinted) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth.value()->SaveRole(role);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.ClearAllRoles();
  user.AddRole(role);
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER, GRANTED TO ROLE");
}

TEST_F(AuthQueryHandlerFixture, GivenUserAndRoleWhenOneDeniedAndOtherDeniedThenBothArePrinted) {
  perms.Deny(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth.value()->SaveRole(role);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.ClearAllRoles();
  user.AddRole(role);
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "DENIED TO USER, DENIED TO ROLE");
}

TEST_F(AuthQueryHandlerFixture, GivenUserAndRoleWhenOneGrantedAndOtherDeniedThenBothArePrinted) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Deny(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", role_perms};
  auth.value()->SaveRole(role);

  memgraph::auth::Permissions user_perms{};
  user_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{
      user_name,
      std::nullopt,
      user_perms,
  };
  user.ClearAllRoles();
  user.AddRole(role);
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER, DENIED TO ROLE");
}

TEST_F(AuthQueryHandlerFixture, GivenUserAndRoleWhenOneDeniedAndOtherGrantedThenBothArePrinted) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", role_perms};
  auth.value()->SaveRole(role);

  memgraph::auth::Permissions user_perms{};
  user_perms.Deny(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, user_perms};
  user.ClearAllRoles();
  user.AddRole(role);
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "MATCH");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "DENIED TO USER, GRANTED TO ROLE");
}

#ifdef MG_ENTERPRISE
TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedPrivilegeOnLabelThenIsDisplayed) {
  auto label_permission = memgraph::auth::FineGrainedAccessPermissions();
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{label_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ ON NODES CONTAINING LABELS :Label1 MATCHING ANY");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedMultiplePrivilegesOnLabelThenAllAreDisplayed) {
  auto label_permission = memgraph::auth::FineGrainedAccessPermissions();
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::CREATE);
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::READ);
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::UPDATE);
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::DELETE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{label_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS :Label1 MATCHING ANY");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalPrivilegeOnLabelThenIsDisplayed) {
  auto label_permission = memgraph::auth::FineGrainedAccessPermissions();
  label_permission.GrantGlobal(memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{label_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ ON ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalMultiplePrivilegesOnLabelThenAllAreDisplayed) {
  auto label_permission = memgraph::auth::FineGrainedAccessPermissions();
  label_permission.GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
  label_permission.GrantGlobal(memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{label_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ, UPDATE ON ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalAllPrivilegesOnLabelThenAllAreDisplayed) {
  auto label_permission = memgraph::auth::FineGrainedAccessPermissions();
  label_permission.GrantGlobal(memgraph::auth::kAllPermissions);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{label_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "CREATE, READ, UPDATE, DELETE ON ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER");
}

// EDGE_TYPES
TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedPrivilegeOnEdgeTypeThenIsDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ ON EDGES CONTAINING TYPES :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedMultiplePrivilegesOnEdgeTypeThenAllAreDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::READ);
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ, UPDATE ON EDGES CONTAINING TYPES :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAllPrivilegesOnEdgeTypeThenAllAreDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.Grant({edge_type_repr}, memgraph::auth::kAllPermissions);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "CREATE, READ, UPDATE, DELETE ON EDGES CONTAINING TYPES :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalPrivilegeOnEdgeTypeThenIsDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.GrantGlobal(memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ ON ALL EDGE_TYPES");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalMultiplePrivilegesOnEdgeTypeThenAllAreDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
  edge_permission.GrantGlobal(memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},

  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ, UPDATE ON ALL EDGE_TYPES");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalAllPrivilegesOnEdgeTypeThenAllAreDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.GrantGlobal(memgraph::auth::kAllPermissions);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "CREATE, READ, UPDATE, DELETE ON ALL EDGE_TYPES");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAndDeniedOnLabelThenNoPermission) {
  auto label_permission = memgraph::auth::FineGrainedAccessPermissions();
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::READ);
  label_permission.Grant({label_repr}, memgraph::auth::FineGrainedPermission::NOTHING);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{label_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "NOTHING ON NODES CONTAINING LABELS :Label1 MATCHING ANY");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION DENIED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAndDeniedOnEdgeTypeThenNoPermission) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::READ);
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::NOTHING);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "NOTHING ON EDGES CONTAINING TYPES :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "DENY");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION DENIED TO USER");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedReadAndDeniedUpdateThenOneIsDisplayed) {
  auto edge_permission = memgraph::auth::FineGrainedAccessPermissions();
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::READ);
  edge_permission.Grant({edge_type_repr}, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{edge_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "READ ON EDGES CONTAINING TYPES :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "GRANT");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
}

namespace {
const memgraph::query::UserProfileQuery::LimitValueResult unlimited{
    .type = memgraph::query::UserProfileQuery::LimitValueResult::Type::UNLIMITED};
const memgraph::query::UserProfileQuery::LimitValueResult quantity{
    .quantity = {.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY, .value = 1}};
const memgraph::query::UserProfileQuery::LimitValueResult mem_limit{
    .mem_limit = {
        .type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT, .value = 1, .scale = 1024}};
}  // namespace

TEST_F(AuthQueryHandlerFixture, CreateProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      {}, nullptr));

  ASSERT_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(auth_handler.CreateProfile(
                   "another_profile",
                   {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], mem_limit}},
                   {}, nullptr),
               memgraph::query::QueryRuntimeException);

  {
    // Stop auth and check if profiles are saved in the durable storage
    auth.reset();
    memgraph::kvstore::KVStore check_durable_kvstore{this->auth_dir_};
    memgraph::auth::UserProfiles check_durable_profiles{check_durable_kvstore};
    for (const auto &profile : check_durable_profiles.GetAll()) {
      if (profile.name == "profile") {
        ASSERT_EQ(profile.limits.size(), 0);
      } else if (profile.name == "other_profile") {
        ASSERT_EQ(profile.limits.size(), 1);
        ASSERT_EQ(profile.limits.begin()->first, memgraph::auth::UserProfiles::Limits::kSessions);
        ASSERT_FALSE(
            std::holds_alternative<memgraph::auth::UserProfiles::unlimitted_t>(profile.limits.begin()->second));
        ASSERT_EQ(std::get<uint64_t>(profile.limits.begin()->second), 1);
      } else {
        FAIL() << "Unexpected profile name: " << profile.name;
      }
    }
  }
}

TEST_F(AuthQueryHandlerFixture, CreateProfileWithPredefinedUsernames) {
  // Create users first
  memgraph::auth::User user1 = memgraph::auth::User{"user1", std::nullopt, perms};
  memgraph::auth::User user2 = memgraph::auth::User{"user2", std::nullopt, perms};
  memgraph::auth::User user3 = memgraph::auth::User{"user3", std::nullopt, perms};
  auth.value()->SaveUser(user1);
  auth.value()->SaveUser(user2);
  auth.value()->SaveUser(user3);

  // Test creating profile with predefined usernames
  std::unordered_set<std::string> usernames = {"user1", "user2"};
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile_with_users", {}, usernames, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "profile_with_users_and_limits",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}}, {}, nullptr));

  // Test creating profile with single username
  std::unordered_set<std::string> single_user = {"user3"};
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile_single_user", {}, single_user, nullptr));

  // Test creating profile with empty usernames set (should work same as no usernames)
  std::unordered_set<std::string> empty_usernames = {};
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile_empty_users", {}, empty_usernames, nullptr));

  // Test that creating duplicate profile with usernames throws
  ASSERT_THROW(auth_handler.CreateProfile("profile_with_users", {}, usernames, nullptr),
               memgraph::query::QueryRuntimeException);

  // Test that usernames are properly assigned to profiles
  auto profile1_users = auth_handler.GetUsernamesForProfile("profile_with_users");
  ASSERT_EQ(profile1_users.size(), 2);
  ASSERT_TRUE(std::find(profile1_users.begin(), profile1_users.end(), "user1") != profile1_users.end());
  ASSERT_TRUE(std::find(profile1_users.begin(), profile1_users.end(), "user2") != profile1_users.end());

  auto profile2_users = auth_handler.GetUsernamesForProfile("profile_single_user");
  ASSERT_EQ(profile2_users.size(), 1);
  ASSERT_EQ(profile2_users[0], "user3");

  auto profile3_users = auth_handler.GetUsernamesForProfile("profile_empty_users");
  ASSERT_EQ(profile3_users.size(), 0);

  // Test that users are properly linked to profiles
  auto user1_profile = auth_handler.GetProfileForUser("user1");
  ASSERT_TRUE(user1_profile.has_value());
  ASSERT_EQ(*user1_profile, "profile_with_users");

  auto user2_profile = auth_handler.GetProfileForUser("user2");
  ASSERT_TRUE(user2_profile.has_value());
  ASSERT_EQ(*user2_profile, "profile_with_users");

  auto user3_profile = auth_handler.GetProfileForUser("user3");
  ASSERT_TRUE(user3_profile.has_value());
  ASSERT_EQ(*user3_profile, "profile_single_user");

  // Test that creating a profile with usernames that are already in another profile
  // should move them to the new profile
  std::unordered_set<std::string> new_usernames = {"user1", "user3"};
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile_moving_users", {}, new_usernames, nullptr));

  // Verify that users were moved to the new profile
  auto moved_profile_users = auth_handler.GetUsernamesForProfile("profile_moving_users");
  ASSERT_EQ(moved_profile_users.size(), 2);
  ASSERT_TRUE(std::find(moved_profile_users.begin(), moved_profile_users.end(), "user1") != moved_profile_users.end());
  ASSERT_TRUE(std::find(moved_profile_users.begin(), moved_profile_users.end(), "user3") != moved_profile_users.end());

  // Verify that user1 was removed from the original profile
  auto original_profile_users = auth_handler.GetUsernamesForProfile("profile_with_users");
  ASSERT_EQ(original_profile_users.size(), 1);
  ASSERT_EQ(original_profile_users[0], "user2");

  // Verify that user3 was removed from the single user profile
  auto single_user_profile_users = auth_handler.GetUsernamesForProfile("profile_single_user");
  ASSERT_EQ(single_user_profile_users.size(), 0);

  // Verify updated profile assignments
  auto updated_user1_profile = auth_handler.GetProfileForUser("user1");
  ASSERT_TRUE(updated_user1_profile.has_value());
  ASSERT_EQ(*updated_user1_profile, "profile_moving_users");

  auto updated_user3_profile = auth_handler.GetProfileForUser("user3");
  ASSERT_TRUE(updated_user3_profile.has_value());
  ASSERT_EQ(*updated_user3_profile, "profile_moving_users");

  {
    // Stop auth and check if profiles with usernames are saved in the durable storage
    auth.reset();
    memgraph::kvstore::KVStore check_durable_kvstore{this->auth_dir_};
    memgraph::auth::UserProfiles check_durable_profiles{check_durable_kvstore};

    bool found_profile_with_users = false;
    bool found_profile_moving_users = false;
    bool found_profile_single_user = false;

    for (const auto &profile : check_durable_profiles.GetAll()) {
      if (profile.name == "profile_with_users") {
        ASSERT_EQ(profile.limits.size(), 0);
        ASSERT_EQ(profile.usernames.size(), 1);
        ASSERT_TRUE(profile.usernames.contains("user2"));
        found_profile_with_users = true;
      } else if (profile.name == "profile_moving_users") {
        ASSERT_EQ(profile.limits.size(), 0);
        ASSERT_EQ(profile.usernames.size(), 2);
        ASSERT_TRUE(profile.usernames.contains("user1"));
        ASSERT_TRUE(profile.usernames.contains("user3"));
        found_profile_moving_users = true;
      } else if (profile.name == "profile_single_user") {
        ASSERT_EQ(profile.limits.size(), 0);
        ASSERT_EQ(profile.usernames.size(), 0);
        found_profile_single_user = true;
      }
    }

    ASSERT_TRUE(found_profile_with_users);
    ASSERT_TRUE(found_profile_moving_users);
    ASSERT_TRUE(found_profile_single_user);
  }
}

TEST_F(AuthQueryHandlerFixture, UpdateProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      {}, nullptr));

  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));
  ASSERT_THROW(
      auth_handler.UpdateProfile(
          "profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], mem_limit}},
          nullptr),
      memgraph::query::QueryRuntimeException);

  ASSERT_THROW(auth_handler.UpdateProfile("non_profile", {}, nullptr), memgraph::query::QueryRuntimeException);

  {
    // Stop auth and check if profiles are saved in the durable storage
    auth.reset();
    memgraph::kvstore::KVStore check_durable_kvstore{this->auth_dir_};
    memgraph::auth::UserProfiles check_durable_profiles{check_durable_kvstore};
    for (const auto &profile : check_durable_profiles.GetAll()) {
      if (profile.name == "profile") {
        ASSERT_EQ(profile.limits.size(), 1);
        ASSERT_EQ(profile.limits.begin()->first, memgraph::auth::UserProfiles::Limits::kSessions);
        ASSERT_FALSE(
            std::holds_alternative<memgraph::auth::UserProfiles::unlimitted_t>(profile.limits.begin()->second));
        ASSERT_EQ(std::get<uint64_t>(profile.limits.begin()->second), 1);
      } else if (profile.name == "other_profile") {
        ASSERT_EQ(profile.limits.size(), 1);
        ASSERT_EQ(profile.limits.begin()->first, memgraph::auth::UserProfiles::Limits::kSessions);
        ASSERT_FALSE(
            std::holds_alternative<memgraph::auth::UserProfiles::unlimitted_t>(profile.limits.begin()->second));
        ASSERT_EQ(std::get<uint64_t>(profile.limits.begin()->second), 1);
      } else {
        FAIL() << "Unexpected profile name: " << profile.name;
      }
    }
  }
}

TEST_F(AuthQueryHandlerFixture, DropProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));
  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_THROW(auth_handler.DropProfile("non_profile", nullptr), memgraph::query::QueryRuntimeException);

  {
    // Stop auth and check if profiles are saved in the durable storage
    auth.reset();
    memgraph::kvstore::KVStore check_durable_kvstore{this->auth_dir_};
    memgraph::auth::UserProfiles check_durable_profiles{check_durable_kvstore};
    for (const auto &profile : check_durable_profiles.GetAll()) {
      if (profile.name == "profile") {
        ASSERT_EQ(profile.limits.size(), 0);
      } else if (profile.name == "other_profile") {
        ASSERT_EQ(profile.limits.size(), 1);
        ASSERT_EQ(profile.limits.begin()->first, memgraph::auth::UserProfiles::Limits::kSessions);
        ASSERT_FALSE(
            std::holds_alternative<memgraph::auth::UserProfiles::unlimitted_t>(profile.limits.begin()->second));
        ASSERT_EQ(std::get<uint64_t>(profile.limits.begin()->second), 1);
      } else {
        FAIL() << "Unexpected profile name: " << profile.name;
      }
    }
  }
}

TEST_F(AuthQueryHandlerFixture, GetProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  {
    const auto profile = auth_handler.GetProfile("profile");
    ASSERT_EQ(profile.size(), memgraph::auth::UserProfiles::kLimits.size());
    for (const auto &[name, limit] : profile) {
      ASSERT_NE(
          std::find(memgraph::auth::UserProfiles::kLimits.begin(), memgraph::auth::UserProfiles::kLimits.end(), name),
          memgraph::auth::UserProfiles::kLimits.end());
      ASSERT_EQ(limit, unlimited);
    }
  }
  {
    const auto profile = auth_handler.GetProfile("other_profile");
    ASSERT_EQ(profile.size(), memgraph::auth::UserProfiles::kLimits.size());
    for (const auto &[name, limit] : profile) {
      ASSERT_NE(
          std::find(memgraph::auth::UserProfiles::kLimits.begin(), memgraph::auth::UserProfiles::kLimits.end(), name),
          memgraph::auth::UserProfiles::kLimits.end());
      if (name == memgraph::auth::UserProfiles::kLimits[0]) {
        ASSERT_EQ(limit, quantity);
      } else {
        ASSERT_EQ(limit, unlimited);
      }
    }
  }
  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr));
  ASSERT_THROW(auth_handler.GetProfile("profile"), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, AllProfiles) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  {
    const auto profiles = auth_handler.AllProfiles();
    ASSERT_EQ(profiles.size(), 2);
    for (const auto &[name, profile] : profiles) {
      ASSERT_TRUE(name == "profile" || name == "other_profile");
      ASSERT_EQ(profile.size(), memgraph::auth::UserProfiles::kLimits.size());
      for (const auto &[key, limit] : profile) {
        ASSERT_NE(
            std::find(memgraph::auth::UserProfiles::kLimits.begin(), memgraph::auth::UserProfiles::kLimits.end(), key),
            memgraph::auth::UserProfiles::kLimits.end());
      }
    }
  }
  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr));
  {
    const auto profiles = auth_handler.AllProfiles();
    ASSERT_EQ(profiles.size(), 1);
    const auto &[name, profile] = profiles[0];
    ASSERT_TRUE(name == "other_profile");
    ASSERT_EQ(profile.size(), memgraph::auth::UserProfiles::kLimits.size());
    for (const auto &[key, limit] : profile) {
      ASSERT_NE(
          std::find(memgraph::auth::UserProfiles::kLimits.begin(), memgraph::auth::UserProfiles::kLimits.end(), key),
          memgraph::auth::UserProfiles::kLimits.end());
    }
  }
  ASSERT_NO_THROW(auth_handler.DropProfile("other_profile", nullptr));
  {
    const auto profiles = auth_handler.AllProfiles();
    ASSERT_EQ(profiles.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, SetProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_THROW(auth_handler.SetProfile("non_profile", "user", nullptr), memgraph::query::QueryRuntimeException);
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  // In the new architecture, we don't validate user existence when setting profiles
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "non_user", nullptr));
}

TEST_F(AuthQueryHandlerFixture, RevokeProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  // In the new architecture, we don't validate user existence when revoking profiles
  ASSERT_NO_THROW(auth_handler.RevokeProfile("non_user", nullptr));
}

TEST_F(AuthQueryHandlerFixture, UserProfileRole) {
  // In the new architecture, only users can have profiles, not roles
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));
  auth_handler.SetRoles("user", {"role"}, {}, nullptr);

  // Set profile for user (not role)
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }

  // Change profile for user
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }

  // Update profile limits
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], mem_limit}}, nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }

  // Revoke profile from user
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }

  // Drop role and verify user still exists
  auth_handler.DropRole("role", nullptr);
  {
    ASSERT_THROW(auth_handler.GetUsernamesForRole("role"), memgraph::query::QueryRuntimeException);
    const auto user = auth.value()->GetUser("user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->GetRoles().size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, GetProfileForUser) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));

  ASSERT_FALSE(auth_handler.GetProfileForUser("user"));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr));
  {
    const auto profile = auth_handler.GetProfileForUser("user");
    ASSERT_TRUE(profile.has_value());
    ASSERT_EQ(profile.value(), "profile");
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr));
  {
    const auto profile = auth_handler.GetProfileForUser("user");
    ASSERT_TRUE(profile.has_value());
    ASSERT_EQ(profile.value(), "other_profile");
  }
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.DropProfile("other_profile", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_FALSE(auth_handler.GetProfileForUser("user"));

  // In the new architecture, we don't validate user existence when getting profiles
  ASSERT_FALSE(auth_handler.GetProfileForUser("non_user"));
}

// Role-based profile management is no longer supported in the new architecture

TEST_F(AuthQueryHandlerFixture, GetUsersForProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user1", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user2", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user3", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user4", {}, nullptr));

  ASSERT_EQ(auth_handler.GetUsernamesForProfile("profile").size(), 0);

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user1", nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user2", nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user3", nullptr));
  {
    const auto users = auth_handler.GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 3);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user2") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user3") != users.end());
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user2", nullptr));
  {
    const auto users = auth_handler.GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 2);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user3") != users.end());
  }
  ASSERT_NO_THROW(auth_handler.DropUser("user3", nullptr));
  {
    // In the new architecture, dropping a user doesn't automatically remove them from profiles
    // The username remains in the profile even after the user is dropped
    const auto users = auth_handler.GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 2);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user3") != users.end());
  }
  ASSERT_THROW(auth_handler.GetUsernamesForProfile("non_profile"), memgraph::query::QueryRuntimeException);
}

// Role-based profile management is no longer supported in the new architecture

#endif

// Multi-tenant tests for database filtering
#ifdef MG_ENTERPRISE
TEST_F(AuthQueryHandlerFixture,
       GivenUserWithMultipleRolesWhenFilteringByDatabaseThenOnlyRelevantPrivilegesAreReturned) {
  // Create roles with different database access
  memgraph::auth::Permissions role1_perms{};
  role1_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role1 = memgraph::auth::Role{"role1", role1_perms};
  role1.db_access().Grant("db1");
  auth.value()->SaveRole(role1);

  memgraph::auth::Permissions role2_perms{};
  role2_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role2 = memgraph::auth::Role{"role2", role2_perms};
  role2.db_access().Grant("db2");
  auth.value()->SaveRole(role2);

  // Create user with both roles
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role1);
  user.AddRole(role2);
  auth.value()->SaveUser(user);

  // Test filtering by db1 - should only show MATCH permission from role1
  auto privileges_db1 = auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{"db1"}});
  ASSERT_EQ(privileges_db1.size(), 1);
  auto result_db1 = *privileges_db1.begin();
  ASSERT_EQ(result_db1.size(), 3);
  ASSERT_TRUE(result_db1[0].IsString());
  ASSERT_EQ(result_db1[0].ValueString(), "MATCH");
  ASSERT_TRUE(result_db1[2].IsString());
  ASSERT_EQ(result_db1[2].ValueString(), "GRANTED TO ROLE");

  // Test filtering by db2 - should only show CREATE permission from role2
  auto privileges_db2 = auth_handler.GetPrivileges(user_name, "db2");
  ASSERT_EQ(privileges_db2.size(), 1);
  auto result_db2 = *privileges_db2.begin();
  ASSERT_EQ(result_db2.size(), 3);
  ASSERT_TRUE(result_db2[0].IsString());
  ASSERT_EQ(result_db2[0].ValueString(), "CREATE");
  ASSERT_TRUE(result_db2[2].IsString());
  ASSERT_EQ(result_db2[2].ValueString(), "GRANTED TO ROLE");

  // Test filtering by db3 - should show no privileges
  auto privileges_db3 = auth_handler.GetPrivileges(user_name, "db3");
  ASSERT_EQ(privileges_db3.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWithRoleWhenFilteringByDatabaseWithNoAccessThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role);
  auth.value()->SaveUser(user);

  // Test filtering by db2 - role doesn't have access to db2
  auto privileges = auth_handler.GetPrivileges(user_name, "db2");
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWithMultipleRolesWhenFilteringByDefaultDatabaseThenAllPrivilegesAreReturned) {
  memgraph::auth::Permissions role1_perms{};
  role1_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role1 = memgraph::auth::Role{"role1", role1_perms};
  auth.value()->SaveRole(role1);

  memgraph::auth::Permissions role2_perms{};
  role2_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role2 = memgraph::auth::Role{"role2", role2_perms};
  auth.value()->SaveRole(role2);

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role1);
  user.AddRole(role2);
  auth.value()->SaveUser(user);

  // Test with empty database name (should return all privileges)
  {
    auto privileges = auth_handler.GetPrivileges(user_name, std::nullopt);
    ASSERT_EQ(privileges.size(), 2);
  }

  // Test with default database
  {
    auto privileges =
        auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
    ASSERT_EQ(privileges.size(), 2);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenUserWithRoleWhenFilteringByDatabaseWithDeniedAccessThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  role.db_access().Deny("db2");
  auth.value()->SaveRole(role);

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role);
  auth.value()->SaveUser(user);
  {
    auto privileges = auth_handler.GetPrivileges(user_name, "db1");
    ASSERT_EQ(privileges.size(), 1);
  }
  {
    auto privileges = auth_handler.GetPrivileges(user_name, "db2");
    ASSERT_EQ(privileges.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenUserWithRoleWhenFilteringByDatabaseWithAllowAllThenPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  role_perms.Deny(memgraph::auth::Permission::DELETE);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  role.db_access().Grant("db3");
  auth.value()->SaveRole(role);

  memgraph::auth::Permissions user_perms{};
  user_perms.Grant(memgraph::auth::Permission::DELETE);
  user_perms.Deny(memgraph::auth::Permission::MATCH);
  user_perms.Grant(memgraph::auth::Permission::AUTH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, user_perms};
  user.db_access().GrantAll();
  user.db_access().Deny("db3");
  user.db_access().Deny("db4");
  user.AddRole(role);
  auth.value()->SaveUser(user);

  // Test filtering by any database - should show privileges
  {
    auto privileges = auth_handler.GetPrivileges(user_name, "db1");
    ASSERT_EQ(privileges.size(), 3);
    {
      auto &result = privileges[0];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "DELETE");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER, DENIED TO ROLE");
    }
    {
      auto &result = privileges[1];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "MATCH");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "DENIED TO USER, GRANTED TO ROLE");
    }
    {
      auto &result = privileges[2];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "AUTH");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER");
    }
  }
  {
    auto privileges = auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{"db2"}});
    EXPECT_EQ(privileges.size(), 3);
    {
      auto &result = privileges[0];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "DELETE");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER");
    }
    {
      auto &result = privileges[1];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "MATCH");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "DENIED TO USER");
    }
    {
      auto &result = privileges[2];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "AUTH");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "GRANTED TO USER");
    }
  }
  {
    auto privileges = auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{"db3"}});
    EXPECT_EQ(privileges.size(), 0);
  }
  {
    auto privileges = auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{"db4"}});
    EXPECT_EQ(privileges.size(), 0);
  }
}
#endif

#ifdef MG_ENTERPRISE
TEST_F(AuthQueryHandlerFixture, GivenRoleWhenFilteringByDatabaseThenOnlyRelevantPrivilegesAreReturned) {
  // Create roles with different database access
  memgraph::auth::Permissions role1_perms{};
  role1_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role1 = memgraph::auth::Role{"role1", role1_perms};
  role1.db_access().Grant("db1");
  auth.value()->SaveRole(role1);

  memgraph::auth::Permissions role2_perms{};
  role2_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role2 = memgraph::auth::Role{"role2", role2_perms};
  role2.db_access().Grant("db2");
  auth.value()->SaveRole(role2);

  // Test filtering by db1 - should only show MATCH permission from role1
  auto privileges_db1 = auth_handler.GetPrivileges("role1", "db1");
  ASSERT_EQ(privileges_db1.size(), 1);
  auto result_db1 = *privileges_db1.begin();
  ASSERT_EQ(result_db1.size(), 3);
  ASSERT_TRUE(result_db1[0].IsString());
  ASSERT_EQ(result_db1[0].ValueString(), "MATCH");
  ASSERT_TRUE(result_db1[1].IsString());
  ASSERT_EQ(result_db1[1].ValueString(), "GRANT");
  ASSERT_TRUE(result_db1[2].IsString());
  ASSERT_EQ(result_db1[2].ValueString(), "GRANTED TO ROLE");

  // Test filtering by db2 - should only show CREATE permission from role2
  auto privileges_db2 = auth_handler.GetPrivileges("role2", "db2");
  ASSERT_EQ(privileges_db2.size(), 1);
  auto result_db2 = *privileges_db2.begin();
  ASSERT_EQ(result_db2.size(), 3);
  ASSERT_TRUE(result_db2[0].IsString());
  ASSERT_EQ(result_db2[0].ValueString(), "CREATE");
  ASSERT_TRUE(result_db2[1].IsString());
  ASSERT_EQ(result_db2[1].ValueString(), "GRANT");
  ASSERT_TRUE(result_db2[2].IsString());
  ASSERT_EQ(result_db2[2].ValueString(), "GRANTED TO ROLE");

  // Test filtering by db3 - should show no privileges
  auto privileges_db3 = auth_handler.GetPrivileges("role1", "db3");
  ASSERT_EQ(privileges_db3.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenFilteringByDatabaseWithNoAccessThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Test filtering by db2 - role doesn't have access to db2
  auto privileges = auth_handler.GetPrivileges("test_role", "db2");
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenFilteringByDatabaseWithDeniedAccessThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  role.db_access().Deny("db2");
  auth.value()->SaveRole(role);

  // Test filtering by db1 - should show privileges
  {
    auto privileges = auth_handler.GetPrivileges("test_role", "db1");
    ASSERT_EQ(privileges.size(), 1);
  }

  // Test filtering by db2 - should show no privileges due to deny
  {
    auto privileges = auth_handler.GetPrivileges("test_role", "db2");
    ASSERT_EQ(privileges.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenFilteringByDatabaseWithAllowAllThenPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  role_perms.Deny(memgraph::auth::Permission::DELETE);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().GrantAll();
  role.db_access().Deny("db3");
  auth.value()->SaveRole(role);

  // Test filtering by any database - should show privileges
  {
    auto privileges = auth_handler.GetPrivileges("test_role", "db1");
    ASSERT_EQ(privileges.size(), 2);
    {
      auto &result = privileges[0];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "DELETE");
      ASSERT_TRUE(result[1].IsString());
      ASSERT_EQ(result[1].ValueString(), "DENY");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "DENIED TO ROLE");
    }
    {
      auto &result = privileges[1];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "MATCH");
      ASSERT_TRUE(result[1].IsString());
      ASSERT_EQ(result[1].ValueString(), "GRANT");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "GRANTED TO ROLE");
    }
  }

  // Test filtering by denied database - should show no privileges
  {
    auto privileges = auth_handler.GetPrivileges("test_role", std::optional<std::string>{std::string{"db3"}});
    ASSERT_EQ(privileges.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture,
       GivenRoleWithFineGrainedPermissionsWhenFilteringByDatabaseThenOnlyRelevantPrivilegesAreReturned) {
  // Create role with fine-grained permissions
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");

  // Add fine-grained permissions
  role.fine_grained_access_handler().label_permissions().Grant({"Person"}, memgraph::auth::kAllPermissions);
  role.fine_grained_access_handler().label_permissions().Grant({"Company"},
                                                               memgraph::auth::FineGrainedPermission::READ);
  role.fine_grained_access_handler().edge_type_permissions().Grant({"WORKS_FOR"},
                                                                   memgraph::auth::FineGrainedPermission::UPDATE);

  auth.value()->SaveRole(role);

  // Test filtering by db1 - should show both generic and fine-grained privileges
  auto privileges_db1 = auth_handler.GetPrivileges("test_role", "db1");
  ASSERT_GT(privileges_db1.size(), 0);

  // Check that we have the generic MATCH privilege
  bool found_match = false;
  for (const auto &privilege : privileges_db1) {
    if (privilege[0].ValueString() == "MATCH") {
      found_match = true;
      ASSERT_EQ(privilege[1].ValueString(), "GRANT");
      ASSERT_EQ(privilege[2].ValueString(), "GRANTED TO ROLE");
      break;
    }
  }
  ASSERT_TRUE(found_match);

  // Check that we have fine-grained privileges
  bool found_label_person = false;
  bool found_label_company = false;
  bool found_edge_works_for = false;

  for (const auto &privilege : privileges_db1) {
    const auto &privilege_name = privilege[0].ValueString();
    if (privilege_name == "CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS :Person MATCHING ANY") {
      found_label_person = true;
      ASSERT_EQ(privilege[1].ValueString(), "GRANT");
      ASSERT_EQ(privilege[2].ValueString(), "LABEL PERMISSION GRANTED TO ROLE");
    } else if (privilege_name == "READ ON NODES CONTAINING LABELS :Company MATCHING ANY") {
      found_label_company = true;
      ASSERT_EQ(privilege[1].ValueString(), "GRANT");
      ASSERT_EQ(privilege[2].ValueString(), "LABEL PERMISSION GRANTED TO ROLE");
    } else if (privilege_name == "UPDATE ON EDGES CONTAINING TYPES :WORKS_FOR") {
      found_edge_works_for = true;
      ASSERT_EQ(privilege[1].ValueString(), "GRANT");
      ASSERT_EQ(privilege[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO ROLE");
    }
  }

  ASSERT_TRUE(found_label_person);
  ASSERT_TRUE(found_label_company);
  ASSERT_TRUE(found_edge_works_for);

  // Test filtering by db3 - should show no privileges
  auto privileges_db3 = auth_handler.GetPrivileges("test_role", "db3");
  ASSERT_EQ(privileges_db3.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWithoutDatabaseAccessWhenFilteringByDatabaseThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  role_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  // No database access granted
  auth.value()->SaveRole(role);

  // Test filtering by any database - should show no privileges
  auto privileges = auth_handler.GetPrivileges("test_role", "db1");
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture,
       GivenRoleWithMultiplePermissionsWhenFilteringByDatabaseThenAllRelevantPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  role_perms.Grant(memgraph::auth::Permission::CREATE);
  role_perms.Grant(memgraph::auth::Permission::DELETE);
  role_perms.Deny(memgraph::auth::Permission::MERGE);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Test filtering by db1 - should show all privileges
  auto privileges = auth_handler.GetPrivileges("test_role", "db1");
  ASSERT_EQ(privileges.size(), 4);

  // Check that all expected privileges are present
  std::set<std::string> expected_privileges = {"MATCH", "CREATE", "DELETE", "MERGE"};
  std::set<std::string> found_privileges;

  for (const auto &privilege : privileges) {
    found_privileges.emplace(privilege[0].ValueString());
  }

  ASSERT_EQ(found_privileges, expected_privileges);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWithNoPermissionsWhenFilteringByDatabaseThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  // No permissions granted
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Test filtering by db1 - should show no privileges
  auto privileges = auth_handler.GetPrivileges("test_role", "db1");
  ASSERT_EQ(privileges.size(), 0);
}
#endif

#ifdef MG_ENTERPRISE
TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_Success) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");
  auth.value()->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Set multi-tenant role for specific database
  user.AddMultiTenantRole(role, "db1");
  auth.value()->SaveUser(user);

  // Verify the role is assigned to the user for db1
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto multi_tenant_roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(multi_tenant_roles.size(), 1);
  ASSERT_EQ(multi_tenant_roles.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_MultipleDatabases) {
  // Create a role with access to multiple databases
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");
  auth.value()->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Set multi-tenant role for multiple databases
  user.AddMultiTenantRole(role, "db1");
  user.AddMultiTenantRole(role, "db2");
  auth.value()->SaveUser(user);

  // Verify the role is assigned to the user for both databases
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles_db1 = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles_db1.size(), 1);
  ASSERT_EQ(roles_db1.begin()->rolename(), "test_role");

  auto roles_db2 = updated_user->GetMultiTenantRoles("db2");
  ASSERT_EQ(roles_db2.size(), 1);
  ASSERT_EQ(roles_db2.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_RoleWithoutDatabaseAccess_Throws) {
  // Create a role without database access
  memgraph::auth::Role role("test_role");
  auth.value()->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Try to set multi-tenant role for a database the role doesn't have access to
  ASSERT_THROW(user.AddMultiTenantRole(role, "db1"), memgraph::auth::AuthException);
}

TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_GlobalRoleAlreadySet_Throws) {
  // Create a role
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Create a user and set the role globally
  memgraph::auth::User user("test_user");
  user.ClearAllRoles();
  user.AddRole(role);
  auth.value()->SaveUser(user);

  // Try to set the same role as multi-tenant role
  ASSERT_THROW(user.AddMultiTenantRole(role, "db1"), memgraph::auth::AuthException);
}

TEST_F(AuthQueryHandlerFixture, ClearMultiTenantRoles_Success) {
  // Create a role with access to multiple databases
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");
  auth.value()->SaveRole(role);

  // Create a user and set multi-tenant roles
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  user.AddMultiTenantRole(role, "db2");
  auth.value()->SaveUser(user);

  // Clear multi-tenant roles for db1
  user.ClearMultiTenantRoles("db1");
  auth.value()->SaveUser(user);

  // Verify roles are cleared for db1 but remain for db2
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles_db1 = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles_db1.size(), 0);

  auto roles_db2 = updated_user->GetMultiTenantRoles("db2");
  ASSERT_EQ(roles_db2.size(), 1);
  ASSERT_EQ(roles_db2.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, ClearMultiTenantRoles_NonExistentDatabase) {
  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Clear multi-tenant roles for non-existent database (should not throw)
  ASSERT_NO_THROW(user.ClearMultiTenantRoles("non_existent_db"));
}

TEST_F(AuthQueryHandlerFixture, GetMultiTenantRoles_Empty) {
  // Create a user without any multi-tenant roles
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, SetRole_WithDatabaseSpecification_Success) {
  // Create roles with database access
  memgraph::auth::Role role1("role1");
  role1.db_access().Grant("db1");
  auth.value()->SaveRole(role1);

  memgraph::auth::Role role2("role2");
  role2.db_access().Grant("db2");
  auth.value()->SaveRole(role2);

  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Set roles with database specification
  {
    std::vector<std::string> roles = {"role1", "role2"};
    std::unordered_set<std::string> databases = {"db1", "db2"};
    ASSERT_THROW(auth_handler.SetRoles("test_user", roles, databases, nullptr), memgraph::query::QueryRuntimeException);
  }

  // Update roles to have access to both databases
  role1.db_access().Grant("db2");
  auth.value()->SaveRole(role1);
  role2.db_access().Grant("db1");
  auth.value()->SaveRole(role2);
  auth.value()->SaveUser(user);

  // Set roles with database specification
  {
    std::vector<std::string> roles = {"role1", "role2"};
    std::unordered_set<std::string> databases = {"db1", "db2"};
    ASSERT_NO_THROW(auth_handler.SetRoles("test_user", roles, databases, nullptr));
  }

  // Verify the roles are set as multi-tenant roles
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles_db1 = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles_db1.size(), 2);
  ASSERT_TRUE(std::find_if(roles_db1.begin(), roles_db1.end(),
                           [](const auto &role) { return role.rolename() == "role1"; }) != roles_db1.end());
  ASSERT_TRUE(std::find_if(roles_db1.begin(), roles_db1.end(),
                           [](const auto &role) { return role.rolename() == "role2"; }) != roles_db1.end());

  auto roles_db2 = updated_user->GetMultiTenantRoles("db2");
  ASSERT_EQ(roles_db2.size(), 2);
  ASSERT_TRUE(std::find_if(roles_db2.begin(), roles_db2.end(),
                           [](const auto &role) { return role.rolename() == "role1"; }) != roles_db2.end());
  ASSERT_TRUE(std::find_if(roles_db2.begin(), roles_db2.end(),
                           [](const auto &role) { return role.rolename() == "role2"; }) != roles_db2.end());
}

TEST_F(AuthQueryHandlerFixture, ClearRole_WithDatabaseSpecification_Success) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");
  auth.value()->SaveRole(role);

  // Create a user and set multi-tenant roles
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  user.AddMultiTenantRole(role, "db2");
  auth.value()->SaveUser(user);

  // Clear roles for specific database
  std::unordered_set<std::string> databases = {"db1"};
  ASSERT_NO_THROW(auth_handler.ClearRoles("test_user", databases, nullptr));

  // Verify roles are cleared for db1 but remain for db2
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles_db1 = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles_db1.size(), 0);

  auto roles_db2 = updated_user->GetMultiTenantRoles("db2");
  ASSERT_EQ(roles_db2.size(), 1);
  ASSERT_EQ(roles_db2.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, GetRolenameForUser_WithDatabaseSpecification) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Create a user and set multi-tenant role
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  auth.value()->SaveUser(user);

  // Get roles for specific database
  auto rolenames = auth_handler.GetRolenamesForUser("test_user", "db1");
  ASSERT_EQ(rolenames.size(), 1);
  ASSERT_EQ(rolenames[0], "test_role");

  // Get roles for different database (should be empty)
  auto rolenames_db2 = auth_handler.GetRolenamesForUser("test_user", "db2");
  ASSERT_EQ(rolenames_db2.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, LinkUser_MultiTenantLinks) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Manually add multi-tenant link to storage
  nlohmann::json mt_link_data = nlohmann::json::array();
  mt_link_data.push_back({{"rolename", "test_role"}});

  // This would normally be done through the storage interface
  // For testing, we'll simulate the storage update
  user.AddMultiTenantRole(role, "db1");
  auth.value()->SaveUser(user);

  // Verify the user has the role linked
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 1);
  ASSERT_EQ(roles.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, RemoveUser_ClearsMultiTenantLinks) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Create a user and set multi-tenant role
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  auth.value()->SaveUser(user);

  // Verify user exists with multi-tenant role
  auto existing_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(existing_user);
  ASSERT_EQ(existing_user->GetMultiTenantRoles("db1").size(), 1);

  // Remove the user
  ASSERT_TRUE(auth.value()->RemoveUser("test_user", nullptr));

  // Verify user is removed
  auto removed_user = auth.value()->GetUser("test_user");
  ASSERT_FALSE(removed_user);
}

TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_AlreadySet_NoChange) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth.value()->SaveUser(user);

  // Set multi-tenant role
  user.AddMultiTenantRole(role, "db1");
  auth.value()->SaveUser(user);

  // Set the same role again (should not change anything)
  ASSERT_NO_THROW(user.AddMultiTenantRole(role, "db1"));
  auth.value()->SaveUser(user);

  // Verify the role is still set correctly
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 1);
  ASSERT_EQ(roles.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, ClearRole_ClearsMultiTenantRoles) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth.value()->SaveRole(role);

  // Create a user and set multi-tenant role
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  auth.value()->SaveUser(user);

  // Clear all roles (should clear multi-tenant roles too)
  user.ClearAllRoles();
  auth.value()->SaveUser(user);

  // Verify all roles are cleared
  auto updated_user = auth.value()->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 0);
  ASSERT_TRUE(updated_user->roles().empty());
}
#endif

TEST_F(AuthQueryHandlerFixture, SetRole_MultipleRoles_Success) {
  // Create roles
  auto role1 = auth.value()->AddRole("role1");
  auto role2 = auth.value()->AddRole("role2");
  auto role3 = auth.value()->AddRole("role3");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  ASSERT_TRUE(role3);

  // Create user
  auto user = auth.value()->AddUser("multiuser");
  ASSERT_TRUE(user);

  // Set multiple roles
  std::vector<std::string> roles = {"role1", "role2", "role3"};
  ASSERT_NO_THROW(auth_handler.SetRoles("multiuser", roles, std::unordered_set<std::string>{}, nullptr));

  // Check user roles
  auto updated_user = auth.value()->GetUser("multiuser");
  ASSERT_TRUE(updated_user);
  std::vector<std::string> assigned_roles;
  for (const auto &role : updated_user->roles()) {
    assigned_roles.push_back(role.rolename());
  }
  std::sort(assigned_roles.begin(), assigned_roles.end());
  std::vector<std::string> expected_roles = {"role1", "role2", "role3"};
  std::sort(expected_roles.begin(), expected_roles.end());
  ASSERT_EQ(assigned_roles, expected_roles);
}

TEST_F(AuthQueryHandlerFixture, SetRole_EmptyRoles_ClearsRoles) {
  // Create role and user
  auto role = auth.value()->AddRole("role1");
  ASSERT_TRUE(role);
  auto user = auth.value()->AddUser("user1");
  ASSERT_TRUE(user);
  user->AddRole(*role);
  auth.value()->SaveUser(*user);

  // Clear roles by setting empty vector
  std::vector<std::string> roles = {};
  ASSERT_NO_THROW(auth_handler.SetRoles("user1", roles, std::unordered_set<std::string>{}, nullptr));
  auto updated_user = auth.value()->GetUser("user1");
  ASSERT_TRUE(updated_user);
  ASSERT_TRUE(updated_user->roles().empty());
}

TEST_F(AuthQueryHandlerFixture, SetRole_NonExistentRole_Throws) {
  // Create user
  auto user = auth.value()->AddUser("user2");
  ASSERT_TRUE(user);
  // Try to set a non-existent role
  std::vector<std::string> roles = {"doesnotexist"};
  ASSERT_THROW(auth_handler.SetRoles("user2", roles, std::unordered_set<std::string>{}, nullptr),
               memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, SetRole_UserDoesNotExist_Throws) {
  std::vector<std::string> roles = {"role1"};
  ASSERT_THROW(auth_handler.SetRoles("no_such_user", roles, std::unordered_set<std::string>{}, nullptr),
               memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, SetRole_DuplicateRoles_NoDuplicatesInResult) {
  // Create roles and user
  auto role1 = auth.value()->AddRole("role1");
  auto role2 = auth.value()->AddRole("role2");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  auto user = auth.value()->AddUser("user3");
  ASSERT_TRUE(user);
  // Set duplicate roles
  std::vector<std::string> roles = {"role1", "role2", "role1", "role2"};
  ASSERT_NO_THROW(auth_handler.SetRoles("user3", roles, std::unordered_set<std::string>{}, nullptr));
  auto updated_user = auth.value()->GetUser("user3");
  ASSERT_TRUE(updated_user);
  std::set<std::string> unique_roles;
  for (const auto &role : updated_user->roles()) {
    unique_roles.insert(role.rolename());
  }
  ASSERT_EQ(unique_roles.size(), 2);
  ASSERT_TRUE(unique_roles.count("role1"));
  ASSERT_TRUE(unique_roles.count("role2"));
}

#ifdef MG_ENTERPRISE
// Concurrent Access Tests
TEST_F(AuthQueryHandlerFixture, ConcurrentProfileCreation) {
  constexpr size_t kNumThreads = 10;
  constexpr size_t kNumProfiles = 100;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};

  // Test concurrent profile creation
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      for (size_t j = 0; j < kNumProfiles; ++j) {
        std::string profile_name = fmt::format("profile_{}_{}", i, j);
        try {
          auth_handler.CreateProfile(profile_name, {}, {}, nullptr);
          success_count.fetch_add(1);
        } catch (const memgraph::query::QueryRuntimeException &) {
          failure_count.fetch_add(1);
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // All profiles should be created successfully (no duplicates)
  ASSERT_EQ(success_count.load(), kNumThreads * kNumProfiles);
  ASSERT_EQ(failure_count.load(), 0);

  // Verify all profiles exist
  auto all_profiles = auth_handler.AllProfiles();
  ASSERT_EQ(all_profiles.size(), kNumThreads * kNumProfiles);
}

TEST_F(AuthQueryHandlerFixture, ConcurrentProfileUpdates) {
  // Create initial profile
  ASSERT_NO_THROW(auth_handler.CreateProfile("test_profile", {}, {}, nullptr));

  constexpr size_t kNumThreads = 5;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};

  // Test concurrent profile updates
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      try {
        auto limit = memgraph::query::UserProfileQuery::LimitValueResult{};
        limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
        limit.quantity.value = i + 1;

        auth_handler.UpdateProfile(
            "test_profile",
            {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], limit}}, nullptr);
        success_count.fetch_add(1);
      } catch (const memgraph::query::QueryRuntimeException &) {
        failure_count.fetch_add(1);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // At least one update should succeed
  ASSERT_GT(success_count.load(), 0);

  // Verify profile was updated
  auto profile = auth_handler.GetProfile("test_profile");
  ASSERT_FALSE(profile.empty());
}

TEST_F(AuthQueryHandlerFixture, ConcurrentProfileAssignment) {
  // Create profiles and users
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile1", {}, {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile2", {}, {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user1", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user2", {}, nullptr));

  constexpr size_t kNumThreads = 4;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};

  // Test concurrent profile assignments
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      std::string profile_name = (i % 2 == 0) ? "profile1" : "profile2";
      std::string user_name = (i % 2 == 0) ? "user1" : "user2";

      try {
        auth_handler.SetProfile(profile_name, user_name, nullptr);
        success_count.fetch_add(1);
      } catch (const memgraph::query::QueryRuntimeException &) {
        failure_count.fetch_add(1);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // All assignments should succeed
  ASSERT_EQ(success_count.load(), kNumThreads);
  ASSERT_EQ(failure_count.load(), 0);

  // Verify assignments
  auto user1_profile = auth_handler.GetProfileForUser("user1");
  auto user2_profile = auth_handler.GetProfileForUser("user2");
  ASSERT_TRUE(user1_profile.has_value());
  ASSERT_TRUE(user2_profile.has_value());
}

TEST_F(AuthQueryHandlerFixture, ConcurrentProfileDeletion) {
  // Create multiple profiles
  constexpr size_t kNumProfiles = 50;
  for (size_t i = 0; i < kNumProfiles; ++i) {
    ASSERT_NO_THROW(auth_handler.CreateProfile(fmt::format("profile_{}", i), {}, {}, nullptr));
  }

  constexpr size_t kNumThreads = 5;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};

  // Test concurrent profile deletion
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      for (size_t j = 0; j < kNumProfiles / kNumThreads; ++j) {
        size_t profile_idx = i * (kNumProfiles / kNumThreads) + j;
        try {
          auth_handler.DropProfile(fmt::format("profile_{}", profile_idx), nullptr);
          success_count.fetch_add(1);
        } catch (const memgraph::query::QueryRuntimeException &) {
          failure_count.fetch_add(1);
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // All deletions should succeed
  ASSERT_EQ(success_count.load(), kNumProfiles);
  ASSERT_EQ(failure_count.load(), 0);

  // Verify all profiles were deleted
  auto all_profiles = auth_handler.AllProfiles();
  ASSERT_EQ(all_profiles.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, ConcurrentResourceAccess) {
  // Create profile with limits
  auto limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  limit.quantity.value = 5;  // 5 sessions limit

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "limited_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], limit}},
      {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("limited_profile", "test_user", nullptr));

  constexpr size_t kNumThreads = 10;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};

  // Test concurrent resource access
  std::latch latch(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      auto resource = resources.GetUser("test_user");
      if (resource->IncrementSessions()) {
        success_count.fetch_add(1);
        // Simulate some work
        latch.arrive_and_wait();
        resource->DecrementSessions();
      } else {
        latch.arrive_and_wait();
        failure_count.fetch_add(1);
      }
    });
  }

  latch.wait();

  for (auto &thread : threads) {
    thread.join();
  }

  // Should have exactly 5 successful increments (due to limit)
  ASSERT_EQ(success_count.load(), 5);
  ASSERT_EQ(failure_count.load(), 5);
}

// Resource Exhaustion Tests
TEST_F(AuthQueryHandlerFixture, SessionLimitExhaustion) {
  // Create profile with very low session limit
  auto limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  limit.quantity.value = 1;  // Only 1 session allowed

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "single_session_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], limit}}, {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("single_session_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // First session should succeed
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_EQ(resource->GetSessions().first, 1);
  ASSERT_EQ(resource->GetSessions().second, 1);

  // Second session should fail
  ASSERT_FALSE(resource->IncrementSessions());
  ASSERT_EQ(resource->GetSessions().first, 1);  // Should remain at 1
  ASSERT_EQ(resource->GetSessions().second, 1);

  // After releasing the session, should be able to create another
  resource->DecrementSessions();
  ASSERT_EQ(resource->GetSessions().first, 0);
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_EQ(resource->GetSessions().first, 1);
}

TEST_F(AuthQueryHandlerFixture, MemoryLimitExhaustion) {
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
  // Create profile with memory limit
  auto limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
  limit.mem_limit.value = 1024;  // 1KB limit
  limit.mem_limit.scale = 1;     // 1 byte units

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "memory_limited_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], limit}}, {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("memory_limited_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // Allocate within limit
  ASSERT_TRUE(resource->IncrementTransactionsMemory(512));
  ASSERT_EQ(resource->GetTransactionsMemory().first, 512);
  ASSERT_EQ(resource->GetTransactionsMemory().second, 1024);

  // Allocate more within limit
  ASSERT_TRUE(resource->IncrementTransactionsMemory(256));
  ASSERT_EQ(resource->GetTransactionsMemory().first, 768);
  ASSERT_EQ(resource->GetTransactionsMemory().second, 1024);

  // Try to exceed limit
  ASSERT_FALSE(resource->IncrementTransactionsMemory(300));  // Would exceed 1024
  ASSERT_EQ(resource->GetTransactionsMemory().first, 768);   // Should remain unchanged
  ASSERT_EQ(resource->GetTransactionsMemory().second, 1024);

  // Deallocate and try again
  resource->DecrementTransactionsMemory(256);
  ASSERT_EQ(resource->GetTransactionsMemory().first, 512);
  ASSERT_TRUE(resource->IncrementTransactionsMemory(300));  // Should now fit
  ASSERT_EQ(resource->GetTransactionsMemory().first, 812);
}

TEST_F(AuthQueryHandlerFixture, MemoryLimitExhaustionWithLargeAllocation) {
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
  // Create profile with moderate memory limit
  auto limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
  limit.mem_limit.value = 2048;  // 2KB limit
  limit.mem_limit.scale = 1;

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "moderate_memory_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], limit}}, {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("moderate_memory_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // Try to allocate more than the limit in one go
  ASSERT_FALSE(resource->IncrementTransactionsMemory(4096));  // 4KB > 2KB limit
  ASSERT_EQ(resource->GetTransactionsMemory().first, 0);      // Should remain at 0
  ASSERT_EQ(resource->GetTransactionsMemory().second, 2048);

  // Verify we can still allocate within limits
  ASSERT_TRUE(resource->IncrementTransactionsMemory(1024));
  ASSERT_EQ(resource->GetTransactionsMemory().first, 1024);
}

TEST_F(AuthQueryHandlerFixture, MemoryLimitExhaustionWithLargeAllocationAndNoThrow) {
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
  // Create profile with moderate memory limit
  auto limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
  limit.mem_limit.value = 2048;  // 2KB limit
  limit.mem_limit.scale = 1;

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "moderate_memory_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], limit}}, {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("moderate_memory_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // Try to allocate more than the limit in one go
  ASSERT_FALSE(resource->IncrementTransactionsMemory(4096));  // 4KB > 2KB limit
  ASSERT_EQ(resource->GetTransactionsMemory().first, 0);      // Should remain at 0
  ASSERT_EQ(resource->GetTransactionsMemory().second, 2048);

  {
    memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
    ASSERT_TRUE(resource->IncrementTransactionsMemory(4096));  // 4KB > 2KB limit
    ASSERT_EQ(resource->GetTransactionsMemory().first, 4096);
    ASSERT_EQ(resource->GetTransactionsMemory().second, 2048);
  }
}

TEST_F(AuthQueryHandlerFixture, ResourceExhaustionRecovery) {
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
  // Create profile with limits
  auto session_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  session_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  session_limit.quantity.value = 2;

  auto memory_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  memory_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
  memory_limit.mem_limit.value = 1024;
  memory_limit.mem_limit.scale = 1;

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "recovery_test_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], session_limit},
       memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], memory_limit}},
      {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("recovery_test_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // Exhaust both resources
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_FALSE(resource->IncrementSessions());  // Should fail

  ASSERT_TRUE(resource->IncrementTransactionsMemory(1024));
  ASSERT_FALSE(resource->IncrementTransactionsMemory(1));  // Should fail

  // Verify current state
  ASSERT_EQ(resource->GetSessions().first, 2);
  ASSERT_EQ(resource->GetSessions().second, 2);
  ASSERT_EQ(resource->GetTransactionsMemory().first, 1024);
  ASSERT_EQ(resource->GetTransactionsMemory().second, 1024);

  // Release resources
  resource->DecrementSessions();
  resource->DecrementSessions();
  resource->DecrementTransactionsMemory(1024);

  // Verify recovery
  ASSERT_EQ(resource->GetSessions().first, 0);
  ASSERT_EQ(resource->GetSessions().second, 2);
  ASSERT_EQ(resource->GetTransactionsMemory().first, 0);
  ASSERT_EQ(resource->GetTransactionsMemory().second, 1024);

  // Should be able to allocate again
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_TRUE(resource->IncrementTransactionsMemory(1024));
}

TEST_F(AuthQueryHandlerFixture, ProfileUpdateDuringResourceExhaustion) {
  // Create profile with low limits
  auto session_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  session_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  session_limit.quantity.value = 1;

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "update_test_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], session_limit}}, {},
      nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("update_test_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // Exhaust the resource
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_FALSE(resource->IncrementSessions());  // Should fail

  // Update profile to increase limits
  auto new_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  new_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  new_limit.quantity.value = 3;  // Increase to 3 sessions

  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "update_test_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], new_limit}}, nullptr));

  // Should now be able to allocate more sessions
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_FALSE(resource->IncrementSessions());  // Should fail at 3

  ASSERT_EQ(resource->GetSessions().first, 3);
  ASSERT_EQ(resource->GetSessions().second, 3);
}

TEST_F(AuthQueryHandlerFixture, ProfileDeletionDuringResourceUsage) {
  // Create profile and use resources
  auto session_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  session_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  session_limit.quantity.value = 2;

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "delete_test_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], session_limit}}, {},
      nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("delete_test_profile", "test_user", nullptr));

  auto resource = resources.GetUser("test_user");

  // Use some resources
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_EQ(resource->GetSessions().first, 1);
  ASSERT_EQ(resource->GetSessions().second, 2);

  // Delete the profile
  ASSERT_NO_THROW(auth_handler.DropProfile("delete_test_profile", nullptr));

  // Resources should be reset to unlimited
  ASSERT_EQ(resource->GetSessions().first, 1);                                    // Current usage remains
  ASSERT_EQ(resource->GetSessions().second, std::numeric_limits<size_t>::max());  // Unlimited

  // Should be able to allocate unlimited sessions now
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_TRUE(resource->IncrementSessions());
  ASSERT_EQ(resource->GetSessions().first, 4);
}

TEST_F(AuthQueryHandlerFixture, ConcurrentResourceExhaustion) {
  // Create profile with low limits
  auto session_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  session_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
  session_limit.quantity.value = 3;  // Only 3 sessions allowed

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "concurrent_exhaustion_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], session_limit}}, {},
      nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("concurrent_exhaustion_profile", "test_user", nullptr));

  constexpr size_t kNumThreads = 10;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};
  std::atomic<size_t> total_allocated{0};

  // Test concurrent resource exhaustion
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      auto resource = resources.GetUser("test_user");
      if (resource->IncrementSessions()) {
        success_count.fetch_add(1);
        total_allocated.fetch_add(1);
        // Hold the resource for a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        resource->DecrementSessions();
        total_allocated.fetch_sub(1);
      } else {
        failure_count.fetch_add(1);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Should have exactly 3 successful allocations at any time
  ASSERT_EQ(success_count.load(), 3);
  ASSERT_EQ(failure_count.load(), 7);
  ASSERT_EQ(total_allocated.load(), 0);  // All resources should be released
}

TEST_F(AuthQueryHandlerFixture, MemoryExhaustionUnderLoad) {
  // Create profile with memory limit
  auto memory_limit = memgraph::query::UserProfileQuery::LimitValueResult{};
  memory_limit.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
  memory_limit.mem_limit.value = 1024;  // 1KB limit
  memory_limit.mem_limit.scale = 1;

  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "memory_load_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], memory_limit}}, {},
      nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("test_user", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("memory_load_profile", "test_user", nullptr));

  constexpr size_t kNumThreads = 8;
  std::vector<std::jthread> threads;
  std::atomic<size_t> success_count{0};
  std::atomic<size_t> failure_count{0};

  // Test concurrent memory allocation under load
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
      auto resource = resources.GetUser("test_user");
      ASSERT_TRUE(resource);
      size_t allocation_size = 256;  // 256 bytes per thread

      if (resource->IncrementTransactionsMemory(allocation_size)) {
        success_count.fetch_add(1);
      } else {
        failure_count.fetch_add(1);
      }
    });
  }

  threads.clear();

  // Should have exactly 4 successful allocations (1024/256 = 4)
  ASSERT_EQ(success_count.load(), 4);
  ASSERT_EQ(failure_count.load(), 4);
}
#endif
