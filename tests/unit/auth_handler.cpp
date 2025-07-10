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

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "dbms/constants.hpp"
#include "exceptions.hpp"
#include "glue/auth_global.hpp"
#include "glue/auth_handler.hpp"
#include "query/typed_value.hpp"
#include "utils/file.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

class AuthQueryHandlerFixture : public testing::Test {
 protected:
  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_auth_handler"};
  memgraph::auth::SynchedAuth auth{
      test_folder_ / ("unit_auth_handler_test_" + std::to_string(static_cast<int>(getpid()))),
      memgraph::auth::Auth::Config{/* default */}};
  memgraph::glue::AuthQueryHandler auth_handler{&auth};

  std::string user_name = "Mate";
  std::string edge_type_repr = "EdgeType1";
  std::string label_repr = "Label1";
  memgraph::auth::Permissions perms{};
#ifdef MG_ENTERPRISE
  memgraph::auth::FineGrainedAccessHandler handler{};
#endif
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
  auth->SaveUser(user);

  { ASSERT_EQ(auth_handler.GetUsernames().size(), 1); }

  {
    auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);

    ASSERT_EQ(privileges.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenAddedGrantPermissionThenItIsReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
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
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
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
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenPrivilegeGrantedThenItIsReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth->SaveRole(role);

  { ASSERT_EQ(auth_handler.GetRolenames().size(), 1); }

  {
    auto privileges = auth_handler.GetPrivileges("Mates_role", memgraph::dbms::kDefaultDB);
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
  auth->SaveRole(role);

  auto privileges = auth_handler.GetPrivileges("Mates_role", memgraph::dbms::kDefaultDB);
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
  auth->SaveRole(role);

  auto privileges = auth_handler.GetPrivileges("Mates_role", memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedTwoPrivilegesThenBothAreReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 2);
}

TEST_F(AuthQueryHandlerFixture, GivenUserAndRoleWhenOneGrantedAndOtherGrantedThenBothArePrinted) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth->SaveRole(role);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.SetRole(role);
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
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
  auth->SaveRole(role);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.SetRole(role);
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
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
  auth->SaveRole(role);

  memgraph::auth::Permissions user_perms{};
  user_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{
      user_name,
      std::nullopt,
      user_perms,
  };
  user.SetRole(role);
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
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
  auth->SaveRole(role);

  memgraph::auth::Permissions user_perms{};
  user_perms.Deny(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, user_perms};
  user.SetRole(role);
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
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
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "LABEL :Label1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "READ");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedMultiplePrivilegesOnLabelThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "LABEL :Label1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "UPDATE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAllPrivilegesOnLabelThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::UPDATE);
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "LABEL :Label1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "CREATE_DELETE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalPrivilegeOnLabelThenIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "READ");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalMultiplePrivilegesOnLabelThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "UPDATE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalAllPrivilegesOnLabelThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "CREATE_DELETE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

// EDGE_TYPES
TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedPrivilegeOnEdgeTypeThenIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "READ");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedMultiplePrivilegesOnEdgeTypeThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "UPDATE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAllPrivilegesOnEdgeTypeThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::UPDATE);
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "CREATE_DELETE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalPrivilegeOnEdgeTypeThenIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL EDGE_TYPES");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "READ");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalMultiplePrivilegesOnEdgeTypeThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},

  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL EDGE_TYPES");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "UPDATE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedGlobalAllPrivilegesOnEdgeTypeThenTopOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  read_permission.Grant("*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL EDGE_TYPES");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "CREATE_DELETE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAndDeniedOnLabelThenNoPermission) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::NOTHING);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
      memgraph::auth::FineGrainedAccessPermissions{},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "LABEL :Label1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "NOTHING");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION DENIED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedAndDeniedOnEdgeTypeThenNoPermission) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::NOTHING);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "NOTHING");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION DENIED TO USER FOR DATABASE memgraph");
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedReadAndDeniedUpdateThenOneIsDisplayed) {
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::READ);
  read_permission.Grant(edge_type_repr, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{},
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
  };

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms, handler};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "READ");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER FOR DATABASE memgraph");
}
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
  auth->SaveRole(role1);

  memgraph::auth::Permissions role2_perms{};
  role2_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role2 = memgraph::auth::Role{"role2", role2_perms};
  role2.db_access().Grant("db2");
  auth->SaveRole(role2);

  // Create user with both roles
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role1);
  user.AddRole(role2);
  auth->SaveUser(user);

  // Test filtering by db1 - should only show MATCH permission from role1
  auto privileges_db1 = auth_handler.GetPrivileges(user_name, "db1");
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
  auth->SaveRole(role);

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role);
  auth->SaveUser(user);

  // Test filtering by db2 - role doesn't have access to db2
  auto privileges = auth_handler.GetPrivileges(user_name, "db2");
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWithMultipleRolesWhenFilteringByDefaultDatabaseThenAllPrivilegesAreReturned) {
  memgraph::auth::Permissions role1_perms{};
  role1_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role1 = memgraph::auth::Role{"role1", role1_perms};
  auth->SaveRole(role1);

  memgraph::auth::Permissions role2_perms{};
  role2_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role2 = memgraph::auth::Role{"role2", role2_perms};
  auth->SaveRole(role2);

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role1);
  user.AddRole(role2);
  auth->SaveUser(user);

  // Test with empty database name
  {
    auto privileges = auth_handler.GetPrivileges(user_name);
    ASSERT_EQ(privileges.size(), 0);
  }

  // Test with default database
  {
    auto privileges = auth_handler.GetPrivileges(user_name, memgraph::dbms::kDefaultDB);
    ASSERT_EQ(privileges.size(), 2);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenUserWithRoleWhenFilteringByDatabaseWithDeniedAccessThenNoPrivilegesAreReturned) {
  memgraph::auth::Permissions role_perms{};
  role_perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"test_role", role_perms};
  role.db_access().Grant("db1");
  role.db_access().Deny("db2");
  auth->SaveRole(role);

  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.AddRole(role);
  auth->SaveUser(user);
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
  auth->SaveRole(role);

  memgraph::auth::Permissions user_perms{};
  user_perms.Grant(memgraph::auth::Permission::DELETE);
  user_perms.Deny(memgraph::auth::Permission::MATCH);
  user_perms.Grant(memgraph::auth::Permission::AUTH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, user_perms};
  user.db_access().GrantAll();
  user.db_access().Deny("db3");
  user.db_access().Deny("db4");
  user.AddRole(role);
  auth->SaveUser(user);

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
    auto privileges = auth_handler.GetPrivileges(user_name, "db2");
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
    auto privileges = auth_handler.GetPrivileges(user_name, "db3");
    EXPECT_EQ(privileges.size(), 2);
    {
      auto &result = privileges[0];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "DELETE");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "DENIED TO ROLE");
    }
    {
      auto &result = privileges[1];
      ASSERT_EQ(result.size(), 3);
      ASSERT_TRUE(result[0].IsString());
      ASSERT_EQ(result[0].ValueString(), "MATCH");
      ASSERT_TRUE(result[2].IsString());
      ASSERT_EQ(result[2].ValueString(), "GRANTED TO ROLE");
    }
  }
  {
    auto privileges = auth_handler.GetPrivileges(user_name, "db4");
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
  auth->SaveRole(role1);

  memgraph::auth::Permissions role2_perms{};
  role2_perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::Role role2 = memgraph::auth::Role{"role2", role2_perms};
  role2.db_access().Grant("db2");
  auth->SaveRole(role2);

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
  auth->SaveRole(role);

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
  auth->SaveRole(role);

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
  auth->SaveRole(role);

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
    auto privileges = auth_handler.GetPrivileges("test_role", "db3");
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
  role.fine_grained_access_handler().label_permissions().Grant("Person",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  role.fine_grained_access_handler().label_permissions().Grant("Company", memgraph::auth::FineGrainedPermission::READ);
  role.fine_grained_access_handler().edge_type_permissions().Grant("WORKS_FOR",
                                                                   memgraph::auth::FineGrainedPermission::UPDATE);

  auth->SaveRole(role);

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
    if (privilege_name == "LABEL :Person") {
      found_label_person = true;
      ASSERT_EQ(privilege[1].ValueString(), "CREATE_DELETE");
      ASSERT_EQ(privilege[2].ValueString(), "LABEL PERMISSION GRANTED TO ROLE FOR DATABASE db1");
    } else if (privilege_name == "LABEL :Company") {
      found_label_company = true;
      ASSERT_EQ(privilege[1].ValueString(), "READ");
      ASSERT_EQ(privilege[2].ValueString(), "LABEL PERMISSION GRANTED TO ROLE FOR DATABASE db1");
    } else if (privilege_name == "EDGE_TYPE :WORKS_FOR") {
      found_edge_works_for = true;
      ASSERT_EQ(privilege[1].ValueString(), "UPDATE");
      ASSERT_EQ(privilege[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO ROLE FOR DATABASE db1");
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
  auth->SaveRole(role);

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
  auth->SaveRole(role);

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
  auth->SaveRole(role);

  // Test filtering by db1 - should show no privileges
  auto privileges = auth_handler.GetPrivileges("test_role", "db1");
  ASSERT_EQ(privileges.size(), 0);
}
#endif

TEST_F(AuthQueryHandlerFixture, SetRole_MultipleRoles_Success) {
  // Create roles
  auto role1 = auth->AddRole("role1");
  auto role2 = auth->AddRole("role2");
  auto role3 = auth->AddRole("role3");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  ASSERT_TRUE(role3);

  // Create user
  auto user = auth->AddUser("multiuser");
  ASSERT_TRUE(user);

  // Set multiple roles
  std::vector<std::string> roles = {"role1", "role2", "role3"};
  ASSERT_NO_THROW(auth_handler.SetRole("multiuser", roles, nullptr));

  // Check user roles
  auto updated_user = auth->GetUser("multiuser");
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
  auto role = auth->AddRole("role1");
  ASSERT_TRUE(role);
  auto user = auth->AddUser("user1");
  ASSERT_TRUE(user);
  user->AddRole(*role);
  auth->SaveUser(*user);

  // Clear roles by setting empty vector
  std::vector<std::string> roles = {};
  ASSERT_NO_THROW(auth_handler.SetRole("user1", roles, nullptr));
  auto updated_user = auth->GetUser("user1");
  ASSERT_TRUE(updated_user);
  ASSERT_TRUE(updated_user->roles().empty());
}

TEST_F(AuthQueryHandlerFixture, SetRole_NonExistentRole_Throws) {
  // Create user
  auto user = auth->AddUser("user2");
  ASSERT_TRUE(user);
  // Try to set a non-existent role
  std::vector<std::string> roles = {"doesnotexist"};
  ASSERT_THROW(auth_handler.SetRole("user2", roles, nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, SetRole_UserDoesNotExist_Throws) {
  std::vector<std::string> roles = {"role1"};
  ASSERT_THROW(auth_handler.SetRole("no_such_user", roles, nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, SetRole_DuplicateRoles_NoDuplicatesInResult) {
  // Create roles and user
  auto role1 = auth->AddRole("role1");
  auto role2 = auth->AddRole("role2");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  auto user = auth->AddUser("user3");
  ASSERT_TRUE(user);
  // Set duplicate roles
  std::vector<std::string> roles = {"role1", "role2", "role1", "role2"};
  ASSERT_NO_THROW(auth_handler.SetRole("user3", roles, nullptr));
  auto updated_user = auth->GetUser("user3");
  ASSERT_TRUE(updated_user);
  std::set<std::string> unique_roles;
  for (const auto &role : updated_user->roles()) {
    unique_roles.insert(role.rolename());
  }
  ASSERT_EQ(unique_roles.size(), 2);
  ASSERT_TRUE(unique_roles.count("role1"));
  ASSERT_TRUE(unique_roles.count("role2"));
}
