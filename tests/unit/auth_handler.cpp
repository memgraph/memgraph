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
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER");
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

  auto privileges = auth_handler.GetPrivileges(user_name);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "ALL LABELS");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "UPDATE");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "GLOBAL LABEL PERMISSION GRANTED TO USER");
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

  auto privileges = auth_handler.GetPrivileges(user_name);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "READ");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "LABEL PERMISSION DENIED TO USER");
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

  auto privileges = auth_handler.GetPrivileges(user_name);
  ASSERT_EQ(privileges.size(), 1);

  auto result = *privileges.begin();
  ASSERT_EQ(result.size(), 3);

  ASSERT_TRUE(result[0].IsString());
  ASSERT_EQ(result[0].ValueString(), "EDGE_TYPE :EdgeType1");

  ASSERT_TRUE(result[1].IsString());
  ASSERT_EQ(result[1].ValueString(), "NOTHING");

  ASSERT_TRUE(result[2].IsString());
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION DENIED TO USER");
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
  ASSERT_EQ(result[2].ValueString(), "EDGE_TYPE PERMISSION GRANTED TO USER");
}
#endif

// Multi-tenant tests for database filtering
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
