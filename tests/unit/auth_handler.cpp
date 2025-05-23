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
#include "auth/profiles/user_profiles.hpp"
#include "exceptions.hpp"
#include "frontend/ast/ast_visitor.hpp"
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
    auto privileges = auth_handler.GetPrivileges(user_name);

    ASSERT_EQ(privileges.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenAddedGrantPermissionThenItIsReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenRoleWhenPrivilegeGrantedThenItIsReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth->SaveRole(role);

  { ASSERT_EQ(auth_handler.GetRolenames().size(), 1); }

  {
    auto privileges = auth_handler.GetPrivileges("Mates_role");
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

  auto privileges = auth_handler.GetPrivileges("Mates_role");
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

  auto privileges = auth_handler.GetPrivileges("Mates_role");
  ASSERT_EQ(privileges.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, GivenUserWhenGrantedTwoPrivilegesThenBothAreReturned) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  perms.Grant(memgraph::auth::Permission::CREATE);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name);
  ASSERT_EQ(privileges.size(), 2);
}

TEST_F(AuthQueryHandlerFixture, GivenUserAndRoleWhenOneGrantedAndOtherGrantedThenBothArePrinted) {
  perms.Grant(memgraph::auth::Permission::MATCH);
  memgraph::auth::Role role = memgraph::auth::Role{"Mates_role", perms};
  auth->SaveRole(role);
  memgraph::auth::User user = memgraph::auth::User{user_name, std::nullopt, perms};
  user.SetRole(role);
  auth->SaveUser(user);

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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

  auto privileges = auth_handler.GetPrivileges(user_name);
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
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_THROW(auth_handler.CreateProfile("profile", {}, nullptr), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(
      auth_handler.CreateProfile(
          "another_profile",
          {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], mem_limit}}, nullptr),
      memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, UpdateProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));
  ASSERT_THROW(
      auth_handler.UpdateProfile(
          "profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], mem_limit}},
          nullptr, nullptr),
      memgraph::query::QueryRuntimeException);

  ASSERT_THROW(auth_handler.UpdateProfile("non_profile", {}, nullptr, nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, DropProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_THROW(auth_handler.DropProfile("non_profile", nullptr, nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, GetProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

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
  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr, nullptr));
  ASSERT_THROW(auth_handler.GetProfile("profile"), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, AllProfiles) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

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
  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr, nullptr));
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
  ASSERT_NO_THROW(auth_handler.DropProfile("other_profile", nullptr, nullptr));
  {
    const auto profiles = auth_handler.AllProfiles();
    ASSERT_EQ(profiles.size(), 0);
  }
}

TEST_F(AuthQueryHandlerFixture, SetProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));  // TODO

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr, nullptr));
  ASSERT_THROW(auth_handler.SetProfile("non_profile", "user", nullptr, nullptr),
               memgraph::query::QueryRuntimeException);
  ASSERT_THROW(auth_handler.SetProfile("profile", "non_user", nullptr, nullptr),
               memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, RevokeProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));  // TODO

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr, nullptr));
  ASSERT_THROW(auth_handler.RevokeProfile("non_user", nullptr, nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, GetProfileForUser) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));  // TODO

  ASSERT_FALSE(auth_handler.GetProfileForUser("user"));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr, nullptr));
  {
    const auto profile = auth_handler.GetProfileForUser("user");
    ASSERT_TRUE(profile.has_value());
    ASSERT_EQ(profile.value(), "profile");
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "user", nullptr, nullptr));
  {
    const auto profile = auth_handler.GetProfileForUser("user");
    ASSERT_TRUE(profile.has_value());
    ASSERT_EQ(profile.value(), "other_profile");
  }
  ASSERT_NO_THROW(auth_handler.DropProfile("other_profile", nullptr, nullptr));
  ASSERT_FALSE(auth_handler.GetProfileForUser("user"));

  ASSERT_THROW(auth_handler.GetProfileForUser("non_user"), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, GetUsersForProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr, nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user1", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user2", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user3", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateUser("user4", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));  // TODO

  ASSERT_EQ(auth_handler.GetUsersForProfile("profile").size(), 0);

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user1", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user2", nullptr, nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user3", nullptr, nullptr));
  {
    const auto users = auth_handler.GetUsersForProfile("profile");
    ASSERT_EQ(users.size(), 3);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user2") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user3") != users.end());
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("user2", nullptr, nullptr));
  {
    const auto users = auth_handler.GetUsersForProfile("profile");
    ASSERT_EQ(users.size(), 2);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user3") != users.end());
  }
  ASSERT_NO_THROW(auth_handler.DropUser("user3", nullptr));
  {
    const auto users = auth_handler.GetUsersForProfile("profile");
    ASSERT_EQ(users.size(), 1);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
  }
  ASSERT_THROW(auth_handler.GetUsersForProfile("non_profile"), memgraph::query::QueryRuntimeException);
}

#endif
