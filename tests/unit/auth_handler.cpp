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
  user.SetRole(role);
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
  user.SetRole(role);
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
  user.SetRole(role);
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
  user.SetRole(role);
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
  auto read_permission = memgraph::auth::FineGrainedAccessPermissions();
  read_permission.Grant(label_repr, memgraph::auth::FineGrainedPermission::READ);

  handler = memgraph::auth::FineGrainedAccessHandler{
      memgraph::auth::FineGrainedAccessPermissions{read_permission},
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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
  auth.value()->SaveUser(user);

  auto privileges =
      auth_handler.GetPrivileges(user_name, std::optional<std::string>{std::string{memgraph::dbms::kDefaultDB}});
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

TEST_F(AuthQueryHandlerFixture, UpdateProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

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
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_NO_THROW(auth_handler.DropProfile("profile", nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
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
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
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
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
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
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
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
  ASSERT_THROW(auth_handler.SetProfile("profile", "non_user", nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, RevokeProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
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
  ASSERT_THROW(auth_handler.RevokeProfile("non_user", nullptr), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, UserProfileRole) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));
  auth_handler.SetRole("user", "role", nullptr);

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "role", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "role", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "role", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[1], mem_limit}}, nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile",
      {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], unlimited}}, nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "user", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  ASSERT_THROW(auth_handler.SetProfile("non_profile", "role", nullptr), memgraph::query::QueryRuntimeException);
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("role", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "role", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  auth_handler.ClearRole("user", nullptr);
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
  ASSERT_NO_THROW(auth_handler.SetRole("user", "role", nullptr));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, mem_limit.mem_limit.value * mem_limit.mem_limit.scale);
  }
  auth_handler.DropRole("role", nullptr);
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }
}

TEST_F(AuthQueryHandlerFixture, GetProfileForUser) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
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

  ASSERT_THROW(auth_handler.GetProfileForUser("non_user"), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, GetProfileForRole) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateRole("role", nullptr));

  ASSERT_FALSE(auth_handler.GetProfileForRole("role"));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "role", nullptr));
  {
    const auto profile = auth_handler.GetProfileForRole("role");
    ASSERT_TRUE(profile.has_value());
    ASSERT_EQ(profile.value(), "profile");
  }
  ASSERT_NO_THROW(auth_handler.SetProfile("other_profile", "role", nullptr));
  {
    const auto profile = auth_handler.GetProfileForRole("role");
    ASSERT_TRUE(profile.has_value());
    ASSERT_EQ(profile.value(), "other_profile");
  }

  ASSERT_TRUE(auth_handler.CreateUser("user", {}, nullptr));
  auth_handler.SetRole("user", "role", nullptr);
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, quantity.quantity.value);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }

  ASSERT_NO_THROW(auth_handler.DropProfile("other_profile", nullptr));
  ASSERT_FALSE(auth_handler.GetProfileForRole("role"));
  {
    const auto resource = resources.GetUser("user");
    ASSERT_EQ(resource->GetSessions().second, -1);
    ASSERT_EQ(resource->GetTransactionsMemory().second, -1);
  }

  ASSERT_THROW(auth_handler.GetProfileForRole("non_role"), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, GetUsersForProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
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
    const auto users = auth_handler.GetUsernamesForProfile("profile");
    ASSERT_EQ(users.size(), 1);
    ASSERT_TRUE(std::find(users.begin(), users.end(), "user1") != users.end());
  }
  ASSERT_THROW(auth_handler.GetUsernamesForProfile("non_profile"), memgraph::query::QueryRuntimeException);
}

TEST_F(AuthQueryHandlerFixture, GetRolesForProfile) {
  ASSERT_NO_THROW(auth_handler.CreateProfile("profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.CreateProfile("other_profile", {}, nullptr));
  ASSERT_NO_THROW(auth_handler.UpdateProfile(
      "other_profile", {memgraph::query::UserProfileQuery::limit_t{memgraph::auth::UserProfiles::kLimits[0], quantity}},
      nullptr));

  ASSERT_TRUE(auth_handler.CreateRole("role1", nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role2", nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role3", nullptr));
  ASSERT_TRUE(auth_handler.CreateRole("role4", nullptr));

  ASSERT_EQ(auth_handler.GetRolenamesForProfile("profile").size(), 0);

  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "role1", nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "role2", nullptr));
  ASSERT_NO_THROW(auth_handler.SetProfile("profile", "role3", nullptr));
  {
    const auto rolenames = auth_handler.GetRolenamesForProfile("profile");
    ASSERT_EQ(rolenames.size(), 3);
    ASSERT_TRUE(std::find(rolenames.begin(), rolenames.end(), "role1") != rolenames.end());
    ASSERT_TRUE(std::find(rolenames.begin(), rolenames.end(), "role2") != rolenames.end());
    ASSERT_TRUE(std::find(rolenames.begin(), rolenames.end(), "role3") != rolenames.end());
  }
  ASSERT_NO_THROW(auth_handler.RevokeProfile("role2", nullptr));
  {
    const auto rolenames = auth_handler.GetRolenamesForProfile("profile");
    ASSERT_EQ(rolenames.size(), 2);
    ASSERT_TRUE(std::find(rolenames.begin(), rolenames.end(), "role1") != rolenames.end());
    ASSERT_TRUE(std::find(rolenames.begin(), rolenames.end(), "role3") != rolenames.end());
  }
  ASSERT_NO_THROW(auth_handler.DropRole("role3", nullptr));
  {
    const auto rolenames = auth_handler.GetRolenamesForProfile("profile");
    ASSERT_EQ(rolenames.size(), 1);
    ASSERT_TRUE(std::find(rolenames.begin(), rolenames.end(), "role1") != rolenames.end());
  }
  ASSERT_THROW(auth_handler.GetRolenamesForProfile("non_profile"), memgraph::query::QueryRuntimeException);
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
      ASSERT_EQ(privilege[2].ValueString(), "LABEL PERMISSION GRANTED TO ROLE");
    } else if (privilege_name == "LABEL :Company") {
      found_label_company = true;
      ASSERT_EQ(privilege[1].ValueString(), "READ");
      ASSERT_EQ(privilege[2].ValueString(), "LABEL PERMISSION GRANTED TO ROLE");
    } else if (privilege_name == "EDGE_TYPE :WORKS_FOR") {
      found_edge_works_for = true;
      ASSERT_EQ(privilege[1].ValueString(), "UPDATE");
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

#ifdef MG_ENTERPRISE
TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_Success) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");
  auth->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  // Set multi-tenant role for specific database
  user.AddMultiTenantRole(role, "db1");
  auth->SaveUser(user);

  // Verify the role is assigned to the user for db1
  auto updated_user = auth->GetUser("test_user");
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
  auth->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  // Set multi-tenant role for multiple databases
  user.AddMultiTenantRole(role, "db1");
  user.AddMultiTenantRole(role, "db2");
  auth->SaveUser(user);

  // Verify the role is assigned to the user for both databases
  auto updated_user = auth->GetUser("test_user");
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
  auth->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  // Try to set multi-tenant role for a database the role doesn't have access to
  ASSERT_THROW(user.AddMultiTenantRole(role, "db1"), memgraph::auth::AuthException);
}

TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_GlobalRoleAlreadySet_Throws) {
  // Create a role
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth->SaveRole(role);

  // Create a user and set the role globally
  memgraph::auth::User user("test_user");
  user.SetRole(role);
  auth->SaveUser(user);

  // Try to set the same role as multi-tenant role
  ASSERT_THROW(user.AddMultiTenantRole(role, "db1"), memgraph::auth::AuthException);
}

TEST_F(AuthQueryHandlerFixture, ClearMultiTenantRoles_Success) {
  // Create a role with access to multiple databases
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  role.db_access().Grant("db2");
  auth->SaveRole(role);

  // Create a user and set multi-tenant roles
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  user.AddMultiTenantRole(role, "db2");
  auth->SaveUser(user);

  // Clear multi-tenant roles for db1
  user.ClearMultiTenantRoles("db1");
  auth->SaveUser(user);

  // Verify roles are cleared for db1 but remain for db2
  auto updated_user = auth->GetUser("test_user");
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
  auth->SaveUser(user);

  // Clear multi-tenant roles for non-existent database (should not throw)
  ASSERT_NO_THROW(user.ClearMultiTenantRoles("non_existent_db"));
}

TEST_F(AuthQueryHandlerFixture, GetMultiTenantRoles_Empty) {
  // Create a user without any multi-tenant roles
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  auto updated_user = auth->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 0);
}

TEST_F(AuthQueryHandlerFixture, SetRole_WithDatabaseSpecification_Success) {
  // Create roles with database access
  memgraph::auth::Role role1("role1");
  role1.db_access().Grant("db1");
  auth->SaveRole(role1);

  memgraph::auth::Role role2("role2");
  role2.db_access().Grant("db2");
  auth->SaveRole(role2);

  // Create a user
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  // Set roles with database specification
  {
    std::vector<std::string> roles = {"role1", "role2"};
    std::unordered_set<std::string> databases = {"db1", "db2"};
    ASSERT_THROW(auth_handler.SetRoles("test_user", roles, databases, nullptr), memgraph::query::QueryRuntimeException);
  }

  // Update roles to have access to both databases
  role1.db_access().Grant("db2");
  auth->SaveRole(role1);
  role2.db_access().Grant("db1");
  auth->SaveRole(role2);
  auth->SaveUser(user);

  // Set roles with database specification
  {
    std::vector<std::string> roles = {"role1", "role2"};
    std::unordered_set<std::string> databases = {"db1", "db2"};
    ASSERT_NO_THROW(auth_handler.SetRoles("test_user", roles, databases, nullptr));
  }

  // Verify the roles are set as multi-tenant roles
  auto updated_user = auth->GetUser("test_user");
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
  auth->SaveRole(role);

  // Create a user and set multi-tenant roles
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  user.AddMultiTenantRole(role, "db2");
  auth->SaveUser(user);

  // Clear roles for specific database
  std::unordered_set<std::string> databases = {"db1"};
  ASSERT_NO_THROW(auth_handler.ClearRoles("test_user", databases, nullptr));

  // Verify roles are cleared for db1 but remain for db2
  auto updated_user = auth->GetUser("test_user");
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
  auth->SaveRole(role);

  // Create a user and set multi-tenant role
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  auth->SaveUser(user);

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
  auth->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  // Manually add multi-tenant link to storage
  nlohmann::json mt_link_data = nlohmann::json::array();
  mt_link_data.push_back({{"rolename", "test_role"}});

  // This would normally be done through the storage interface
  // For testing, we'll simulate the storage update
  user.AddMultiTenantRole(role, "db1");
  auth->SaveUser(user);

  // Verify the user has the role linked
  auto updated_user = auth->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 1);
  ASSERT_EQ(roles.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, RemoveUser_ClearsMultiTenantLinks) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth->SaveRole(role);

  // Create a user and set multi-tenant role
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  auth->SaveUser(user);

  // Verify user exists with multi-tenant role
  auto existing_user = auth->GetUser("test_user");
  ASSERT_TRUE(existing_user);
  ASSERT_EQ(existing_user->GetMultiTenantRoles("db1").size(), 1);

  // Remove the user
  ASSERT_TRUE(auth->RemoveUser("test_user", nullptr));

  // Verify user is removed
  auto removed_user = auth->GetUser("test_user");
  ASSERT_FALSE(removed_user);
}

TEST_F(AuthQueryHandlerFixture, SetMultiTenantRole_AlreadySet_NoChange) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth->SaveRole(role);

  // Create a user
  memgraph::auth::User user("test_user");
  auth->SaveUser(user);

  // Set multi-tenant role
  user.AddMultiTenantRole(role, "db1");
  auth->SaveUser(user);

  // Set the same role again (should not change anything)
  ASSERT_NO_THROW(user.AddMultiTenantRole(role, "db1"));
  auth->SaveUser(user);

  // Verify the role is still set correctly
  auto updated_user = auth->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 1);
  ASSERT_EQ(roles.begin()->rolename(), "test_role");
}

TEST_F(AuthQueryHandlerFixture, ClearRole_ClearsMultiTenantRoles) {
  // Create a role with database access
  memgraph::auth::Role role("test_role");
  role.db_access().Grant("db1");
  auth->SaveRole(role);

  // Create a user and set multi-tenant role
  memgraph::auth::User user("test_user");
  user.AddMultiTenantRole(role, "db1");
  auth->SaveUser(user);

  // Clear all roles (should clear multi-tenant roles too)
  user.ClearAllRoles();
  auth->SaveUser(user);

  // Verify all roles are cleared
  auto updated_user = auth->GetUser("test_user");
  ASSERT_TRUE(updated_user);

  auto roles = updated_user->GetMultiTenantRoles("db1");
  ASSERT_EQ(roles.size(), 0);
  ASSERT_TRUE(updated_user->roles().empty());
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
  ASSERT_NO_THROW(auth_handler.SetRoles("multiuser", roles, std::unordered_set<std::string>{}, nullptr));

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
  ASSERT_NO_THROW(auth_handler.SetRoles("user1", roles, std::unordered_set<std::string>{}, nullptr));
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
  auto role1 = auth->AddRole("role1");
  auto role2 = auth->AddRole("role2");
  ASSERT_TRUE(role1);
  ASSERT_TRUE(role2);
  auto user = auth->AddUser("user3");
  ASSERT_TRUE(user);
  // Set duplicate roles
  std::vector<std::string> roles = {"role1", "role2", "role1", "role2"};
  ASSERT_NO_THROW(auth_handler.SetRoles("user3", roles, std::unordered_set<std::string>{}, nullptr));
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
