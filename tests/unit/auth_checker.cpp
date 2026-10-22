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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <span>

#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "auth/repository.hpp"
#include "auth_test_utils.hpp"
#include "disk_test_utils.hpp"
#include "glue/auth_checker.hpp"
#include "kvstore/kvstore.hpp"

#include "license/license.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/query_user.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"

#ifdef MG_ENTERPRISE
template <typename StorageType>
class FineGrainedAuthCheckerFixture : public testing::Test {
 protected:
  const std::string testSuite = "auth_checker";

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new StorageType(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{db->Access(memgraph::storage::WRITE)};
  memgraph::query::DbAccessor dba{storage_dba.get()};

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  memgraph::query::VertexAccessor v1{dba.InsertVertex()};
  memgraph::query::VertexAccessor v2{dba.InsertVertex()};
  memgraph::query::VertexAccessor v3{dba.InsertVertex()};
  memgraph::storage::EdgeTypeId edge_type_one{db->NameToEdgeType("edge_type_1")};
  memgraph::storage::EdgeTypeId edge_type_two{db->NameToEdgeType("edge_type_2")};

  memgraph::query::EdgeAccessor r1{*dba.InsertEdge(&this->v1, &this->v2, edge_type_one)};
  memgraph::query::EdgeAccessor r2{*dba.InsertEdge(&this->v1, &this->v3, edge_type_one)};
  memgraph::query::EdgeAccessor r3{*dba.InsertEdge(&this->v1, &this->v2, edge_type_two)};
  memgraph::query::EdgeAccessor r4{*dba.InsertEdge(&this->v1, &this->v3, edge_type_two)};

  void SetUp() override {
    memgraph::license::global_license_checker.EnableTesting();
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).has_value());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).has_value());
    ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l3")).has_value());
    dba.AdvanceCommand();
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(FineGrainedAuthCheckerFixture, StorageTypes);

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantedAllLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v2, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v2, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v3, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantedAllEdgeTypes) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r2, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DeniedAllLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().DenyGlobal(memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(
      auth_checker.Has(this->v1, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v1, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v2, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v2, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DeniedAllEdgeTypes) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().DenyGlobal(memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r2, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantLabel) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant({"l1"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DenyLabel) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Deny({"l3"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantAndDenySpecificLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant({"l1"}, memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Grant({"l2"}, memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Deny({"l3"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v2, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v2, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, MultipleVertexLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant({"l1"}, memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Grant({"l2"}, memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Deny({"l3"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  ASSERT_TRUE(this->v1.AddLabel(this->dba.NameToLabel("l3")).has_value());
  ASSERT_TRUE(this->v2.AddLabel(this->dba.NameToLabel("l1")).has_value());
  this->dba.AdvanceCommand();

  ASSERT_FALSE(
      auth_checker.Has(this->v1, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v1, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v2, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v2, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantEdgeType) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().Grant({"edge_type_1"},
                                                                   memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DenyEdgeType) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().Deny({"edge_type_1"},
                                                                  memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantAndDenySpecificEdgeTypes) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().Grant({"edge_type_1"},
                                                                   memgraph::auth::kAllEdgeTypePermissions);
  user.fine_grained_access_handler().edge_type_permissions().Deny({"edge_type_2"},
                                                                  memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r2, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GlobalGrantWithSpecificLabelDenyRequiresAuthChecker) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Deny({"l3"}, memgraph::auth::FineGrainedPermission::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.HasAllGlobalPrivilegesOnVertices());
  ASSERT_FALSE(auth_checker.HasUnrestrictedAccessToVertices());
}

// Global grant on * plus a grant-only rule that narrows permissions on a label must still attach the FGA checker:
// FineGrainedAccessPermissions::Has denies permissions not covered by the matching rule, even when no explicit DENY
// bitmask is set on any rule.
TYPED_TEST(FineGrainedAuthCheckerFixture, GlobalGrantWithSpecificLabelGrantOnlyRequiresAuthChecker) {
  memgraph::auth::User user{"test"};
  auto &label_perms = user.fine_grained_access_handler().label_permissions();
  label_perms.GrantGlobal(memgraph::auth::kAllLabelPermissions);
  label_perms.Grant({"l3"}, memgraph::auth::FineGrainedPermission::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.HasAllGlobalPrivilegesOnVertices());
  ASSERT_FALSE(auth_checker.HasUnrestrictedAccessToVertices());

  ASSERT_TRUE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v3, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(
      this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GlobalGrantWithSpecificEdgeTypeDenyRequiresAuthChecker) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(memgraph::auth::kAllEdgeTypePermissions);
  user.fine_grained_access_handler().edge_type_permissions().Deny({"edge_type_2"},
                                                                  memgraph::auth::FineGrainedPermission::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.HasAllGlobalPrivilegesOnEdges());
  ASSERT_FALSE(auth_checker.HasUnrestrictedAccessToEdges());
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GlobalGrantWithSpecificEdgeTypeGrantOnlyRequiresAuthChecker) {
  memgraph::auth::User user{"test"};
  auto &edge_perms = user.fine_grained_access_handler().edge_type_permissions();
  edge_perms.GrantGlobal(memgraph::auth::kAllEdgeTypePermissions);
  edge_perms.Grant({"edge_type_2"}, memgraph::auth::FineGrainedPermission::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.HasAllGlobalPrivilegesOnEdges());
  ASSERT_FALSE(auth_checker.HasUnrestrictedAccessToEdges());

  ASSERT_TRUE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE));
}

TEST(AuthChecker, Generate) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);

  auto empty_user = auth_checker.GenQueryUser(std::nullopt, {});
  ASSERT_THROW(auth_checker.GenQueryUser("does_not_exist", {}), memgraph::auth::AuthException);

  EXPECT_FALSE(empty_user && *empty_user);
  // Still empty auth, so the above should have su permissions
  using enum memgraph::query::AuthQuery::Privilege;
  EXPECT_TRUE(empty_user->IsAuthorized({AUTH, REMOVE, REPLICATION}, "", &memgraph::query::session_long_policy));
  EXPECT_TRUE(empty_user->IsAuthorized(
      {FREE_MEMORY, WEBSOCKET, MULTI_DATABASE_EDIT}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_TRUE(
      empty_user->IsAuthorized({TRIGGER, DURABILITY, STORAGE_MODE}, "some_db", &memgraph::query::session_long_policy));

  // Add user
  auto user = AddUser(*auth.Lock(), "new_user");
  ASSERT_TRUE(user);

  // ~Empty user should now fail~
  // NOTE: Cache invalidation has been disabled, so this will pass; change if it is ever turned on
  EXPECT_TRUE(empty_user->IsAuthorized({AUTH, REMOVE, REPLICATION}, "", &memgraph::query::session_long_policy));
  EXPECT_TRUE(empty_user->IsAuthorized(
      {FREE_MEMORY, WEBSOCKET, MULTI_DATABASE_EDIT}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_TRUE(
      empty_user->IsAuthorized({TRIGGER, DURABILITY, STORAGE_MODE}, "some_db", &memgraph::query::session_long_policy));

  // Add role and new user
  auto new_role = *auth->AddRole("new_role");
  auto new_user2 = *AddUser(*auth.Lock(), "new_user2");
  auto role = auth_checker.GenQueryUser("anyuser", {"new_role"});
  auto user2 = auth_checker.GenQueryUser("new_user2", {});

  // Should be permission-less by default
  EXPECT_FALSE(role->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({FREE_MEMORY}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({SERVER_SIDE_PARAMETERS}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({FREE_MEMORY}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({SERVER_SIDE_PARAMETERS}, "memgraph", &memgraph::query::session_long_policy));

  // Update permissions and recheck
  new_user2.permissions().Grant(memgraph::auth::Permission::AUTH);
  new_user2.permissions().Grant(memgraph::auth::Permission::SERVER_SIDE_PARAMETERS);
  new_role.permissions().Grant(memgraph::auth::Permission::TRIGGER);
  auth->SaveUser(new_user2);
  auth->SaveRole(new_role);
  role = auth_checker.GenQueryUser("no check", {"new_role"});
  user2 = auth_checker.GenQueryUser("new_user2", {});
  EXPECT_FALSE(role->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({FREE_MEMORY}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({SERVER_SIDE_PARAMETERS}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({FREE_MEMORY}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_TRUE(user2->IsAuthorized({SERVER_SIDE_PARAMETERS}, "memgraph", &memgraph::query::session_long_policy));

  // Connect role and recheck
  new_user2.ClearAllRoles();
  new_user2.AddRole(new_role);
  auth->SaveUser(new_user2);
  user2 = auth_checker.GenQueryUser("new_user2", {});
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({FREE_MEMORY}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_TRUE(user2->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));

  // Add database and recheck
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "non_default", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "another", &memgraph::query::session_long_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "non_default", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another", &memgraph::query::session_long_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));

  new_user2.db_access().Grant("another");
  new_role.db_access().Grant("non_default");
  auth->SaveUser(new_user2);
  auth->SaveRole(new_role);
  // Session policy test
  // Session long policy
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "non_default", &memgraph::query::session_long_policy));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "another", &memgraph::query::session_long_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "non_default", &memgraph::query::session_long_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another", &memgraph::query::session_long_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::session_long_policy));
  // Up to date policy
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "non_default", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "another", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "non_default", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::up_to_date_policy));

  new_user2.db_access().Deny("memgraph");
  new_role.db_access().Deny("non_default");
  auth->SaveUser(new_user2);
  auth->SaveRole(new_role);
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "non_default", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "another", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "non_default", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::up_to_date_policy));

  new_user2.db_access().Revoke("memgraph");
  new_role.db_access().Revoke("non_default");
  auth->SaveUser(new_user2);
  auth->SaveRole(new_role);
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "non_default", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "another", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "memgraph", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "non_default", &memgraph::query::up_to_date_policy));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph", &memgraph::query::up_to_date_policy));

  // User impersonation
  EXPECT_FALSE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // grant all
  new_user2.permissions().Grant(memgraph::auth::Permission::IMPERSONATE_USER);
  new_user2.GrantUserImp();
  auth->SaveUser(new_user2);
  EXPECT_TRUE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // deny "new_user"
  new_user2.DenyUserImp({*user});
  auth->SaveUser(new_user2);
  EXPECT_FALSE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // grant just "new_user"
  new_user2.GrantUserImp({*user});
  auth->SaveUser(new_user2);
  EXPECT_TRUE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // revoke permission
  new_user2.permissions().Revoke(memgraph::auth::Permission::IMPERSONATE_USER);
  auth->SaveUser(new_user2);
  EXPECT_FALSE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));

  // via role
  // grant all
  new_role.permissions().Grant(memgraph::auth::Permission::IMPERSONATE_USER);
  new_role.GrantUserImp();
  auth->SaveRole(new_role);
  EXPECT_TRUE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // deny "new_user"
  new_role.DenyUserImp({*user});
  auth->SaveRole(new_role);
  EXPECT_FALSE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // grant "new_user" to user
  new_user2.GrantUserImp({*user});
  auth->SaveUser(new_user2);
  EXPECT_FALSE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
  // revoke permission
  new_role.permissions().Revoke(memgraph::auth::Permission::IMPERSONATE_USER);
  auth->SaveRole(new_role);
  EXPECT_FALSE(user2->CanImpersonate("new_user", &memgraph::query::up_to_date_policy));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, RequiresFgDuringExec_WildcardVertexEdgeUnrestricted_NoPropertyRules) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  ASSERT_FALSE(auth_checker.NeedsFineGrainedAuthChecker());
}

TYPED_TEST(FineGrainedAuthCheckerFixture, RequiresFgDuringExec_WildcardVertexEdgeUnrestricted_PropertyRulesNonEmpty) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(memgraph::auth::kAllEdgeTypePermissions);
  user.property_access_handler().label_properties().Deny({"l1"}, "ssn", memgraph::auth::PropertyPermissionType::READ);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  ASSERT_TRUE(auth_checker.NeedsFineGrainedAuthChecker());
}

TYPED_TEST(FineGrainedAuthCheckerFixture, RequiresFgDuringExec_LabelFgRestricted_AlwaysTrue) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant({"l1"}, memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(memgraph::auth::kAllEdgeTypePermissions);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  ASSERT_TRUE(auth_checker.NeedsFineGrainedAuthChecker());
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGAMultiLabelDenyWins) {
  // v1 has label "l1"; add "l2" so it has two labels
  ASSERT_TRUE(this->v1.AddLabel(this->dba.NameToLabel("l2")).has_value());
  this->dba.AdvanceCommand();

  memgraph::auth::User user{"test"};
  // GRANT all properties on l1, DENY ssn on l2
  user.property_access_handler().label_properties().Grant({"l1"}, "*", memgraph::auth::PropertyPermissionType::READ);
  user.property_access_handler().label_properties().Deny({"l2"}, "ssn", memgraph::auth::PropertyPermissionType::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  auto ssn_id = this->dba.NameToProperty("ssn");
  auto name_id = this->dba.NameToProperty("name");
  auto labels = this->v1.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels.has_value());

  std::vector<memgraph::storage::LabelId> label_ids(labels->begin(), labels->end());

  // ssn denied because l2 denies it (DENY wins across labels)
  EXPECT_FALSE(
      auth_checker.HasPropertyPermission(label_ids, ssn_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
  // name allowed (l1 grants *, l2 has no rule for name so neutral → l1's grant applies)
  EXPECT_TRUE(
      auth_checker.HasPropertyPermission(label_ids, name_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGANoRulesDenied) {
  memgraph::auth::User user{"test"};
  // No property rules at all → no access
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  auto ssn_id = this->dba.NameToProperty("ssn");
  auto labels = this->v1.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels.has_value());
  std::vector<memgraph::storage::LabelId> label_ids(labels->begin(), labels->end());

  EXPECT_FALSE(
      auth_checker.HasPropertyPermission(label_ids, ssn_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGARulesExistButNoGrant) {
  memgraph::auth::User user{"test"};
  // Rules exist for l1 (grant "name" only), so "ssn" has no grant → denied
  user.property_access_handler().label_properties().Grant({"l1"}, "name", memgraph::auth::PropertyPermissionType::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  auto ssn_id = this->dba.NameToProperty("ssn");
  auto name_id = this->dba.NameToProperty("name");
  auto labels = this->v1.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels.has_value());
  std::vector<memgraph::storage::LabelId> label_ids(labels->begin(), labels->end());

  EXPECT_FALSE(
      auth_checker.HasPropertyPermission(label_ids, ssn_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
  EXPECT_TRUE(
      auth_checker.HasPropertyPermission(label_ids, name_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGAEdgeTypeUnmentionedPropertyDenied) {
  memgraph::auth::User user{"test"};
  // Grant only "secret_code" on edge_type_1 — no wildcard, no rule for "amount"
  user.property_access_handler().edge_type_properties().Grant(
      {"edge_type_1"}, "secret_code", memgraph::auth::PropertyPermissionType::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  auto secret_id = this->dba.NameToProperty("secret_code");
  auto amount_id = this->dba.NameToProperty("amount");

  EXPECT_TRUE(auth_checker.HasPropertyPermission(
      this->edge_type_one, secret_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
  // "amount" has no grant → denied (no rules = no access)
  EXPECT_FALSE(auth_checker.HasPropertyPermission(
      this->edge_type_one, amount_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGAEdgeTypeDenyOverridesGrant) {
  memgraph::auth::User user{"test"};
  // Grant all edge properties, deny "secret_code" on edge_type_1
  user.property_access_handler().edge_type_properties().Grant(
      {"edge_type_1"}, "*", memgraph::auth::PropertyPermissionType::READ);
  user.property_access_handler().edge_type_properties().Deny(
      {"edge_type_1"}, "secret_code", memgraph::auth::PropertyPermissionType::READ);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  auto secret_id = this->dba.NameToProperty("secret_code");
  auto amount_id = this->dba.NameToProperty("amount");

  EXPECT_FALSE(auth_checker.HasPropertyPermission(
      this->edge_type_one, secret_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
  EXPECT_TRUE(auth_checker.HasPropertyPermission(
      this->edge_type_one, amount_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGAMatchingAnyMultiLabel) {
  // v1 has label "l1"; add "l2" so it has labels {l1, l2}
  ASSERT_TRUE(this->v1.AddLabel(this->dba.NameToLabel("l2")).has_value());
  this->dba.AdvanceCommand();

  memgraph::auth::User user{"test"};
  // GRANT READ {*} ON NODES CONTAINING LABELS :l1, :l2 MATCHING ANY
  user.property_access_handler().label_properties().Grant(
      {"l1", "l2"}, "*", memgraph::auth::PropertyPermissionType::READ, memgraph::auth::MatchingMode::ANY);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  auto name_id = this->dba.NameToProperty("name");

  // v1 has {l1, l2} — l1 is in the rule, so ANY matches
  auto labels = this->v1.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels.has_value());
  std::vector<memgraph::storage::LabelId> label_ids(labels->begin(), labels->end());
  EXPECT_TRUE(
      auth_checker.HasPropertyPermission(label_ids, name_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));

  // v2 has only {l2} — l2 is in the rule, so ANY matches
  auto labels2 = this->v2.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels2.has_value());
  std::vector<memgraph::storage::LabelId> label_ids2(labels2->begin(), labels2->end());
  EXPECT_TRUE(auth_checker.HasPropertyPermission(
      label_ids2, name_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGAMatchingExactlyMultiLabel) {
  // v1 has label "l1"; add "l2" so it has labels {l1, l2}
  ASSERT_TRUE(this->v1.AddLabel(this->dba.NameToLabel("l2")).has_value());
  this->dba.AdvanceCommand();

  memgraph::auth::User user{"test"};
  // GRANT READ {*} ON NODES CONTAINING LABELS :l1, :l2 MATCHING EXACTLY
  user.property_access_handler().label_properties().Grant(
      {"l1", "l2"}, "*", memgraph::auth::PropertyPermissionType::READ, memgraph::auth::MatchingMode::EXACTLY);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  auto name_id = this->dba.NameToProperty("name");

  // v1 has {l1, l2} — exact match
  auto labels = this->v1.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels.has_value());
  std::vector<memgraph::storage::LabelId> label_ids(labels->begin(), labels->end());
  EXPECT_TRUE(
      auth_checker.HasPropertyPermission(label_ids, name_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));

  // v2 has only {l2} — does NOT match exactly (missing l1)
  auto labels2 = this->v2.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels2.has_value());
  std::vector<memgraph::storage::LabelId> label_ids2(labels2->begin(), labels2->end());
  EXPECT_FALSE(auth_checker.HasPropertyPermission(
      label_ids2, name_id, memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, PropertyFGAGlobalGrantDeniedOnUnlabeledVertex) {
  memgraph::auth::User user{"test"};
  user.property_access_handler().label_properties().GrantGlobal("*", memgraph::auth::PropertyPermissionType::READ);

  auto unlabeled = this->dba.InsertVertex();
  auto name_id = this->dba.NameToProperty("name");
  ASSERT_TRUE(unlabeled.SetProperty(name_id, memgraph::storage::PropertyValue("value")).has_value());
  this->dba.AdvanceCommand();

  auto labels = unlabeled.Labels(memgraph::storage::View::NEW);
  ASSERT_TRUE(labels.has_value());
  ASSERT_TRUE(labels->empty());

  EXPECT_EQ(
      user.property_access_handler().label_properties().HasGlobal("name", memgraph::auth::PropertyPermissionType::READ),
      memgraph::auth::PermissionLevel::GRANT);

  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  // Global READ {*} should apply even when the vertex has no labels (issue #1).
  EXPECT_TRUE(auth_checker.HasPropertyPermission(std::span<memgraph::storage::LabelId const>{},
                                                 name_id,
                                                 memgraph::query::AuthQuery::PropertyPermissionType::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, CachedCheckerRebuildsOnLicenseActivation) {
  namespace auth = memgraph::auth;
  namespace query = memgraph::query;

  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_cached_fga_license_test"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) std::filesystem::remove_all(auth_dir);
  });

  auth::SynchedAuth synched_auth(auth_dir, auth::Auth::Config{});
  memgraph::glue::AuthChecker auth_checker(&synched_auth);

  auto user = *AddUser(*synched_auth.Lock(), "test_user");
  user.fine_grained_access_handler().label_permissions().Grant({"l1"}, auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Deny({"l3"}, auth::kAllLabelPermissions);
  synched_auth->SaveUser(user);

  auto query_user = auth_checker.GenQueryUser("test_user", {});
  ASSERT_TRUE(query_user && *query_user);

  memgraph::license::global_license_checker.DisableTesting();

  query::CachedFineGrainedAuth cache;
  cache.Refresh(auth_checker, *query_user, &this->dba, this->dba.DatabaseName());
  EXPECT_EQ(cache.get(), nullptr);

  memgraph::license::global_license_checker.EnableTesting();

  // Refresh should detect the license change and rebuild
  cache.Refresh(auth_checker, *query_user, &this->dba, this->dba.DatabaseName());
  ASSERT_NE(cache.get(), nullptr);

  EXPECT_TRUE(cache.get()->Has(this->v1, memgraph::storage::View::NEW, query::AuthQuery::FineGrainedPrivilege::READ));
  EXPECT_FALSE(cache.get()->Has(this->v3, memgraph::storage::View::NEW, query::AuthQuery::FineGrainedPrivilege::READ));
}

#endif

TEST(AuthChecker, DroppedPrincipalSessionIsDenied) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_dropped"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);
  using enum memgraph::query::AuthQuery::Privilege;
  auto *const session_long = &memgraph::query::session_long_policy;
  auto *const up_to_date = &memgraph::query::up_to_date_policy;

  // Another user keeps access control on after the drop.
  ASSERT_TRUE(AddUser(*auth.Lock(), "admin"));
  auto limited = *AddUser(*auth.Lock(), "limited");
  limited.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(limited);
  auto role = *auth->AddRole("reader");
  role.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveRole(role);

  auto keep = *auth->AddRole("keep");
  keep.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveRole(keep);
  auto lose = *auth->AddRole("lose");
  lose.permissions().Grant(memgraph::auth::Permission::CREATE);
  auth->SaveRole(lose);

#ifdef MG_ENTERPRISE
  limited.permissions().Grant(memgraph::auth::Permission::IMPERSONATE_USER);
  limited.GrantUserImp();
  auth->SaveUser(limited);
  role.permissions().Grant(memgraph::auth::Permission::IMPERSONATE_USER);
  role.GrantUserImp();
  auth->SaveRole(role);
#endif

  auto user_session = auth_checker.GenQueryUser("limited", {});
  auto role_session = auth_checker.GenQueryUser("sso_user", {"reader"});
  auto partial_session = auth_checker.GenQueryUser("sso_partial", {"keep", "lose"});
  EXPECT_TRUE(partial_session->IsAuthorized({MATCH}, "memgraph", session_long));
  EXPECT_TRUE(partial_session->IsAuthorized({CREATE}, "memgraph", session_long));
  EXPECT_TRUE(user_session->IsAuthorized({MATCH}, "memgraph", session_long));
  EXPECT_FALSE(user_session->IsAuthorized({CREATE}, "memgraph", session_long));
  EXPECT_TRUE(role_session->IsAuthorized({MATCH}, "memgraph", session_long));
  EXPECT_FALSE(role_session->IsAuthorized({CREATE}, "memgraph", session_long));
#ifdef MG_ENTERPRISE
  EXPECT_TRUE(user_session->CanImpersonate("admin", session_long));
  EXPECT_TRUE(role_session->CanImpersonate("admin", session_long));
#endif

  ASSERT_TRUE(auth->RemoveUser("limited"));
  ASSERT_TRUE(auth->RemoveRole("reader"));
  ASSERT_TRUE(auth->RemoveRole("lose"));

  // Partial role loss: the surviving role's privilege stays, the dropped role's is gone.
  EXPECT_TRUE(partial_session->IsAuthorized({MATCH}, "memgraph", up_to_date));
  EXPECT_TRUE(partial_session->IsAuthorized({MATCH}, "memgraph", session_long));
  EXPECT_FALSE(partial_session->IsAuthorized({CREATE}, "memgraph", session_long));

  // Up-to-date checks see the drop and refresh the cached principal.
  EXPECT_FALSE(user_session->IsAuthorized({TRANSACTION_MANAGEMENT}, "memgraph", up_to_date));
  EXPECT_FALSE(role_session->IsAuthorized({TRANSACTION_MANAGEMENT}, "memgraph", up_to_date));

  // Once dropped, every later check is denied.
  for (const auto &session : {user_session, role_session}) {
    EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", session_long));
    EXPECT_FALSE(session->IsAuthorized({AUTH, DELETE}, std::nullopt, session_long));
    EXPECT_FALSE(session->IsAuthorized({MATCH}, "memgraph", session_long));
    EXPECT_FALSE(session->IsAuthorized({}, "memgraph", session_long));
    EXPECT_FALSE(session->IsAuthorized({}, std::nullopt, session_long));
    EXPECT_FALSE(session->clone()->IsAuthorized({CREATE}, "memgraph", session_long));
#ifdef MG_ENTERPRISE
    EXPECT_FALSE(session->CanImpersonate("admin", session_long));
    EXPECT_FALSE(session->CanImpersonate("admin", up_to_date));
#endif
  }

  // Re-creating a user with the same name does not revive the dropped session.
  auto recreated = *AddUser(*auth.Lock(), "limited");
  recreated.permissions().Grant(memgraph::auth::Permission::CREATE);
  auth->SaveUser(recreated);
  EXPECT_FALSE(user_session->IsAuthorized({CREATE}, "memgraph", up_to_date));
  EXPECT_FALSE(user_session->IsAuthorized({CREATE}, "memgraph", session_long));

  auto empty_session = auth_checker.GenEmptyUser();
  EXPECT_TRUE(empty_session->IsAuthorized({CREATE}, "memgraph", session_long));
}

TEST(AuthChecker, DroppedLastUserSessionGetsAuthOffAccess) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_dropped_last"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);
  using enum memgraph::query::AuthQuery::Privilege;
  auto *const session_long = &memgraph::query::session_long_policy;
  auto *const up_to_date = &memgraph::query::up_to_date_policy;

  auto solo = *AddUser(*auth.Lock(), "solo");
  solo.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(solo);

  auto session = auth_checker.GenQueryUser("solo", {});
  EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", up_to_date));

  // No users left: access control is off, so the dropped session is no longer restricted.
  ASSERT_TRUE(auth->RemoveUser("solo"));
  EXPECT_TRUE(session->IsAuthorized({CREATE}, "memgraph", up_to_date));
  EXPECT_TRUE(session->IsAuthorized({CREATE}, "memgraph", session_long));

  // Access control back on: the dropped session is not revived.
  auto recreated = *AddUser(*auth.Lock(), "solo");
  recreated.permissions().Grant(memgraph::auth::Permission::CREATE);
  auth->SaveUser(recreated);
  EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", up_to_date));
  EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", session_long));
}

TEST(AuthChecker, RecreatedUserDoesNotReviveSession) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_recreated"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);
  using enum memgraph::query::AuthQuery::Privilege;
  auto *const session_long = &memgraph::query::session_long_policy;
  auto *const up_to_date = &memgraph::query::up_to_date_policy;

  ASSERT_TRUE(AddUser(*auth.Lock(), "admin"));
  auto limited = *AddUser(*auth.Lock(), "limited");
  limited.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(limited);

  auto session = auth_checker.GenQueryUser("limited", {});
  EXPECT_TRUE(session->IsAuthorized({MATCH}, "memgraph", up_to_date));

  // Dropped and re-created under the same name with no session check in between: the new record is a new identity.
  ASSERT_TRUE(auth->RemoveUser("limited"));
  auto recreated = *AddUser(*auth.Lock(), "limited");
  recreated.permissions().Grant(memgraph::auth::Permission::MATCH);
  recreated.permissions().Grant(memgraph::auth::Permission::CREATE);
#ifdef MG_ENTERPRISE
  recreated.permissions().Grant(memgraph::auth::Permission::IMPERSONATE_USER);
  recreated.GrantUserImp();
#endif
  auth->SaveUser(recreated);

  EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", up_to_date));
  EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", session_long));
  EXPECT_FALSE(session->IsAuthorized({MATCH}, "memgraph", session_long));
#ifdef MG_ENTERPRISE
  EXPECT_FALSE(session->CanImpersonate("admin", up_to_date));
#endif

  // Control: a fresh session for the re-created user holds its grants.
  auto fresh = auth_checker.GenQueryUser("limited", {});
  EXPECT_TRUE(fresh->IsAuthorized({CREATE}, "memgraph", up_to_date));
  EXPECT_TRUE(fresh->IsAuthorized({MATCH}, "memgraph", session_long));
}

TEST(AuthChecker, RestoredOwnerIsMatchedByPersistedUuid) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_restored_owner"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);
  using enum memgraph::query::AuthQuery::Privilege;
  auto *const up_to_date = &memgraph::query::up_to_date_policy;

  ASSERT_TRUE(AddUser(*auth.Lock(), "admin"));
  auto owner = *AddUser(*auth.Lock(), "owner");
  owner.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(owner);

  auto original = auth_checker.GenQueryUser("owner", {});
  auto const stored_uuid = original->UserUuid();
  ASSERT_TRUE(stored_uuid.has_value());

  auto matching = auth_checker.GenQueryUser("owner", {}, stored_uuid);
  EXPECT_EQ(matching->UserUuid(), stored_uuid);
  EXPECT_TRUE(matching->IsAuthorized({MATCH}, "memgraph", up_to_date));

  // Legacy record (no uuid): resolved by name alone.
  auto legacy = auth_checker.GenQueryUser("owner", {}, std::nullopt);
  EXPECT_TRUE(legacy->IsAuthorized({MATCH}, "memgraph", up_to_date));

  // Re-created under the same name: the persisted uuid no longer matches, so the owner is refused but keeps its uuid.
  ASSERT_TRUE(auth->RemoveUser("owner"));
  auto recreated = *AddUser(*auth.Lock(), "owner");
  recreated.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(recreated);

  auto stale = auth_checker.GenQueryUser("owner", {}, stored_uuid);
  EXPECT_EQ(stale->username(), std::optional<std::string>{"owner"});
  EXPECT_EQ(stale->UserUuid(), stored_uuid);
  EXPECT_FALSE(stale->IsAuthorized({MATCH}, "memgraph", up_to_date));
  EXPECT_FALSE(stale->clone()->IsAuthorized({MATCH}, "memgraph", up_to_date));

  EXPECT_TRUE(auth_checker.GenQueryUser("owner", {}, std::nullopt)->IsAuthorized({MATCH}, "memgraph", up_to_date));
}

TEST(AuthChecker, OwnerCloneOfDroppedSessionPrincipalStaysRefused) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_owner_clone"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);
  using enum memgraph::query::AuthQuery::Privilege;
  auto *const up_to_date = &memgraph::query::up_to_date_policy;

  ASSERT_TRUE(AddUser(*auth.Lock(), "admin"));
  auto definer = *AddUser(*auth.Lock(), "definer");
  definer.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(definer);

  auto session = auth_checker.GenQueryUser("definer", {});
  auto const session_uuid = session->UserUuid();
  ASSERT_TRUE(session_uuid.has_value());

  ASSERT_TRUE(auth->RemoveUser("definer"));
  auto recreated = *AddUser(*auth.Lock(), "definer");
  recreated.permissions().Grant(memgraph::auth::Permission::MATCH);
  auth->SaveUser(recreated);

  // An owner cloned from the session keeps the old identity and is refused.
  auto owner = session->clone();
  EXPECT_EQ(owner->UserUuid(), session_uuid);
  EXPECT_FALSE(owner->IsAuthorized({MATCH}, "memgraph", up_to_date));

  // Rebuilding by name binds to the re-created user instead.
  auto by_name = auth_checker.GenQueryUser("definer", {});
  EXPECT_NE(by_name->UserUuid(), session_uuid);
  EXPECT_TRUE(by_name->IsAuthorized({MATCH}, "memgraph", up_to_date));
}

TEST(AuthChecker, UserWithoutStoredUuidKeepsSessionAcrossRefresh) {
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_legacy_uuid"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  using enum memgraph::query::AuthQuery::Privilege;

  // Strip the uuid from the stored record, as a user saved before uuids existed would look.
  {
    memgraph::auth::SynchedAuth seed(auth_dir, memgraph::auth::Auth::Config{/* default config */});
    auto legacy = *AddUser(*seed.Lock(), "legacy");
    legacy.permissions().Grant(memgraph::auth::Permission::MATCH);
    seed->SaveUser(legacy);
  }
  {
    memgraph::kvstore::KVStore store{auth_dir};
    auto const key = memgraph::auth::Repository::UserKey("legacy");
    auto record = nlohmann::json::parse(*store.Get(key));
    ASSERT_EQ(record.erase("uuid"), 1);
    ASSERT_TRUE(store.Put(key, record.dump()));
  }

  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);
  auto session = auth_checker.GenQueryUser("legacy", {});
  EXPECT_TRUE(session->IsAuthorized({MATCH}, "memgraph", &memgraph::query::up_to_date_policy));

  // The refresh re-loads the uuid-less record; its identity must stay stable (Nil) rather than read as a new user.
  auto other = *AddUser(*auth.Lock(), "other");
  auth->SaveUser(other);
  EXPECT_TRUE(session->IsAuthorized({MATCH}, "memgraph", &memgraph::query::up_to_date_policy));
  EXPECT_TRUE(session->IsAuthorized({MATCH}, "memgraph", &memgraph::query::session_long_policy));
  EXPECT_FALSE(session->IsAuthorized({CREATE}, "memgraph", &memgraph::query::up_to_date_policy));
}

#ifdef MG_ENTERPRISE
TEST(AuthChecker, DroppedUserStillGetsFineGrainedChecker) {
  memgraph::license::global_license_checker.EnableTesting();
  std::filesystem::path auth_dir{std::filesystem::temp_directory_path() / "MG_auth_checker_dropped_fga"};
  memgraph::utils::OnScopeExit clean([&]() {
    if (std::filesystem::exists(auth_dir)) {
      std::filesystem::remove_all(auth_dir);
    }
  });
  memgraph::auth::SynchedAuth auth(auth_dir, memgraph::auth::Auth::Config{/* default config */});
  memgraph::glue::AuthChecker auth_checker(&auth);

  ASSERT_TRUE(AddUser(*auth.Lock(), "admin"));
  auto limited = *AddUser(*auth.Lock(), "limited");
  limited.fine_grained_access_handler().label_permissions().Grant({"visible"},
                                                                  memgraph::auth::FineGrainedPermission::READ);
  auth->SaveUser(limited);
  auto session = auth_checker.GenQueryUser("limited", {});

  memgraph::storage::Config config{};
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(config)};
  auto storage_dba = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba{storage_dba.get()};

  ASSERT_TRUE(auth->RemoveUser("limited"));
  EXPECT_FALSE(session->IsAuthorized(
      {memgraph::query::AuthQuery::Privilege::MATCH}, "memgraph", &memgraph::query::up_to_date_policy));

  auto checker = auth_checker.GetFineGrainedAuthChecker(*session, &dba);
  ASSERT_NE(checker, nullptr);
  EXPECT_TRUE(checker->NeedsFineGrainedAuthChecker());
}
#endif
