// Copyright 2024 Memgraph Ltd.
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

#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "disk_test_utils.hpp"
#include "glue/auth_checker.hpp"

#include "license/license.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
using memgraph::replication_coordination_glue::ReplicationRole;
#ifdef MG_ENTERPRISE
template <typename StorageType>
class FineGrainedAuthCheckerFixture : public testing::Test {
 protected:
  const std::string testSuite = "auth_checker";

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new StorageType(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{db->Access(ReplicationRole::MAIN)};
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
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).HasValue());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).HasValue());
    ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l3")).HasValue());
    dba.AdvanceCommand();
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(FineGrainedAuthCheckerFixture, StorageTypes);

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantedAllLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

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
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r2, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DeniedAllLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);
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
  user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r2, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantLabel) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l1",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(
      auth_checker.Has(this->v1, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DenyLabel) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::NOTHING);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::NEW, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(
      auth_checker.Has(this->v3, memgraph::storage::View::OLD, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantAndDenySpecificLabels) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l1",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("l2",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::NOTHING);
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
  user.fine_grained_access_handler().label_permissions().Grant("l1",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("l2",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::NOTHING);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};
  ASSERT_TRUE(this->v1.AddLabel(this->dba.NameToLabel("l3")).HasValue());
  ASSERT_TRUE(this->v2.AddLabel(this->dba.NameToLabel("l1")).HasValue());
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
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "edge_type_1", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, DenyEdgeType) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                   memgraph::auth::FineGrainedPermission::NOTHING);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_FALSE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
}

TYPED_TEST(FineGrainedAuthCheckerFixture, GrantAndDenySpecificEdgeTypes) {
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "edge_type_1", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                   memgraph::auth::FineGrainedPermission::NOTHING);
  memgraph::glue::FineGrainedAuthChecker auth_checker{user, &this->dba};

  ASSERT_TRUE(auth_checker.Has(this->r1, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_TRUE(auth_checker.Has(this->r2, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r3, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(auth_checker.Has(this->r4, memgraph::query::AuthQuery::FineGrainedPrivilege::READ));
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

  auto empty_user = auth_checker.GenQueryUser(std::nullopt, std::nullopt);
  ASSERT_THROW(auth_checker.GenQueryUser("does_not_exist", std::nullopt), memgraph::auth::AuthException);

  EXPECT_FALSE(empty_user && *empty_user);
  // Still empty auth, so the above should have su permissions
  using enum memgraph::query::AuthQuery::Privilege;
  EXPECT_TRUE(empty_user->IsAuthorized({AUTH, REMOVE, REPLICATION}, ""));
  EXPECT_TRUE(empty_user->IsAuthorized({FREE_MEMORY, WEBSOCKET, MULTI_DATABASE_EDIT}, "memgraph"));
  EXPECT_TRUE(empty_user->IsAuthorized({TRIGGER, DURABILITY, STORAGE_MODE}, "some_db"));

  // Add user
  auth->AddUser("new_user");

  // ~Empty user should now fail~
  // NOTE: Cache invalidation has been disabled, so this will pass; change if it is ever turned on
  EXPECT_TRUE(empty_user->IsAuthorized({AUTH, REMOVE, REPLICATION}, ""));
  EXPECT_TRUE(empty_user->IsAuthorized({FREE_MEMORY, WEBSOCKET, MULTI_DATABASE_EDIT}, "memgraph"));
  EXPECT_TRUE(empty_user->IsAuthorized({TRIGGER, DURABILITY, STORAGE_MODE}, "some_db"));

  // Add role and new user
  auto new_role = *auth->AddRole("new_role");
  auto new_user2 = *auth->AddUser("new_user2");
  auto role = auth_checker.GenQueryUser("anyuser", "new_role");
  auto user2 = auth_checker.GenQueryUser("new_user2", std::nullopt);

  // Should be permission-less by default
  EXPECT_FALSE(role->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_FALSE(role->IsAuthorized({FREE_MEMORY}, "memgraph"));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "memgraph"));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_FALSE(user2->IsAuthorized({FREE_MEMORY}, "memgraph"));
  EXPECT_FALSE(user2->IsAuthorized({TRIGGER}, "memgraph"));

  // Update permissions and recheck
  new_user2.permissions().Grant(memgraph::auth::Permission::AUTH);
  new_role.permissions().Grant(memgraph::auth::Permission::TRIGGER);
  auth->SaveUser(new_user2);
  auth->SaveRole(new_role);
  // NOTE: Cache invalidation is disabled, so we need to generate a new user every time;
  role = auth_checker.GenQueryUser("no check", "new_role");
  user2 = auth_checker.GenQueryUser("new_user2", std::nullopt);
  EXPECT_FALSE(role->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_FALSE(role->IsAuthorized({FREE_MEMORY}, "memgraph"));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph"));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_FALSE(user2->IsAuthorized({FREE_MEMORY}, "memgraph"));
  EXPECT_FALSE(user2->IsAuthorized({TRIGGER}, "memgraph"));

  // Connect role and recheck
  new_user2.SetRole(new_role);
  auth->SaveUser(new_user2);
  // NOTE: Cache invalidation is disabled, so we need to generate a new user every time;
  user2 = auth_checker.GenQueryUser("new_user2", std::nullopt);
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_FALSE(user2->IsAuthorized({FREE_MEMORY}, "memgraph"));
  EXPECT_TRUE(user2->IsAuthorized({TRIGGER}, "memgraph"));

  // Add database and recheck
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "non_default"));
  EXPECT_FALSE(user2->IsAuthorized({AUTH}, "another"));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "non_default"));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another"));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph"));
  new_user2.db_access().Add("another");
  new_role.db_access().Add("non_default");
  auth->SaveUser(new_user2);
  auth->SaveRole(new_role);
  // NOTE: Cache invalidation is disabled, so we need to generate a new user every time;
  role = auth_checker.GenQueryUser("just tags", "new_role");
  user2 = auth_checker.GenQueryUser("new_user2", std::nullopt);
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "non_default"));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "another"));
  EXPECT_TRUE(user2->IsAuthorized({AUTH}, "memgraph"));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "non_default"));
  EXPECT_FALSE(role->IsAuthorized({TRIGGER}, "another"));
  EXPECT_TRUE(role->IsAuthorized({TRIGGER}, "memgraph"));
}
#endif
