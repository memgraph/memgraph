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

#include "auth/models.hpp"
#include "disk_test_utils.hpp"
#include "glue/auth_checker.hpp"

#include "license/license.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
using memgraph::replication::ReplicationRole;
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
#endif
