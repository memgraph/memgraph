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
#include <filesystem>

#include "dbms/constants.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;
using memgraph::replication_coordination_glue::ReplicationRole;
constexpr auto testSuite = "storage_v2_get_info";
const std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / testSuite};

template <typename StorageType>
class InfoTest : public testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(storage_directory);
    config_.salient.name = memgraph::dbms::kDefaultDB;
    memgraph::storage::UpdatePaths(config_, storage_directory);
    config_.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    this->storage = std::make_unique<StorageType>(config_);
  }

  void TearDown() override {
    std::filesystem::remove_all(storage_directory);
    this->storage.reset(nullptr);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  StorageMode mode{std::is_same_v<StorageType, DiskStorage> ? StorageMode::ON_DISK_TRANSACTIONAL
                                                            : StorageMode::IN_MEMORY_TRANSACTIONAL};
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;

TYPED_TEST_CASE(InfoTest, StorageTypes);
// TYPED_TEST_CASE(IndexTest, InMemoryStorageType);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(InfoTest, InfoCheck) {
  auto lbl = this->storage->NameToLabel("label");
  auto lbl2 = this->storage->NameToLabel("abc");
  auto lbl3 = this->storage->NameToLabel("3");
  auto prop = this->storage->NameToProperty("prop");
  auto prop2 = this->storage->NameToProperty("another prop");

  {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      ASSERT_FALSE(unique_acc->CreateExistenceConstraint(lbl, prop).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      ASSERT_FALSE(unique_acc->DropExistenceConstraint(lbl, prop).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    auto acc = this->storage->Access(ReplicationRole::MAIN);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    auto v4 = acc->CreateVertex();
    [[maybe_unused]] auto v5 = acc->CreateVertex();

    ASSERT_FALSE(v2.AddLabel(lbl).HasError());
    ASSERT_FALSE(v3.AddLabel(lbl).HasError());
    ASSERT_FALSE(v3.SetProperty(prop, PropertyValue(42)).HasError());
    ASSERT_FALSE(v4.AddLabel(lbl).HasError());

    auto et = acc->NameToEdgeType("et5");
    ASSERT_FALSE(acc->CreateEdge(&v1, &v2, et).HasError());
    ASSERT_FALSE(acc->CreateEdge(&v4, &v3, et).HasError());

    ASSERT_FALSE(acc->Commit().HasError());
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->CreateIndex(lbl).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->CreateIndex(lbl, prop).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->CreateIndex(lbl, prop2).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->DropIndex(lbl, prop).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->CreateUniqueConstraint(lbl, {prop2}).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->CreateUniqueConstraint(lbl2, {prop}).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_FALSE(unique_acc->CreateUniqueConstraint(lbl3, {prop}).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    ASSERT_EQ(unique_acc->DropUniqueConstraint(lbl, {prop2}),
              memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  StorageInfo info = this->storage->GetInfo(ReplicationRole::MAIN);

  ASSERT_EQ(info.vertex_count, 5);
  ASSERT_EQ(info.edge_count, 2);
  ASSERT_EQ(info.average_degree, 0.8);
  ASSERT_GT(info.memory_res, 10'000'000);  // 200MB < > 10MB
  ASSERT_LT(info.memory_res, 200'000'000);
  ASSERT_GT(info.disk_usage, 100);  // 1MB < > 100B
  ASSERT_LT(info.disk_usage, 1000'000);
  ASSERT_EQ(info.label_indices, 1);
  ASSERT_EQ(info.label_property_indices, 1);
  ASSERT_EQ(info.text_indices, 0);
  ASSERT_EQ(info.existence_constraints, 0);
  ASSERT_EQ(info.unique_constraints, 2);
  ASSERT_EQ(info.storage_mode, this->mode);
  ASSERT_EQ(info.isolation_level, IsolationLevel::SNAPSHOT_ISOLATION);
  ASSERT_EQ(info.durability_snapshot_enabled, true);
  ASSERT_EQ(info.durability_wal_enabled, true);
}
