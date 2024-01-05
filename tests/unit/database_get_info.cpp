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
#include <optional>

#include "dbms/database.hpp"
#include "dbms/dbms_handler.hpp"
#include "disk_test_utils.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/replication/enums.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

constexpr auto testSuite = "database_v2_get_info";
const std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / testSuite};

struct TestConfig {};
struct DefaultConfig : TestConfig {};
struct TenantConfig : TestConfig {};

template <typename TestType>
class InfoTest : public testing::Test {
  using StorageType = typename TestType::first_type;
  using ConfigType = typename TestType::second_type;

 protected:
  void SetUp() {
#ifdef MG_ENTERPRISE
    dbms_handler_.emplace(config, &auth, false);
    auto db_acc = dbms_handler_->Get();  // Default db
    if (std::is_same_v<ConfigType, TenantConfig>) {
      constexpr std::string_view db_name = "test_db";
      MG_ASSERT(dbms_handler_->New(std::string{db_name}).HasValue(), "Failed to create database.");
      db_acc = dbms_handler_->Get(db_name);
    }
#else
    dbms_handler_.emplace(config);
    auto db_acc = dbms_handler_->Get();
#endif
    MG_ASSERT(db_acc, "Failed to access db");
    MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
              "Wrong storage mode!");
    db_acc_ = std::move(db_acc);
  }

  void TearDown() {
    db_acc_.reset();
    dbms_handler_.reset();
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(storage_directory);
  }

  StorageMode mode{std::is_same_v<StorageType, DiskStorage> ? StorageMode::ON_DISK_TRANSACTIONAL
                                                            : StorageMode::IN_MEMORY_TRANSACTIONAL};

  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;
  std::optional<memgraph::dbms::DbmsHandler> dbms_handler_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> auth{
      storage_directory, memgraph::auth::Auth::Config{}};
  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        memgraph::storage::UpdatePaths(config, storage_directory);
        config.durability.snapshot_wal_mode =
            memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };
};

using TestTypes = ::testing::Types<std::pair<memgraph::storage::InMemoryStorage, DefaultConfig>,
                                   std::pair<memgraph::storage::DiskStorage, DefaultConfig>

#ifdef MG_ENTERPRISE
                                   ,
                                   std::pair<memgraph::storage::InMemoryStorage, TenantConfig>,
                                   std::pair<memgraph::storage::DiskStorage, TenantConfig>
#endif
                                   >;

TYPED_TEST_CASE(InfoTest, TestTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(InfoTest, InfoCheck) {
  auto &db_acc = *this->db_acc_;
  auto lbl = db_acc->storage()->NameToLabel("label");
  auto lbl2 = db_acc->storage()->NameToLabel("abc");
  auto lbl3 = db_acc->storage()->NameToLabel("3");
  auto prop = db_acc->storage()->NameToProperty("prop");
  auto prop2 = db_acc->storage()->NameToProperty("another prop");

  {
    {
      auto unique_acc = db_acc->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateExistenceConstraint(lbl, prop).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      auto unique_acc = db_acc->UniqueAccess();
      ASSERT_FALSE(unique_acc->DropExistenceConstraint(lbl, prop).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    auto acc = db_acc->Access();
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
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(lbl).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(lbl, prop).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(lbl, prop2).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->DropIndex(lbl, prop).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateUniqueConstraint(lbl, {prop2}).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateUniqueConstraint(lbl2, {prop}).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateUniqueConstraint(lbl3, {prop}).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = db_acc->UniqueAccess();
    ASSERT_EQ(unique_acc->DropUniqueConstraint(lbl, {prop2}),
              memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  const auto &info = db_acc->GetInfo(
      memgraph::replication_coordination_glue::ReplicationRole::MAIN);  // force to use configured directory

  ASSERT_EQ(info.storage_info.vertex_count, 5);
  ASSERT_EQ(info.storage_info.edge_count, 2);
  ASSERT_EQ(info.storage_info.average_degree, 0.8);
  ASSERT_GT(info.storage_info.memory_res, 10'000'000);  // 250MB < > 10MB
  ASSERT_LT(info.storage_info.memory_res, 250'000'000);
  ASSERT_GT(info.storage_info.disk_usage, 100);  // 1MB < > 100B
  ASSERT_LT(info.storage_info.disk_usage, 1000'000);
  ASSERT_EQ(info.storage_info.label_indices, 1);
  ASSERT_EQ(info.storage_info.label_property_indices, 1);
  ASSERT_EQ(info.storage_info.existence_constraints, 0);
  ASSERT_EQ(info.storage_info.unique_constraints, 2);
  ASSERT_EQ(info.storage_info.storage_mode, this->mode);
  ASSERT_EQ(info.storage_info.isolation_level, IsolationLevel::SNAPSHOT_ISOLATION);
  ASSERT_EQ(info.storage_info.durability_snapshot_enabled, true);
  ASSERT_EQ(info.storage_info.durability_wal_enabled, true);

  ASSERT_EQ(info.triggers, 0);
  ASSERT_EQ(info.streams, 0);
}
