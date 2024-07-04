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

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "query/interpreter_context.hpp"
#include "query/time_to_live/time_to_live.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace {
std::filesystem::path GetCleanDataDirectory() {
  const auto path = std::filesystem::temp_directory_path() / "ttl";
  std::filesystem::remove_all(path);
  return path;
}
}  // namespace

template <typename StorageType>
class TTLFixture : public ::testing::Test {
 protected:
  const std::string testSuite = "ttl";
  std::filesystem::path data_directory_{GetCleanDataDirectory()};
  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory_;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };

  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db_{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };
  memgraph::system::System system_state;
  memgraph::query::AllowEverythingAuthChecker auth_checker;
  memgraph::query::InterpreterContext interpreter_context_{memgraph::query::InterpreterConfig{},
                                                           nullptr,
                                                           &repl_state,
                                                           system_state,
#ifdef MG_ENTERPRISE
                                                           std::nullopt,
#endif
                                                           nullptr,
                                                           &auth_checker};

  std::optional<memgraph::query::ttl::TTL> ttl_;

  void SetUp() override {
    {
      auto acc = db_->Access();
      for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
        acc->DetachDeleteVertex({&v});
      }
      acc->Commit();
    }
    ttl_.emplace();
  }

  void TearDown() override {
    ttl_.reset();
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory_);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(TTLFixture, StorageTypes);

TYPED_TEST(TTLFixture, EnableTest) {
  EXPECT_THROW(this->ttl_->Execute({/* one-shot */}, this->db_, &this->interpreter_context_),
               memgraph::query::ttl::TtlException);
  this->ttl_->Enable();
  EXPECT_NO_THROW(this->ttl_->Execute({/* one-shot */}, this->db_, &this->interpreter_context_));
  this->ttl_->Disable();
  EXPECT_THROW(this->ttl_->Execute({/* one-shot */}, this->db_, &this->interpreter_context_),
               memgraph::query::ttl::TtlException);
}

TYPED_TEST(TTLFixture, Periodic) {
  auto lbl = this->db_->storage()->NameToLabel("L");
  auto prop = this->db_->storage()->NameToProperty("prop");
  auto ttl_lbl = this->db_->storage()->NameToLabel("TTL");
  auto ttl_prop = this->db_->storage()->NameToProperty("ttl");
  auto now = std::chrono::system_clock::now();
  auto older = now - std::chrono::seconds(10);
  auto older_ts = std::chrono::duration_cast<std::chrono::microseconds>(older.time_since_epoch()).count();
  auto newer = now + std::chrono::seconds(3);
  auto newer_ts = std::chrono::duration_cast<std::chrono::microseconds>(newer.time_since_epoch()).count();
  {
    auto acc = this->db_->Access();
    auto v1 = acc->CreateVertex();  // No label no property
    auto v2 = acc->CreateVertex();  // A label a property
    auto v3 = acc->CreateVertex();  // TTL label no ttl property
    auto v4 = acc->CreateVertex();  // No TTL label, with ttl property
    auto v5 = acc->CreateVertex();  // TTL label and ttl property (older)
    auto v6 = acc->CreateVertex();  // TTL label and ttl property (newer)
    ASSERT_FALSE(v2.AddLabel(lbl).HasError());
    ASSERT_FALSE(v2.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v3.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v4.SetProperty(ttl_prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v5.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v5.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v6.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v6.SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  EXPECT_NO_THROW(this->ttl_->Execute(memgraph::query::ttl::TtlInfo{.period = std::chrono::milliseconds(700)},
                                      this->db_, &this->interpreter_context_));
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 5);
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 4);
  }
}

TYPED_TEST(TTLFixture, StartTime) {
  auto lbl = this->db_->storage()->NameToLabel("L");
  auto prop = this->db_->storage()->NameToProperty("prop");
  auto ttl_lbl = this->db_->storage()->NameToLabel("TTL");
  auto ttl_prop = this->db_->storage()->NameToProperty("ttl");
  auto now = std::chrono::system_clock::now();
  auto older = now - std::chrono::seconds(10);
  auto older_ts = std::chrono::duration_cast<std::chrono::microseconds>(older.time_since_epoch()).count();
  auto newer = now + std::chrono::seconds(4);
  auto newer_ts = std::chrono::duration_cast<std::chrono::microseconds>(newer.time_since_epoch()).count();
  {
    auto acc = this->db_->Access();
    auto v1 = acc->CreateVertex();  // No label no property
    auto v2 = acc->CreateVertex();  // A label a property
    auto v3 = acc->CreateVertex();  // TTL label no ttl property
    auto v4 = acc->CreateVertex();  // No TTL label, with ttl property
    auto v5 = acc->CreateVertex();  // TTL label and ttl property (older)
    auto v6 = acc->CreateVertex();  // TTL label and ttl property (newer)
    ASSERT_FALSE(v2.AddLabel(lbl).HasError());
    ASSERT_FALSE(v2.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v3.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v4.SetProperty(ttl_prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v5.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v5.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v6.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v6.SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  EXPECT_NO_THROW(this->ttl_->Execute(
      memgraph::query::ttl::TtlInfo{
          .period = std::chrono::milliseconds(100),
          .start_time = std::chrono::duration_cast<std::chrono::microseconds>(
              (std::chrono::system_clock::now() + std::chrono::seconds(3)).time_since_epoch())},
      this->db_, &this->interpreter_context_));
  // Shouldn't start still
  for (int i = 0; i < 3; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    {
      auto acc = this->db_->Access();
      auto all_v = acc->Vertices(memgraph::storage::View::NEW);
      size_t size = 0;
      for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
        if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
      EXPECT_EQ(size, 6);
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(800));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 5);
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 4);
  }
}
