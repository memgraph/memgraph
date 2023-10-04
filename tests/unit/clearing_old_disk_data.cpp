// Copyright 2023 Memgraph Ltd.
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
#include <rocksdb/options.h>
#include <limits>

#include "disk_test_utils.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

class ClearingOldDiskDataTest : public ::testing::Test {
 public:
  const std::string testSuite = "clearing_old_disk_data";
  std::unique_ptr<memgraph::storage::DiskStorage> disk_storage =
      std::make_unique<memgraph::storage::DiskStorage>(disk_test_utils::GenerateOnDiskConfig(testSuite));

  void TearDown() override { disk_test_utils::RemoveRocksDbDirs(testSuite); }
};

TEST_F(ClearingOldDiskDataTest, TestNumOfEntriesWithVertexTimestampUpdate) {
  auto *tx_db = disk_storage->GetRocksDBStorage()->db_;
  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 0);

  auto acc1 = disk_storage->Access(std::nullopt);
  auto vertex1 = acc1->CreateVertex();
  auto label1 = acc1->NameToLabel("DiskLabel");
  auto property1 = acc1->NameToProperty("DiskProperty");
  ASSERT_TRUE(vertex1.AddLabel(label1).HasValue());
  ASSERT_TRUE(vertex1.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc1->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

  auto acc2 = disk_storage->Access(std::nullopt);
  auto vertex2 = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW).value();
  /// This is the same property as in the first transaction, we just want to test
  /// the number of entries inside RocksDB when the timestamp changes
  auto property2 = acc2->NameToProperty("DiskProperty");
  ASSERT_TRUE(vertex2.SetProperty(property2, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc2->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
}

TEST_F(ClearingOldDiskDataTest, TestNumOfEntriesWithVertexValueUpdate) {
  auto *tx_db = disk_storage->GetRocksDBStorage()->db_;
  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 0);

  auto acc1 = disk_storage->Access(std::nullopt);
  auto vertex1 = acc1->CreateVertex();
  auto label1 = acc1->NameToLabel("DiskLabel");
  auto property1 = acc1->NameToProperty("DiskProperty");
  ASSERT_TRUE(vertex1.AddLabel(label1).HasValue());
  ASSERT_TRUE(vertex1.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc1->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

  auto acc2 = disk_storage->Access(std::nullopt);
  auto vertex2 = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW).value();
  /// This is the same property as in the first transaction, we just want to test
  /// the number of entries inside RocksDB when the timestamp changes
  auto property2 = acc2->NameToProperty("DiskProperty");
  ASSERT_TRUE(vertex2.SetProperty(property2, memgraph::storage::PropertyValue(15)).HasValue());
  ASSERT_FALSE(acc2->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
}

TEST_F(ClearingOldDiskDataTest, TestNumOfEntriesWithVertexKeyUpdate) {
  auto *tx_db = disk_storage->GetRocksDBStorage()->db_;
  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 0);

  auto acc1 = disk_storage->Access(std::nullopt);
  auto vertex1 = acc1->CreateVertex();
  auto label1 = acc1->NameToLabel("DiskLabel");
  auto property1 = acc1->NameToProperty("DiskProperty");
  ASSERT_TRUE(vertex1.AddLabel(label1).HasValue());
  ASSERT_TRUE(vertex1.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc1->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

  auto acc2 = disk_storage->Access(std::nullopt);
  auto vertex2 = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW).value();
  auto label2 = acc2->NameToLabel("DiskLabel2");
  ASSERT_TRUE(vertex2.AddLabel(label2).HasValue());
  ASSERT_FALSE(acc2->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
}

TEST_F(ClearingOldDiskDataTest, TestNumOfEntriesWithEdgeTimestampUpdate) {
  auto *tx_db = disk_storage->GetRocksDBStorage()->db_;
  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 0);

  auto acc1 = disk_storage->Access(std::nullopt);

  auto label1 = acc1->NameToLabel("DiskLabel");
  auto property1 = acc1->NameToProperty("DiskProperty");
  auto edge_type = acc1->NameToEdgeType("test");

  auto from = acc1->CreateVertex();
  auto to = acc1->CreateVertex();
  auto edge = acc1->CreateEdge(&from, &to, edge_type);
  MG_ASSERT(edge.HasValue());

  ASSERT_TRUE(from.AddLabel(label1).HasValue());
  ASSERT_TRUE(to.AddLabel(label1).HasValue());
  ASSERT_TRUE(from.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_TRUE(to.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_TRUE(edge->SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc1->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 5);

  auto acc2 = disk_storage->Access(std::nullopt);
  auto from_vertex = acc2->FindVertex(from.Gid(), memgraph::storage::View::NEW).value();

  auto ret = from_vertex.OutEdges(memgraph::storage::View::NEW);
  auto fetched_edge = ret.GetValue().edges[0];

  /// This is the same property as in the first transaction, we just want to test
  /// the number of entries inside RocksDB when the timestamp changes
  auto property2 = acc2->NameToProperty("DiskProperty");
  ASSERT_TRUE(fetched_edge.SetProperty(property2, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc2->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 5);
}

TEST_F(ClearingOldDiskDataTest, TestNumOfEntriesWithEdgeValueUpdate) {
  auto *tx_db = disk_storage->GetRocksDBStorage()->db_;
  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 0);

  auto acc1 = disk_storage->Access(std::nullopt);

  auto label1 = acc1->NameToLabel("DiskLabel");
  auto property1 = acc1->NameToProperty("DiskProperty");
  auto edge_type = acc1->NameToEdgeType("test");

  auto from = acc1->CreateVertex();
  auto to = acc1->CreateVertex();
  auto edge = acc1->CreateEdge(&from, &to, edge_type);
  MG_ASSERT(edge.HasValue());

  ASSERT_TRUE(from.AddLabel(label1).HasValue());
  ASSERT_TRUE(to.AddLabel(label1).HasValue());
  ASSERT_TRUE(from.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_TRUE(to.SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_TRUE(edge->SetProperty(property1, memgraph::storage::PropertyValue(10)).HasValue());
  ASSERT_FALSE(acc1->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 5);

  auto acc2 = disk_storage->Access(std::nullopt);
  auto from_vertex = acc2->FindVertex(from.Gid(), memgraph::storage::View::NEW).value();

  auto ret = from_vertex.OutEdges(memgraph::storage::View::NEW);
  auto fetched_edge = ret.GetValue().edges[0];

  auto property2 = acc2->NameToProperty("DiskProperty");
  ASSERT_TRUE(fetched_edge.SetProperty(property2, memgraph::storage::PropertyValue(15)).HasValue());
  ASSERT_FALSE(acc2->Commit().HasError());

  ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 5);
}
