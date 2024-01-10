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

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

using testing::Types;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

class ShowStorageInfoTest : public testing::Test {
 public:
  const std::string testSuite = "storage_v2__show_storage_info";

  ShowStorageInfoTest() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    storage = std::make_unique<memgraph::storage::DiskStorage>(config_);
  }

  void TearDown() override {
    storage.reset(nullptr);
    disk_test_utils::RemoveRocksDbDirs(testSuite);
  }

  std::unique_ptr<Storage> storage;
  memgraph::storage::Config config_;
};

TEST_F(ShowStorageInfoTest, CountOnAbort) {
  auto acc = this->storage->Access(memgraph::replication::ReplicationRole::MAIN);
  auto src_vertex = acc->CreateVertex();
  auto dest_vertex = acc->CreateVertex();
  auto et = acc->NameToEdgeType("et5");
  auto edge = acc->CreateEdge(&src_vertex, &dest_vertex, et).GetValue();
  ASSERT_EQ(edge.EdgeType(), et);
  ASSERT_EQ(edge.FromVertex(), src_vertex);
  ASSERT_EQ(edge.ToVertex(), dest_vertex);
  memgraph::storage::StorageInfo info_before_abort = this->storage->GetBaseInfo();
  ASSERT_EQ(info_before_abort.vertex_count, 2);
  ASSERT_EQ(info_before_abort.edge_count, 1);
  acc->Abort();
  memgraph::storage::StorageInfo info_after_abort = this->storage->GetBaseInfo();
  ASSERT_EQ(info_after_abort.vertex_count, 0);
  ASSERT_EQ(info_after_abort.edge_count, 0);
}
