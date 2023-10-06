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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <limits>

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"

using testing::Types;
using testing::UnorderedElementsAre;

template <typename StorageType>
class StorageV2Test : public testing::Test {
 public:
  StorageV2Test() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    store = std::make_unique<StorageType>(config_);
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    store.reset(nullptr);
  }

  const std::string testSuite = "storage_v2";
  std::unique_ptr<memgraph::storage::Storage> store;
  memgraph::storage::Config config_;
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(StorageV2Test, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, Commit) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    acc->Abort();
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc->DeleteVertex(&*vertex);
    ASSERT_FALSE(res.HasError());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);

    acc->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, Abort) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    acc->Abort();
  }
  {
    auto acc = this->store->Access();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, AdvanceCommandCommit) {
  memgraph::storage::Gid gid1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();

    auto vertex1 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);

    acc->AdvanceCommand();

    auto vertex2 = acc->CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    ASSERT_TRUE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 2U);

    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    ASSERT_TRUE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 2U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 2U);
    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, AdvanceCommandAbort) {
  memgraph::storage::Gid gid1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();

    auto vertex1 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);

    acc->AdvanceCommand();

    auto vertex2 = acc->CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    ASSERT_TRUE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 2U);

    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());

    acc->Abort();
  }
  {
    auto acc = this->store->Access();
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, SnapshotIsolation) {
  auto acc1 = this->store->Access();
  auto acc2 = this->store->Access();

  auto vertex = acc1->CreateVertex();
  auto gid = vertex.Gid();

  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 1U);
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 0U);

  ASSERT_FALSE(acc1->Commit().HasError());

  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 0U);

  acc2->Abort();

  auto acc3 = this->store->Access();
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::NEW), 1U);
  acc3->Abort();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, AccessorMove) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);

    auto moved(std::move(acc));

    ASSERT_FALSE(moved->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*moved, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(moved->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*moved, memgraph::storage::View::NEW), 1U);

    ASSERT_FALSE(moved->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteCommit) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  auto acc1 = this->store->Access();  // read transaction
  auto acc2 = this->store->Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    ASSERT_FALSE(acc2->Commit().HasError());
  }

  auto acc3 = this->store->Access();  // read transaction
  auto acc4 = this->store->Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::NEW), 1U);

  // Delete the vertex in transaction 4
  {
    auto vertex = acc4->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 1U);

    auto res = acc4->DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    acc4->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    ASSERT_FALSE(acc4->Commit().HasError());
  }

  auto acc5 = this->store->Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_FALSE(acc5->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc5, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc5->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc5, memgraph::storage::View::NEW), 0U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteAbort) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = this->store->Access();  // read transaction
  auto acc2 = this->store->Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    ASSERT_FALSE(acc2->Commit().HasError());
  }

  auto acc3 = this->store->Access();  // read transaction
  auto acc4 = this->store->Access();  // write transaction (aborted)

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::NEW), 1U);

  // Delete the vertex in transaction 4, but abort the transaction
  {
    auto vertex = acc4->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 1U);

    auto res = acc4->DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    acc4->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    acc4->Abort();
  }

  auto acc5 = this->store->Access();  // read transaction
  auto acc6 = this->store->Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc5, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc5->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc5, memgraph::storage::View::NEW), 1U);

  // Delete the vertex in transaction 6
  {
    auto vertex = acc6->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::NEW), 1U);

    auto res = acc6->DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::NEW), 0U);

    acc6->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::NEW), 0U);

    ASSERT_FALSE(acc6->Commit().HasError());
  }

  auto acc7 = this->store->Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc3, memgraph::storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc5, memgraph::storage::View::OLD), 1U);
  ASSERT_TRUE(acc5->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc5, memgraph::storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 7
  ASSERT_FALSE(acc7->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc7, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc7->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc7, memgraph::storage::View::NEW), 0U);

  // Commit all accessors
  ASSERT_FALSE(acc1->Commit().HasError());
  ASSERT_FALSE(acc3->Commit().HasError());
  ASSERT_FALSE(acc5->Commit().HasError());
  ASSERT_FALSE(acc7->Commit().HasError());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteSerializationError) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  auto acc1 = this->store->Access();
  auto acc2 = this->store->Access();

  // Delete vertex in accessor 1
  {
    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 1U);

    {
      auto res = acc1->DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
      EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 1U);
      EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);
    }

    {
      auto res = acc1->DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
      EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 1U);
      EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);
    }

    acc1->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);
  }

  // Delete vertex in accessor 2
  {
    auto vertex = acc2->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    auto res = acc2->DeleteVertex(&*vertex);
    if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
      // Serialization error for disk will be on commit
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), memgraph::storage::Error::SERIALIZATION_ERROR);
    }

    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 1U);
    if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
      // Beucase of pessimistic Serialization error happened on DeleteVertex() function
      EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    } else {
      EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 0U);
    }

    acc2->AdvanceCommand();
    if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
      EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 1U);
      EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    } else {
      EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
      EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 0U);
    }
  }

  // Finalize both accessors
  ASSERT_FALSE(acc1->Commit().HasError());
  if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    acc2->Abort();
  } else {
    auto res = acc2->Commit();
    ASSERT_TRUE(res.HasError());
    ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.GetError()), memgraph::storage::SerializationError());
  }

  // Check whether the vertex exists
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_FALSE(vertex);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteSpecialCases) {
  memgraph::storage::Gid gid1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex and delete it in the same transaction, but abort the
  // transaction
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid1 = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    auto res = acc->DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid2 = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    auto res = acc->DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the vertices exist
  {
    auto acc = this->store->Access();
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteLabel) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Add label, delete the vertex and check the label API (same command)
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }

  // Add label, delete the vertex and check the label API (different command)
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Advance command
    acc->AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);

    // Advance command
    acc->AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::OLD).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteProperty) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Set property, delete the vertex and check the property API (same command)
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetProperty(property, memgraph::storage::PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, memgraph::storage::PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }

  // Set property, delete the vertex and check the property API (different
  // command)
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetProperty(property, memgraph::storage::PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Advance command
    acc->AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).GetValue());

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);

    // Advance command
    acc->AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, memgraph::storage::PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelCommit) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc->Commit().HasError());
    spdlog::debug("Commit done");
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).GetValue());

    acc->Abort();
    spdlog::debug("Abort done");
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc->Commit().HasError());
    spdlog::debug("Commit done");
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).GetValue());

    acc->Abort();
    spdlog::debug("Abort done");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelAbort) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Add label 5, but abort the transaction.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc->Abort();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).GetValue());

    acc->Abort();
  }

  // Add label 5.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check that label 5 exists.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).GetValue());

    acc->Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc->Abort();
  }

  // Check that label 5 exists.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).GetValue());

    acc->Abort();
  }

  // Remove label 5.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).GetValue());

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelSerializationError) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  auto acc1 = this->store->Access();
  auto acc2 = this->store->Access();

  // Add label 1 in accessor 1.
  {
    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc1->NameToLabel("label1");
    auto label2 = acc1->NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label1, memgraph::storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }
  }

  // Add label 2 in accessor 2.
  {
    auto vertex = acc2->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc2->NameToLabel("label1");
    auto label2 = acc2->NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label2);
      if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
        // InMemoryStorage works with pessimistic transactions.
        ASSERT_TRUE(res.HasError());
        ASSERT_EQ(res.GetError(), memgraph::storage::Error::SERIALIZATION_ERROR);
      } else {
        // Disk storage works with optimistic transactions.
        ASSERT_TRUE(res.HasValue());
        ASSERT_TRUE(res.GetValue());
      }
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1->Commit().HasError());
  if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    acc2->Abort();
  } else {
    // Disk storage works with optimistic transactions. So on write conflict, transaction fails on commit.
    auto res = acc2->Commit();
    ASSERT_TRUE(res.HasError());
    ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.GetError()), memgraph::storage::SerializationError());
  }

  // Check which labels exist.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc->NameToLabel("label1");
    auto label2 = acc->NameToLabel("label2");

    ASSERT_TRUE(vertex->HasLabel(label1, memgraph::storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    ASSERT_TRUE(vertex->HasLabel(label1, memgraph::storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexPropertyCommit) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexPropertyAbort) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc->Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    acc->Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }

  // Set property 5 to null.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check that property 5 is null.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexPropertySerializationError) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  auto acc1 = this->store->Access();
  auto acc2 = this->store->Access();

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc1->NameToProperty("property1");
    auto property2 = acc1->NameToProperty("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property1, memgraph::storage::PropertyValue(123));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc2->NameToProperty("property1");
    auto property2 = acc2->NameToProperty("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(property2, memgraph::storage::PropertyValue("nandare"));
      if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
        // InMemoryStorage works with pessimistic transactions.
        ASSERT_TRUE(res.HasError());
        ASSERT_EQ(res.GetError(), memgraph::storage::Error::SERIALIZATION_ERROR);
      } else {
        // Disk storage works with optimistic transactions.
        ASSERT_TRUE(res.HasValue());
        ASSERT_TRUE(res->IsNull());
      }
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1->Commit().HasError());
  if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    acc2->Abort();
  } else {
    // Disk storage works with optimistic transactions. So on write conflict, transaction fails on commit.
    auto res = acc2->Commit();
    ASSERT_TRUE(res.HasError());
    ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.GetError()), memgraph::storage::SerializationError());
  }

  // Check which properties exist.
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc->NameToProperty("property1");
    auto property2 = acc->NameToProperty("property2");

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelPropertyMixed) {
  auto acc = this->store->Access();
  auto vertex = acc->CreateVertex();

  auto label = acc->NameToLabel("label5");
  auto property = acc->NameToProperty("property5");

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Add label 5
  ASSERT_TRUE(vertex.AddLabel(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Set property 5 to "nandare"
  ASSERT_TRUE(vertex.SetProperty(property, memgraph::storage::PropertyValue("nandare"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD)->size(), 0);
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Set property 5 to "haihai"
  ASSERT_FALSE(vertex.SetProperty(property, memgraph::storage::PropertyValue("haihai"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Remove label 5
  ASSERT_TRUE(vertex.RemoveLabel(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Set property 5 to null
  ASSERT_FALSE(vertex.SetProperty(property, memgraph::storage::PropertyValue())->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  ASSERT_FALSE(acc->Commit().HasError());
}

TYPED_TEST(StorageV2Test, VertexPropertyClear) {
  memgraph::storage::Gid gid;
  auto property1 = this->store->NameToProperty("property1");
  auto property2 = this->store->NameToProperty("property2");
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    auto old_value = vertex.SetProperty(property1, memgraph::storage::PropertyValue("value"));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::OLD)->ValueString(), "value");
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_THAT(vertex->Properties(memgraph::storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value"))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    acc->Abort();
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto old_value = vertex->SetProperty(property2, memgraph::storage::PropertyValue(42));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::OLD)->ValueString(), "value");
    ASSERT_EQ(vertex->GetProperty(property2, memgraph::storage::View::OLD)->ValueInt(), 42);
    ASSERT_THAT(vertex->Properties(memgraph::storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value")),
                                     std::pair(property2, memgraph::storage::PropertyValue(42))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = this->store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    acc->Abort();
  }
}

TYPED_TEST(StorageV2Test, VertexNonexistentLabelPropertyEdgeAPI) {
  auto label = this->store->NameToLabel("label");
  auto property = this->store->NameToProperty("property");

  auto acc = this->store->Access();
  auto vertex = acc->CreateVertex();

  // Check state before (OLD view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, memgraph::storage::View::OLD).GetError(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD).GetError(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);

  // Check state before (NEW view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.HasLabel(label, memgraph::storage::View::NEW), false);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.GetProperty(property, memgraph::storage::View::NEW), memgraph::storage::PropertyValue());
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
  ASSERT_EQ(*vertex.InDegree(memgraph::storage::View::NEW), 0);
  ASSERT_EQ(*vertex.OutDegree(memgraph::storage::View::NEW), 0);

  // Modify vertex.
  ASSERT_TRUE(vertex.AddLabel(label).HasValue());
  ASSERT_TRUE(vertex.SetProperty(property, memgraph::storage::PropertyValue("value")).HasValue());
  ASSERT_TRUE(acc->CreateEdge(&vertex, &vertex, acc->NameToEdgeType("edge")).HasValue());

  // Check state after (OLD view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, memgraph::storage::View::OLD).GetError(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD).GetError(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.HasLabel(label, memgraph::storage::View::NEW), true);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.GetProperty(property, memgraph::storage::View::NEW), memgraph::storage::PropertyValue("value"));
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::NEW)->edges.size(), 1);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::NEW)->edges.size(), 1);
  ASSERT_EQ(*vertex.InDegree(memgraph::storage::View::NEW), 1);
  ASSERT_EQ(*vertex.OutDegree(memgraph::storage::View::NEW), 1);

  ASSERT_FALSE(acc->Commit().HasError());
}

TYPED_TEST(StorageV2Test, VertexVisibilitySingleTransaction) {
  auto acc1 = this->store->Access();
  auto acc2 = this->store->Access();

  auto vertex = acc1->CreateVertex();
  auto gid = vertex.Gid();

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

  ASSERT_TRUE(vertex.AddLabel(acc1->NameToLabel("label")).HasValue());

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

  ASSERT_TRUE(vertex.SetProperty(acc1->NameToProperty("meaning"), memgraph::storage::PropertyValue(42)).HasValue());

  auto acc3 = this->store->Access();

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

  ASSERT_TRUE(acc1->DeleteVertex(&vertex).HasValue());

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

  acc1->AdvanceCommand();
  acc3->AdvanceCommand();

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

  acc1->Abort();
  acc2->Abort();
  acc3->Abort();
}

TYPED_TEST(StorageV2Test, VertexVisibilityMultipleTransactions) {
  memgraph::storage::Gid gid;

  {
    auto acc1 = this->store->Access();
    auto acc2 = this->store->Access();

    auto vertex = acc1->CreateVertex();
    gid = vertex.Gid();

    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    acc2->AdvanceCommand();

    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    acc1->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    ASSERT_FALSE(acc1->Commit().HasError());
    ASSERT_FALSE(acc2->Commit().HasError());
  }

  {
    auto acc1 = this->store->Access();
    auto acc2 = this->store->Access();

    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    ASSERT_TRUE(vertex->AddLabel(acc1->NameToLabel("label")).HasValue());

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    acc1->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    acc2->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    ASSERT_TRUE(vertex->SetProperty(acc1->NameToProperty("meaning"), memgraph::storage::PropertyValue(42)).HasValue());

    auto acc3 = this->store->Access();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc1->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc2->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc3->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    ASSERT_FALSE(acc1->Commit().HasError());
    ASSERT_FALSE(acc2->Commit().HasError());
    ASSERT_FALSE(acc3->Commit().HasError());
  }

  {
    auto acc1 = this->store->Access();
    auto acc2 = this->store->Access();

    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1->DeleteVertex(&*vertex).HasValue());

    auto acc3 = this->store->Access();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc2->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc1->AdvanceCommand();

    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc3->AdvanceCommand();

    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc1->Abort();
    acc2->Abort();
    acc3->Abort();
  }

  {
    auto acc = this->store->Access();

    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW));

    acc->AdvanceCommand();

    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW));

    acc->Abort();
  }

  {
    auto acc1 = this->store->Access();
    auto acc2 = this->store->Access();

    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1->DeleteVertex(&*vertex).HasValue());

    auto acc3 = this->store->Access();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc2->AdvanceCommand();

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc1->AdvanceCommand();

    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    acc3->AdvanceCommand();

    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

    ASSERT_FALSE(acc1->Commit().HasError());
    ASSERT_FALSE(acc2->Commit().HasError());
    ASSERT_FALSE(acc3->Commit().HasError());
  }

  {
    auto acc = this->store->Access();

    EXPECT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc->FindVertex(gid, memgraph::storage::View::NEW));

    acc->AdvanceCommand();

    EXPECT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_FALSE(acc->FindVertex(gid, memgraph::storage::View::NEW));

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, DeletedVertexAccessor) {
  const auto property = this->store->NameToProperty("property");
  const memgraph::storage::PropertyValue property_value{"property_value"};

  std::optional<memgraph::storage::Gid> gid;
  // Create the vertex
  {
    auto acc = this->store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(vertex.SetProperty(property, property_value).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  auto acc = this->store->Access();
  auto vertex = acc->FindVertex(*gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(vertex);
  auto maybe_deleted_vertex = acc->DeleteVertex(&*vertex);
  ASSERT_FALSE(maybe_deleted_vertex.HasError());

  auto deleted_vertex = maybe_deleted_vertex.GetValue();
  ASSERT_TRUE(deleted_vertex);
  // you cannot modify deleted vertex
  ASSERT_TRUE(deleted_vertex->ClearProperties().HasError());

  // you can call read only methods
  const auto maybe_property = deleted_vertex->GetProperty(property, memgraph::storage::View::OLD);
  ASSERT_FALSE(maybe_property.HasError());
  ASSERT_EQ(property_value, *maybe_property);
  ASSERT_FALSE(acc->Commit().HasError());

  {
    // you can call read only methods and get valid results even after the
    // transaction which deleted the vertex committed, but only if the transaction
    // accessor is still alive
    const auto maybe_property = deleted_vertex->GetProperty(property, memgraph::storage::View::OLD);
    ASSERT_FALSE(maybe_property.HasError());
    ASSERT_EQ(property_value, *maybe_property);
  }
}
