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

#include <filesystem>
#include <limits>

#include "disk_test_utils.hpp"
#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/park_state.hpp"
#include "utils/worker_resume_event.hpp"

using testing::Types;
using testing::UnorderedElementsAre;

template <typename StorageType>
class StorageV2Test : public testing::Test {
 public:
  StorageV2Test() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    config_.track_label_counts = true;
    store = std::make_unique<StorageType>(config_);
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    store.reset(nullptr);
  }

  const std::string testSuite = "storage_v2";
  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> store;
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(StorageV2Test, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, Commit) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    acc->Abort();
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc->DeleteVertex(&*vertex);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);

    acc->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    acc->Abort();
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);

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

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);

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
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
  auto acc1 = this->store->Access(memgraph::storage::WRITE);
  auto acc2 = this->store->Access(memgraph::storage::WRITE);

  auto vertex = acc1->CreateVertex();
  auto gid = vertex.Gid();

  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 0U);
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 1U);
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 0U);

  ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
  ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 0U);

  acc2->Abort();

  auto acc3 = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
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

    ASSERT_TRUE(moved->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
  auto acc1 = this->store->Access(memgraph::storage::WRITE);  // read transaction
  auto acc2 = this->store->Access(memgraph::storage::WRITE);  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc3 = this->store->Access(memgraph::storage::WRITE);  // read transaction
  auto acc4 = this->store->Access(memgraph::storage::WRITE);  // write transaction

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
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    acc4->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    ASSERT_TRUE(acc4->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc5 = this->store->Access(memgraph::storage::WRITE);  // read transaction

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

  auto acc1 = this->store->Access(memgraph::storage::WRITE);  // read transaction
  auto acc2 = this->store->Access(memgraph::storage::WRITE);  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc2, memgraph::storage::View::NEW), 1U);
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc3 = this->store->Access(memgraph::storage::WRITE);  // read transaction
  auto acc4 = this->store->Access(memgraph::storage::WRITE);  // write transaction (aborted)

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
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    acc4->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc4, memgraph::storage::View::NEW), 0U);

    acc4->Abort();
  }

  auto acc5 = this->store->Access(memgraph::storage::WRITE);  // read transaction
  auto acc6 = this->store->Access(memgraph::storage::WRITE);  // write transaction

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
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::NEW), 0U);

    acc6->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc6, memgraph::storage::View::NEW), 0U);

    ASSERT_TRUE(acc6->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc7 = this->store->Access(memgraph::storage::WRITE);  // read transaction

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
  ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  ASSERT_TRUE(acc5->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  ASSERT_TRUE(acc7->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteSerializationError) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc1 = this->store->Access(memgraph::storage::WRITE);
  auto acc2 = this->store->Access(memgraph::storage::WRITE);

  // Delete vertex in accessor 1
  {
    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 1U);

    {
      auto res = acc1->DeleteVertex(&*vertex);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
      EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::OLD), 1U);
      EXPECT_EQ(CountVertices(*acc1, memgraph::storage::View::NEW), 0U);
    }

    {
      auto res = acc1->DeleteVertex(&*vertex);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
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
      ASSERT_FALSE(res.has_value());
      ASSERT_EQ(res.error(), memgraph::storage::Error::SERIALIZATION_ERROR);
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
  ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    acc2->Abort();
  } else {
    auto res = acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.error()), memgraph::storage::SerializationError());
  }

  // Check whether the vertex exists
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_FALSE(vertex);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteSpecialCases) {
  memgraph::storage::Gid gid1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex and delete it in the same transaction, but abort the
  // transaction
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid1 = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid1, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid1, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    auto res = acc->DeleteVertex(&vertex);
    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(res.value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid2 = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid2, memgraph::storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    ASSERT_TRUE(acc->FindVertex(gid2, memgraph::storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 1U);
    auto res = acc->DeleteVertex(&vertex);
    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(res.value());
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    acc->AdvanceCommand();
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(*acc, memgraph::storage::View::NEW), 0U);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Check whether the vertices exist
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Add label, delete the vertex and check the label API (same command)
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).value());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).value());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_FALSE(ret.has_value());
      ASSERT_EQ(ret.error(), memgraph::storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_FALSE(ret.has_value());
      ASSERT_EQ(ret.error(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }

  // Add label, delete the vertex and check the label API (different command)
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).value());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Advance command
    acc->AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).value());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);

    // Advance command
    acc->AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::OLD).error(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->HasLabel(label, memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD).error(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_FALSE(ret.has_value());
      ASSERT_EQ(ret.error(), memgraph::storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_FALSE(ret.has_value());
      ASSERT_EQ(ret.error(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexDeleteProperty) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD).has_value());
    ASSERT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Set property, delete the vertex and check the property API (same command)
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).value());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW).error(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, memgraph::storage::PropertyValue("haihai"));
      ASSERT_FALSE(ret.has_value());
      ASSERT_EQ(ret.error(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }

  // Set property, delete the vertex and check the property API (different
  // command)
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Advance command
    acc->AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).value());

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW).error(),
              memgraph::storage::Error::DELETED_OBJECT);
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);

    // Advance command
    acc->AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD).error(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW).error(),
              memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD).error(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).error(), memgraph::storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, memgraph::storage::PropertyValue("haihai"));
      ASSERT_FALSE(ret.has_value());
      ASSERT_EQ(ret.error(), memgraph::storage::Error::DELETED_OBJECT);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelCommit) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    spdlog::debug("Commit done");
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).value());

    acc->Abort();
    spdlog::debug("Abort done");
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    spdlog::debug("Commit done");
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).value());

    acc->Abort();
    spdlog::debug("Abort done");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelAbort) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Add label 5, but abort the transaction.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }

    acc->Abort();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).value());

    acc->Abort();
  }

  // Add label 5.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Check that label 5 exists.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).value());

    acc->Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }

    acc->Abort();
  }

  // Check that label 5 exists.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).value());

    acc->Abort();
  }

  // Remove label 5.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_TRUE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc->NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    auto other_label = acc->NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(other_label, memgraph::storage::View::NEW).value());

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelSerializationError) {
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc1 = this->store->Access(memgraph::storage::WRITE);
  auto acc2 = this->store->Access(memgraph::storage::WRITE);

  // Add label 1 in accessor 1.
  {
    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc1->NameToLabel("label1");
    auto label2 = acc1->NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::NEW).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res.value());
    }

    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::OLD).value());
    ASSERT_TRUE(vertex->HasLabel(label1, memgraph::storage::View::NEW).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.has_value());
      ASSERT_FALSE(res.value());
    }
  }

  // Add label 2 in accessor 2.
  {
    auto vertex = acc2->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc2->NameToLabel("label1");
    auto label2 = acc2->NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label1, memgraph::storage::View::NEW).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).value());
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label2);
      if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
        // InMemoryStorage works with pessimistic transactions.
        ASSERT_FALSE(res.has_value());
        ASSERT_EQ(res.error(), memgraph::storage::Error::SERIALIZATION_ERROR);
      } else {
        // Disk storage works with optimistic transactions.
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value());
      }
    }
  }

  // Finalize both accessors.
  ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    acc2->Abort();
  } else {
    // Disk storage works with optimistic transactions. So on write conflict, transaction fails on commit.
    auto res = acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.error()), memgraph::storage::SerializationError());
  }

  // Check which labels exist.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc->NameToLabel("label1");
    auto label2 = acc->NameToLabel("label2");

    ASSERT_TRUE(vertex->HasLabel(label1, memgraph::storage::View::OLD).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::OLD).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::OLD).value();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    ASSERT_TRUE(vertex->HasLabel(label1, memgraph::storage::View::NEW).value());
    ASSERT_FALSE(vertex->HasLabel(label2, memgraph::storage::View::NEW).value());
    {
      auto labels = vertex->Labels(memgraph::storage::View::NEW).value();
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.has_value());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.has_value());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.has_value());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.has_value());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.has_value());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.has_value());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc->Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.has_value());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.has_value());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.has_value());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    acc->Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.has_value());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW)->size(), 0);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Check that property 5 is null.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc1 = this->store->Access(memgraph::storage::WRITE);
  auto acc2 = this->store->Access(memgraph::storage::WRITE);

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
      ASSERT_TRUE(old_value.has_value());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
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
        ASSERT_FALSE(res.has_value());
        ASSERT_EQ(res.error(), memgraph::storage::Error::SERIALIZATION_ERROR);
      } else {
        // Disk storage works with optimistic transactions.
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res->IsNull());
      }
    }
  }

  // Finalize both accessors.
  ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  if (std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    acc2->Abort();
  } else {
    // Disk storage works with optimistic transactions. So on write conflict, transaction fails on commit.
    auto res = acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.error()), memgraph::storage::SerializationError());
  }

  // Check which properties exist.
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc->NameToProperty("property1");
    auto property2 = acc->NameToProperty("property2");

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    {
      auto properties = vertex->Properties(memgraph::storage::View::OLD).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    {
      auto properties = vertex->Properties(memgraph::storage::View::NEW).value();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc->Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(StorageV2Test, VertexLabelPropertyMixed) {
  auto acc = this->store->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();

  auto label = acc->NameToLabel("label5");
  auto property = acc->NameToProperty("property5");

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Add label 5
  ASSERT_TRUE(vertex.AddLabel(label).value());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
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
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD)->size(), 0);
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Set property 5 to "haihai"
  ASSERT_FALSE(vertex.SetProperty(property, memgraph::storage::PropertyValue("haihai"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(memgraph::storage::View::NEW).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Remove label 5
  ASSERT_TRUE(vertex.RemoveLabel(label).value());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  {
    auto labels = vertex.Labels(memgraph::storage::View::OLD).value();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(memgraph::storage::View::NEW).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Set property 5 to null
  ASSERT_FALSE(vertex.SetProperty(property, memgraph::storage::PropertyValue())->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "haihai");
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  {
    auto properties = vertex.Properties(memgraph::storage::View::OLD).value();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  // Advance command
  acc->AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::OLD).value());
  ASSERT_FALSE(vertex.HasLabel(label, memgraph::storage::View::NEW).value());
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 0);

  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

TYPED_TEST(StorageV2Test, VertexPropertyClear) {
  memgraph::storage::Gid gid;
  auto property1 = this->store->NameToProperty("property1");
  auto property2 = this->store->NameToProperty("property2");
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();

    auto old_value = vertex.SetProperty(property1, memgraph::storage::PropertyValue("value"));
    ASSERT_TRUE(old_value.has_value());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::OLD)->ValueString(), "value");
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_THAT(vertex->Properties(memgraph::storage::View::OLD).value(),
                UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value"))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.has_value());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).value().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.has_value());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).value().size(), 0);

    acc->Abort();
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto old_value = vertex->SetProperty(property2, memgraph::storage::PropertyValue(42));
    ASSERT_TRUE(old_value.has_value());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, memgraph::storage::View::OLD)->ValueString(), "value");
    ASSERT_EQ(vertex->GetProperty(property2, memgraph::storage::View::OLD)->ValueInt(), 42);
    ASSERT_THAT(vertex->Properties(memgraph::storage::View::OLD).value(),
                UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value")),
                                     std::pair(property2, memgraph::storage::PropertyValue(42))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.has_value());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).value().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.has_value());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).value().size(), 0);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(memgraph::storage::View::NEW).value().size(), 0);

    acc->Abort();
  }
}

TYPED_TEST(StorageV2Test, VertexNonexistentLabelPropertyEdgeAPI) {
  auto label = this->store->NameToLabel("label");
  auto property = this->store->NameToProperty("property");

  auto acc = this->store->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();

  // Check state before (OLD view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD).error(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);

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
  ASSERT_TRUE(vertex.AddLabel(label).has_value());
  ASSERT_TRUE(vertex.SetProperty(property, memgraph::storage::PropertyValue("value")).has_value());
  ASSERT_TRUE(acc->CreateEdge(&vertex, &vertex, acc->NameToEdgeType("edge")).has_value());

  // Check state after (OLD view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, memgraph::storage::View::OLD).error(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(memgraph::storage::View::OLD).error(), memgraph::storage::Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(vertex.Labels(memgraph::storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.HasLabel(label, memgraph::storage::View::NEW), true);
  ASSERT_EQ(vertex.Properties(memgraph::storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.GetProperty(property, memgraph::storage::View::NEW), memgraph::storage::PropertyValue("value"));
  ASSERT_EQ(vertex.InEdges(memgraph::storage::View::NEW)->edges.size(), 1);
  ASSERT_EQ(vertex.OutEdges(memgraph::storage::View::NEW)->edges.size(), 1);
  ASSERT_EQ(*vertex.InDegree(memgraph::storage::View::NEW), 1);
  ASSERT_EQ(*vertex.OutDegree(memgraph::storage::View::NEW), 1);

  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

TYPED_TEST(StorageV2Test, VertexVisibilitySingleTransaction) {
  auto acc1 = this->store->Access(memgraph::storage::WRITE);
  auto acc2 = this->store->Access(memgraph::storage::WRITE);

  auto vertex = acc1->CreateVertex();
  auto gid = vertex.Gid();

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

  ASSERT_TRUE(vertex.AddLabel(acc1->NameToLabel("label")).has_value());

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

  ASSERT_TRUE(vertex.SetProperty(acc1->NameToProperty("meaning"), memgraph::storage::PropertyValue(42)).has_value());

  auto acc3 = this->store->Access(memgraph::storage::WRITE);

  EXPECT_FALSE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc2->FindVertex(gid, memgraph::storage::View::NEW));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::OLD));
  EXPECT_FALSE(acc3->FindVertex(gid, memgraph::storage::View::NEW));

  ASSERT_TRUE(acc1->DeleteVertex(&vertex).has_value());

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
    auto acc1 = this->store->Access(memgraph::storage::WRITE);
    auto acc2 = this->store->Access(memgraph::storage::WRITE);

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

    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc1 = this->store->Access(memgraph::storage::WRITE);
    auto acc2 = this->store->Access(memgraph::storage::WRITE);

    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc1->FindVertex(gid, memgraph::storage::View::NEW));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc2->FindVertex(gid, memgraph::storage::View::NEW));

    ASSERT_TRUE(vertex->AddLabel(acc1->NameToLabel("label")).has_value());

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

    ASSERT_TRUE(vertex->SetProperty(acc1->NameToProperty("meaning"), memgraph::storage::PropertyValue(42)).has_value());

    auto acc3 = this->store->Access(memgraph::storage::WRITE);

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

    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc1 = this->store->Access(memgraph::storage::WRITE);
    auto acc2 = this->store->Access(memgraph::storage::WRITE);

    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1->DeleteVertex(&*vertex).has_value());

    auto acc3 = this->store->Access(memgraph::storage::WRITE);

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
    auto acc = this->store->Access(memgraph::storage::WRITE);

    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW));

    acc->AdvanceCommand();

    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::OLD));
    EXPECT_TRUE(acc->FindVertex(gid, memgraph::storage::View::NEW));

    acc->Abort();
  }

  {
    auto acc1 = this->store->Access(memgraph::storage::WRITE);
    auto acc2 = this->store->Access(memgraph::storage::WRITE);

    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1->DeleteVertex(&*vertex).has_value());

    auto acc3 = this->store->Access(memgraph::storage::WRITE);

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

    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = this->store->Access(memgraph::storage::WRITE);

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
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(vertex.SetProperty(property, property_value).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = this->store->Access(memgraph::storage::WRITE);
  auto vertex = acc->FindVertex(*gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(vertex);
  auto maybe_deleted_vertex = acc->DeleteVertex(&*vertex);
  ASSERT_TRUE(maybe_deleted_vertex.has_value());

  auto deleted_vertex = maybe_deleted_vertex.value();
  ASSERT_TRUE(deleted_vertex);
  // you cannot modify deleted vertex
  ASSERT_FALSE(deleted_vertex->ClearProperties().has_value());

  // you can call read only methods
  const auto maybe_property = deleted_vertex->GetProperty(property, memgraph::storage::View::OLD);
  ASSERT_TRUE(maybe_property.has_value());
  ASSERT_EQ(property_value, *maybe_property);
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  {
    // you can call read only methods and get valid results even after the
    // transaction which deleted the vertex committed, but only if the transaction
    // accessor is still alive
    const auto maybe_property = deleted_vertex->GetProperty(property, memgraph::storage::View::OLD);
    ASSERT_TRUE(maybe_property.has_value());
    ASSERT_EQ(property_value, *maybe_property);
  }
}

TYPED_TEST(StorageV2Test, UpdatesLabelsCountAfterCommit) {
  auto label1 = this->store->NameToLabel("Person");
  auto label2 = this->store->NameToLabel("Customer");
  auto label3 = this->store->NameToLabel("Employee");

  memgraph::storage::Gid v1_gid, v2_gid;

  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();

    ASSERT_TRUE(v1.AddLabel(label1));
    ASSERT_TRUE(v2.AddLabel(label1));
    ASSERT_TRUE(v2.AddLabel(label2));

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 0);
  }

  // Check commited AddLabel
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1->AddLabel(label3));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 1);
  }

  // Check commited RemoveLabel
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v2->RemoveLabel(label1));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 1);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 1);
  }

  // Check commited CreateVertex
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();

    ASSERT_TRUE(vertex.AddLabel(label3));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 1);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 2);
  }

  // Check commited DeleteVertex
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v2);
    ASSERT_TRUE(acc->DeleteVertex(&*v2));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 1);
    ASSERT_EQ(counts[label2], 0);
    ASSERT_EQ(counts[label3], 2);
  }
}

TYPED_TEST(StorageV2Test, UpdatesLabelsCountAfterAbort) {
  auto label1 = this->store->NameToLabel("Person");
  auto label2 = this->store->NameToLabel("Customer");
  auto label3 = this->store->NameToLabel("Employee");

  memgraph::storage::Gid v1_gid, v2_gid;

  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();

    ASSERT_TRUE(v1.AddLabel(label1));
    ASSERT_TRUE(v2.AddLabel(label1));
    ASSERT_TRUE(v2.AddLabel(label2));

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 0);
  }

  // Check aborted AddLabel
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1->AddLabel(label3));
    acc->Abort();
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 0);
  }

  // Check aborted RemoveLabel
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v2->RemoveLabel(label1));
    acc->Abort();
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 0);
  }

  // Check aborted CreateVertex
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();

    ASSERT_TRUE(vertex.AddLabel(label3));

    acc->Abort();
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 0);
  }

  // Check aborted DeleteVertex
  {
    auto acc = this->store->Access(memgraph::storage::WRITE);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v2);
    ASSERT_TRUE(acc->DeleteVertex(&*v2));
    acc->Abort();
  }

  {
    auto counts = this->store->GetLabelCounts();
    ASSERT_EQ(counts[label1], 2);
    ASSERT_EQ(counts[label2], 1);
    ASSERT_EQ(counts[label3], 0);
  }
}

// ---------------------------------------------------------------------------------------------
// Storage::TryAccess / TryUniqueAccess / TryReadOnlyAccess: non-blocking counterparts to
// Access()/UniqueAccess()/ReadOnlyAccess(). InMemoryStorage provides the real (main_lock_-probing)
// implementation, so these tests exercise it directly rather than going through the
// InMemoryStorage/DiskStorage TYPED_TEST_SUITE above (DiskStorage currently inherits Storage's
// safe no-op stub, which always returns std::nullopt regardless of contention -- there is nothing
// meaningful to assert about "returns a valid accessor when free" for that stub).
class StorageTryAccessTest : public ::testing::Test {
 protected:
  memgraph::storage::InMemoryStorage store{memgraph::storage::Config{
      .transaction{.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}}};
};

TEST_F(StorageTryAccessTest, SucceedsAndReturnsUsableAccessorWhenFree) {
  using namespace memgraph::storage;

  auto acc = store.TryAccess(READ);
  ASSERT_TRUE(acc.has_value());
  ASSERT_NE(*acc, nullptr);
  EXPECT_EQ((*acc)->type(), READ);
  EXPECT_NO_THROW((*acc)->CreateVertex());
  EXPECT_NO_THROW((*acc)->Abort());
  acc.reset();  // destroy the accessor to release its main_lock_ share (Abort() alone does not)

  auto write_acc = store.TryAccess(WRITE);
  ASSERT_TRUE(write_acc.has_value());
  EXPECT_EQ((*write_acc)->type(), WRITE);
  EXPECT_NO_THROW((*write_acc)->Abort());
  write_acc.reset();

  auto unique_acc = store.TryUniqueAccess();
  ASSERT_TRUE(unique_acc.has_value());
  EXPECT_EQ((*unique_acc)->type(), UNIQUE);
  EXPECT_NO_THROW((*unique_acc)->Abort());
  unique_acc.reset();

  auto ro_acc = store.TryReadOnlyAccess();
  ASSERT_TRUE(ro_acc.has_value());
  EXPECT_EQ((*ro_acc)->type(), READ_ONLY);
  EXPECT_NO_THROW((*ro_acc)->Abort());
}

// UNIQUE held -> every non-blocking probe (of every kind) must report nullopt, must not throw,
// and -- crucially -- must not have silently created (and then discarded) a transaction while
// probing. We prove the "no transaction created" part by comparing Transaction::transaction_id
// sequencing: InMemoryStorage's transaction id counter is strictly monotonic and only advances
// when Storage::CreateTransaction() actually runs (which only happens once an Accessor is
// constructed), so if any nullopt attempt below had spuriously created a transaction, the id
// observed after releasing UNIQUE would jump by more than 1.
TEST_F(StorageTryAccessTest, UniqueHeldBlocksNewSharedWithoutCreatingATransaction) {
  using namespace memgraph::storage;

  auto unique_acc = store.UniqueAccess();  // blocking call; trivially succeeds, nothing else held
  ASSERT_NE(unique_acc, nullptr);
  const uint64_t unique_id = unique_acc->GetTransaction()->transaction_id;

  EXPECT_FALSE(store.TryAccess(READ).has_value());
  EXPECT_FALSE(store.TryAccess(WRITE).has_value());
  EXPECT_FALSE(store.TryReadOnlyAccess().has_value());
  EXPECT_FALSE(store.TryUniqueAccess().has_value());

  unique_acc->Abort();
  unique_acc.reset();  // releases the UNIQUE hold on main_lock_

  auto after = store.TryAccess(READ);
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ((*after)->GetTransaction()->transaction_id, unique_id + 1)
      << "a TryAccess/TryUniqueAccess/TryReadOnlyAccess nullopt path created a transaction it "
         "should not have";
  (*after)->Abort();
}

// Mirror of the above but the OTHER conflict direction: a held shared (READ) lock must block a
// new TryUniqueAccess(), while still allowing further compatible shared acquisitions through.
TEST_F(StorageTryAccessTest, SharedHeldBlocksNewUniqueButNotNewCompatibleShared) {
  using namespace memgraph::storage;

  auto read_acc = store.Access(READ);  // blocking call; trivially succeeds
  ASSERT_NE(read_acc, nullptr);

  EXPECT_FALSE(store.TryUniqueAccess().has_value());

  // READ is not exclusive with itself/WRITE -- a second READ probe should still succeed.
  auto another_read = store.TryAccess(READ);
  ASSERT_TRUE(another_read.has_value());
  EXPECT_NO_THROW((*another_read)->Abort());

  read_acc->Abort();
}

// READ_ONLY held -> conflicts with WRITE (both directly and via TryAccess(WRITE)), matching the
// existing WRITE vs READ_ONLY mutual exclusion enforced by ResourceLock.
TEST_F(StorageTryAccessTest, ReadOnlyHeldBlocksNewWrite) {
  using namespace memgraph::storage;

  auto ro_acc = store.ReadOnlyAccess();  // blocking call; trivially succeeds
  ASSERT_NE(ro_acc, nullptr);

  EXPECT_FALSE(store.TryAccess(WRITE).has_value());

  ro_acc->Abort();
  ro_acc.reset();  // destroy the accessor to release its READ_ONLY main_lock_ share (Abort() alone does not)

  auto write_acc = store.TryAccess(WRITE);
  EXPECT_TRUE(write_acc.has_value());
  if (write_acc) EXPECT_NO_THROW((*write_acc)->Abort());
}

// --- IP-1 coro-prepare-accessor-yield: storage-side wake hook ---
// (opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md REVISION 3 §R3.1)
//
// Storage::NotifyMainLockReleased() must fire (claim + invoke on_resume) a waiter parked on
// main_lock_resume_event() exactly when: the experimental flag is ON, AND the release was of a
// UNIQUE or READ_ONLY guard. It must NOT fire on a plain READ/WRITE release, and must be a cheap,
// safe no-op with the flag off or with nobody parked.
class StorageMainLockWakeHookTest : public ::testing::Test {
 protected:
  StorageMainLockWakeHookTest() : saved_flag_(FLAGS_experimental_coro_prepare_accessor_yield) {}

  ~StorageMainLockWakeHookTest() override {
    // Restore so this test doesn't leak the flag into unrelated tests sharing the process.
    FLAGS_experimental_coro_prepare_accessor_yield = saved_flag_;
    memgraph::flags::run_time::RefreshCoroPrepareAccessorYieldEnabled();
  }

  static void SetFlag(bool enabled) {
    FLAGS_experimental_coro_prepare_accessor_yield = enabled;
    memgraph::flags::run_time::RefreshCoroPrepareAccessorYieldEnabled();
  }

  // Registers a recording waiter on `store`'s main_lock_resume_event() at the current epoch and
  // returns its ParkState; `*fired` becomes true exactly when some wake source claims and invokes
  // it.
  auto RegisterRecordingWaiter(std::shared_ptr<bool> fired) -> std::shared_ptr<memgraph::utils::ParkState> {
    auto &event = store.main_lock_resume_event();
    auto ps = std::make_shared<memgraph::utils::ParkState>();
    ps->on_resume = [fired] { *fired = true; };
    const auto epoch = event.Epoch();
    bool const registered = event.RegisterWaiter(ps, epoch);
    EXPECT_TRUE(registered) << "epoch moved between Epoch() and RegisterWaiter() -- unexpected in a single-threaded "
                               "test with GC disabled";
    return ps;
  }

  // Periodic GC disabled: a background GC UNIQUE-release could otherwise call
  // NotifyMainLockReleased() on its own and make these single-threaded assertions racy/flaky.
  memgraph::storage::InMemoryStorage store{memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::NONE},
      .transaction = {.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}}};

 private:
  bool saved_flag_;
};

TEST_F(StorageMainLockWakeHookTest, UniqueReleaseFiresWhenFlagOnAndWaiterParked) {
  using namespace memgraph::storage;
  SetFlag(true);

  auto fired = std::make_shared<bool>(false);
  auto ps = RegisterRecordingWaiter(fired);
  ASSERT_EQ(store.main_lock_resume_event().WaitersPending(), 1U);

  {
    auto unique_acc = store.UniqueAccess();
    unique_acc->Abort();
    EXPECT_FALSE(*fired) << "must not fire while main_lock_ is still held (Abort() alone does not release it)";
  }
  // unique_acc destroyed here -> Storage::Accessor::~Accessor() releases the UNIQUE guard and
  // calls NotifyMainLockReleased().

  EXPECT_TRUE(*fired);
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U);
  EXPECT_TRUE(ps->claimed.load());
}

TEST_F(StorageMainLockWakeHookTest, ReadOnlyReleaseFiresWhenFlagOnAndWaiterParked) {
  using namespace memgraph::storage;
  SetFlag(true);

  auto fired = std::make_shared<bool>(false);
  RegisterRecordingWaiter(fired);

  {
    auto ro_acc = store.ReadOnlyAccess();
    ro_acc->Abort();
    EXPECT_FALSE(*fired);
  }

  EXPECT_TRUE(*fired);
}

TEST_F(StorageMainLockWakeHookTest, DoesNotFireWhenFlagOff) {
  using namespace memgraph::storage;
  SetFlag(false);

  auto fired = std::make_shared<bool>(false);
  RegisterRecordingWaiter(fired);

  {
    auto unique_acc = store.UniqueAccess();
    unique_acc->Abort();
  }

  EXPECT_FALSE(*fired);
  // Flag-off returns before touching the event at all -- the waiter is still registered.
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 1U);
}

// F5 (IP-1 design doc REVISION 5): a parked READ_ONLY/UNIQUE waiter is unblocked precisely when
// the LAST conflicting holder releases -- and that holder is very often a WRITE (a READ_ONLY acquire
// is blocked by outstanding WRITEs) or, for a parked UNIQUE, any READ. So a WRITE/READ release MUST
// now fire the wake when a waiter is parked. (Before F5 these releases were skipped, so a parked
// query behind ongoing writes slept until its deadline instead of resuming when the writes drained.)
TEST_F(StorageMainLockWakeHookTest, WriteReleaseFiresWhenFlagOnAndWaiterParked) {
  using namespace memgraph::storage;
  SetFlag(true);

  auto fired = std::make_shared<bool>(false);
  auto ps = RegisterRecordingWaiter(fired);
  ASSERT_EQ(store.main_lock_resume_event().WaitersPending(), 1U);

  {
    auto write_acc = store.Access(WRITE);
    write_acc->Abort();
    EXPECT_FALSE(*fired) << "must not fire while the WRITE accessor still holds main_lock_";
  }
  // write_acc destroyed here -> ~Accessor() releases the guard and calls NotifyMainLockReleased();
  // with a parked waiter and the flag on, that must wake it (F5).

  EXPECT_TRUE(*fired) << "a WRITE release must wake a parked READ_ONLY/UNIQUE waiter (F5)";
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U);
  EXPECT_TRUE(ps->claimed.load());
}

TEST_F(StorageMainLockWakeHookTest, ReadReleaseFiresWhenFlagOnAndWaiterParked) {
  using namespace memgraph::storage;
  SetFlag(true);

  auto fired = std::make_shared<bool>(false);
  auto ps = RegisterRecordingWaiter(fired);
  ASSERT_EQ(store.main_lock_resume_event().WaitersPending(), 1U);

  {
    auto read_acc = store.Access(READ);
    read_acc->Abort();
    EXPECT_FALSE(*fired) << "must not fire while the READ accessor still holds main_lock_";
  }

  EXPECT_TRUE(*fired) << "a READ release must wake a parked UNIQUE waiter (F5)";
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U);
  EXPECT_TRUE(ps->claimed.load());
}

TEST_F(StorageMainLockWakeHookTest, NoWaitersIsACheapSafeNoOp) {
  using namespace memgraph::storage;
  SetFlag(true);

  ASSERT_EQ(store.main_lock_resume_event().WaitersPending(), 0U);
  EXPECT_NO_THROW(store.NotifyMainLockReleased());

  // A real UNIQUE release with zero waiters registered must also be a harmless no-op.
  auto unique_acc = store.UniqueAccess();
  EXPECT_NO_THROW(unique_acc->Abort());
  EXPECT_NO_THROW(unique_acc.reset());
}
