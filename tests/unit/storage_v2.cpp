// Copyright 2021 Memgraph Ltd.
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

#include <limits>

#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"

using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, Commit) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);
    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);
    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.DeleteVertex(&*vertex);
    ASSERT_FALSE(res.HasError());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);

    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, Abort) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);
    acc.Abort();
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, AdvanceCommandCommit) {
  storage::Storage store;
  storage::Gid gid1 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid2 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();

    auto vertex1 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);

    acc.AdvanceCommand();

    auto vertex2 = acc.CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 2U);

    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 2U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 2U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, AdvanceCommandAbort) {
  storage::Storage store;
  storage::Gid gid1 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid2 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();

    auto vertex1 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);

    acc.AdvanceCommand();

    auto vertex2 = acc.CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 2U);

    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, SnapshotIsolation) {
  storage::Storage store;

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex = acc1.CreateVertex();
  auto gid = vertex.Gid();

  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
  EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 0U);
  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 1U);
  EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 0U);

  ASSERT_FALSE(acc1.Commit().HasError());

  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 0U);
  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 0U);

  acc2.Abort();

  auto acc3 = store.Access();
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::NEW), 1U);
  acc3.Abort();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, AccessorMove) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);

    storage::Storage::Accessor moved(std::move(acc));

    ASSERT_FALSE(moved.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(moved, storage::View::OLD), 0U);
    ASSERT_TRUE(moved.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(moved, storage::View::NEW), 1U);

    ASSERT_FALSE(moved.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteCommit) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = store.Access();  // read transaction
  auto acc2 = store.Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 0U);
    ASSERT_TRUE(acc2.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 1U);
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  auto acc3 = store.Access();  // read transaction
  auto acc4 = store.Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::NEW), 1U);

  // Delete the vertex in transaction 4
  {
    auto vertex = acc4.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc4, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, storage::View::NEW), 1U);

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc4, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, storage::View::NEW), 0U);

    acc4.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc4, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc4, storage::View::NEW), 0U);

    ASSERT_FALSE(acc4.Commit().HasError());
  }

  auto acc5 = store.Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_FALSE(acc5.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, storage::View::OLD), 0U);
  ASSERT_FALSE(acc5.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, storage::View::NEW), 0U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteAbort) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = store.Access();  // read transaction
  auto acc2 = store.Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 0U);
    ASSERT_TRUE(acc2.FindVertex(gid, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 1U);
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  auto acc3 = store.Access();  // read transaction
  auto acc4 = store.Access();  // write transaction (aborted)

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::NEW), 1U);

  // Delete the vertex in transaction 4, but abort the transaction
  {
    auto vertex = acc4.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc4, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, storage::View::NEW), 1U);

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc4, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, storage::View::NEW), 0U);

    acc4.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc4, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc4, storage::View::NEW), 0U);

    acc4.Abort();
  }

  auto acc5 = store.Access();  // read transaction
  auto acc6 = store.Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, storage::View::OLD), 1U);
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, storage::View::NEW), 1U);

  // Delete the vertex in transaction 6
  {
    auto vertex = acc6.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc6, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc6, storage::View::NEW), 1U);

    auto res = acc6.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc6, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc6, storage::View::NEW), 0U);

    acc6.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc6, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc6, storage::View::NEW), 0U);

    ASSERT_FALSE(acc6.Commit().HasError());
  }

  auto acc7 = store.Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, storage::View::OLD), 1U);
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, storage::View::NEW), 1U);

  // Check whether the vertex exists in transaction 7
  ASSERT_FALSE(acc7.FindVertex(gid, storage::View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc7, storage::View::OLD), 0U);
  ASSERT_FALSE(acc7.FindVertex(gid, storage::View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc7, storage::View::NEW), 0U);

  // Commit all accessors
  ASSERT_FALSE(acc1.Commit().HasError());
  ASSERT_FALSE(acc3.Commit().HasError());
  ASSERT_FALSE(acc5.Commit().HasError());
  ASSERT_FALSE(acc7.Commit().HasError());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteSerializationError) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Delete vertex in accessor 1
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 1U);

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
      EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 1U);
      EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);
    }

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
      EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 1U);
      EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);
    }

    acc1.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc1, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc1, storage::View::NEW), 0U);
  }

  // Delete vertex in accessor 2
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 1U);
    auto res = acc2.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasError());
    ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 1U);
    acc2.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc2, storage::View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, storage::View::NEW), 1U);
  }

  // Finalize both accessors
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check whether the vertex exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_FALSE(vertex);
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteSpecialCases) {
  storage::Storage store;
  storage::Gid gid1 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid2 = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex and delete it in the same transaction, but abort the
  // transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid1 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid2 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 1U);
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the vertices exist
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, storage::View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, storage::View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteLabel) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Add label, delete the vertex and check the label API (same command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label, storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Add label, delete the vertex and check the label API (different command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label, storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_EQ(vertex->HasLabel(label, storage::View::OLD).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->HasLabel(label, storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteProperty) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Set property, delete the vertex and check the property API (same command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetProperty(property, storage::PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, storage::PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Set property, delete the vertex and check the property API (different
  // command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetProperty(property, storage::PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(storage::View::OLD).GetError(), storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetError(), storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, storage::PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelCommit) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex.HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex.Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::NEW).GetValue());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::NEW).GetValue());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelAbort) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Add label 5, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Abort();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::NEW).GetValue());

    acc.Abort();
  }

  // Add label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Abort();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, storage::View::NEW).GetValue());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelSerializationError) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Add label 1 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc1.NameToLabel("label1");
    auto label2 = acc1.NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_FALSE(vertex->HasLabel(label1, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
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
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc2.NameToLabel("label1");
    auto label2 = acc2.NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check which labels exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc.NameToLabel("label1");
    auto label2 = acc.NameToLabel("label2");

    ASSERT_TRUE(vertex->HasLabel(label1, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    ASSERT_TRUE(vertex->HasLabel(label1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexPropertyCommit) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex.SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex.SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexPropertyAbort) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc.Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    acc.Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexPropertySerializationError) {
  storage::Storage store;
  storage::Gid gid = storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.Commit().HasError());
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc1.NameToProperty("property1");
    auto property2 = acc1.NameToProperty("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property1, storage::PropertyValue(123));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property1, storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc2.NameToProperty("property1");
    auto property2 = acc2.NameToProperty("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(property2, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check which properties exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc.NameToProperty("property1");
    auto property2 = acc.NameToProperty("property2");

    ASSERT_EQ(vertex->GetProperty(property1, storage::View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::OLD)->IsNull());
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(vertex->GetProperty(property1, storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelPropertyMixed) {
  storage::Storage store;
  auto acc = store.Access();
  auto vertex = acc.CreateVertex();

  auto label = acc.NameToLabel("label5");
  auto property = acc.NameToProperty("property5");

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Add label 5
  ASSERT_TRUE(vertex.AddLabel(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::OLD)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Set property 5 to "nandare"
  ASSERT_TRUE(vertex.SetProperty(property, storage::PropertyValue("nandare"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::OLD)->IsNull());
  ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
  ASSERT_EQ(vertex.Properties(storage::View::OLD)->size(), 0);
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "nandare");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Set property 5 to "haihai"
  ASSERT_FALSE(vertex.SetProperty(property, storage::PropertyValue("haihai"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Remove label 5
  ASSERT_TRUE(vertex.RemoveLabel(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Set property 5 to null
  ASSERT_FALSE(vertex.SetProperty(property, storage::PropertyValue())->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  ASSERT_FALSE(acc.Commit().HasError());
}

TEST(StorageV2, VertexPropertyClear) {
  storage::Storage store;
  storage::Gid gid;
  auto property1 = store.NameToProperty("property1");
  auto property2 = store.NameToProperty("property2");
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    auto old_value = vertex.SetProperty(property1, storage::PropertyValue("value"));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, storage::View::OLD)->ValueString(), "value");
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::OLD)->IsNull());
    ASSERT_THAT(vertex->Properties(storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, storage::PropertyValue("value"))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    auto old_value = vertex->SetProperty(property2, storage::PropertyValue(42));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, storage::View::OLD)->ValueString(), "value");
    ASSERT_EQ(vertex->GetProperty(property2, storage::View::OLD)->ValueInt(), 42);
    ASSERT_THAT(vertex->Properties(storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, storage::PropertyValue("value")),
                                     std::pair(property2, storage::PropertyValue(42))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetValue().size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(property1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
}

TEST(StorageV2, VertexNonexistentLabelPropertyEdgeAPI) {
  storage::Storage store;

  auto label = store.NameToLabel("label");
  auto property = store.NameToProperty("property");

  auto acc = store.Access();
  auto vertex = acc.CreateVertex();

  // Check state before (OLD view).
  ASSERT_EQ(vertex.Labels(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);

  // Check state before (NEW view).
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.HasLabel(label, storage::View::NEW), false);
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.GetProperty(property, storage::View::NEW), storage::PropertyValue());
  ASSERT_EQ(vertex.InEdges(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.OutEdges(storage::View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.InDegree(storage::View::NEW), 0);
  ASSERT_EQ(*vertex.OutDegree(storage::View::NEW), 0);

  // Modify vertex.
  ASSERT_TRUE(vertex.AddLabel(label).HasValue());
  ASSERT_TRUE(vertex.SetProperty(property, storage::PropertyValue("value")).HasValue());
  ASSERT_TRUE(acc.CreateEdge(&vertex, &vertex, acc.NameToEdgeType("edge")).HasValue());

  // Check state after (OLD view).
  ASSERT_EQ(vertex.Labels(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(storage::View::OLD).GetError(), storage::Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.HasLabel(label, storage::View::NEW), true);
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.GetProperty(property, storage::View::NEW), storage::PropertyValue("value"));
  ASSERT_EQ(vertex.InEdges(storage::View::NEW)->size(), 1);
  ASSERT_EQ(vertex.OutEdges(storage::View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.InDegree(storage::View::NEW), 1);
  ASSERT_EQ(*vertex.OutDegree(storage::View::NEW), 1);

  ASSERT_FALSE(acc.Commit().HasError());
}

TEST(StorageV2, VertexVisibilitySingleTransaction) {
  storage::Storage store;

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex = acc1.CreateVertex();
  auto gid = vertex.Gid();

  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
  EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));

  ASSERT_TRUE(vertex.AddLabel(acc1.NameToLabel("label")).HasValue());

  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
  EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));

  ASSERT_TRUE(vertex.SetProperty(acc1.NameToProperty("meaning"), storage::PropertyValue(42)).HasValue());

  auto acc3 = store.Access();

  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
  EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc3.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc3.FindVertex(gid, storage::View::NEW));

  ASSERT_TRUE(acc1.DeleteVertex(&vertex).HasValue());

  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc3.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc3.FindVertex(gid, storage::View::NEW));

  acc1.AdvanceCommand();
  acc3.AdvanceCommand();

  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));
  EXPECT_FALSE(acc3.FindVertex(gid, storage::View::OLD));
  EXPECT_FALSE(acc3.FindVertex(gid, storage::View::NEW));

  acc1.Abort();
  acc2.Abort();
  acc3.Abort();
}

TEST(StorageV2, VertexVisibilityMultipleTransactions) {
  storage::Storage store;
  storage::Gid gid;

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.CreateVertex();
    gid = vertex.Gid();

    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));

    acc2.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_FALSE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid, storage::View::NEW));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));

    ASSERT_TRUE(vertex->AddLabel(acc1.NameToLabel("label")).HasValue());

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));

    ASSERT_TRUE(vertex->SetProperty(acc1.NameToProperty("meaning"), storage::PropertyValue(42)).HasValue());

    auto acc3 = store.Access();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc3.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
    ASSERT_FALSE(acc3.Commit().HasError());
  }

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1.DeleteVertex(&*vertex).HasValue());

    auto acc3 = store.Access();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc1.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc3.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc1.Abort();
    acc2.Abort();
    acc3.Abort();
  }

  {
    auto acc = store.Access();

    EXPECT_TRUE(acc.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc.FindVertex(gid, storage::View::NEW));

    acc.AdvanceCommand();

    EXPECT_TRUE(acc.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc.FindVertex(gid, storage::View::NEW));

    acc.Abort();
  }

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1.DeleteVertex(&*vertex).HasValue());

    auto acc3 = store.Access();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc1.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    acc3.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, storage::View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, storage::View::NEW));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
    ASSERT_FALSE(acc3.Commit().HasError());
  }

  {
    auto acc = store.Access();

    EXPECT_FALSE(acc.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc.FindVertex(gid, storage::View::NEW));

    acc.AdvanceCommand();

    EXPECT_FALSE(acc.FindVertex(gid, storage::View::OLD));
    EXPECT_FALSE(acc.FindVertex(gid, storage::View::NEW));

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, DeletedVertexAccessor) {
  storage::Storage store;

  const auto property = store.NameToProperty("property");
  const storage::PropertyValue property_value{"property_value"};

  std::optional<storage::Gid> gid;
  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(vertex.SetProperty(property, property_value).HasError());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  auto acc = store.Access();
  auto vertex = acc.FindVertex(*gid, storage::View::OLD);
  ASSERT_TRUE(vertex);
  auto maybe_deleted_vertex = acc.DeleteVertex(&*vertex);
  ASSERT_FALSE(maybe_deleted_vertex.HasError());

  auto deleted_vertex = maybe_deleted_vertex.GetValue();
  ASSERT_TRUE(deleted_vertex);
  // you cannot modify deleted vertex
  ASSERT_TRUE(deleted_vertex->ClearProperties().HasError());

  // you can call read only methods
  const auto maybe_property = deleted_vertex->GetProperty(property, storage::View::OLD);
  ASSERT_FALSE(maybe_property.HasError());
  ASSERT_EQ(property_value, *maybe_property);
  ASSERT_FALSE(acc.Commit().HasError());

  {
    // you can call read only methods and get valid results even after the
    // transaction which deleted the vertex committed, but only if the transaction
    // accessor is still alive
    const auto maybe_property = deleted_vertex->GetProperty(property, storage::View::OLD);
    ASSERT_FALSE(maybe_property.HasError());
    ASSERT_EQ(property_value, *maybe_property);
  }
}
