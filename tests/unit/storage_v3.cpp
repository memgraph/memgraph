// Copyright 2022 Memgraph Ltd.
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

#include "storage/v3/property_value.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage_v3_test_utils.hpp"

using testing::UnorderedElementsAre;

namespace memgraph::storage::v3::tests {

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, Commit) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.DeleteVertex(&*vertex);
    ASSERT_FALSE(res.HasError());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);

    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, Abort) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    acc.Abort();
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, AdvanceCommandCommit) {
  Storage store;
  Gid gid1 = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  Gid gid2 = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();

    auto vertex1 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid1, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);

    acc.AdvanceCommand();

    auto vertex2 = acc.CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 2U);

    ASSERT_TRUE(acc.FindVertex(gid1, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, View::NEW).has_value());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid1, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, View::NEW).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 2U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 2U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, AdvanceCommandAbort) {
  Storage store;
  Gid gid1 = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  Gid gid2 = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();

    auto vertex1 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid1, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);

    acc.AdvanceCommand();

    auto vertex2 = acc.CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 2U);

    ASSERT_TRUE(acc.FindVertex(gid1, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, View::NEW).has_value());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid1, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid1, View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, SnapshotIsolation) {
  Storage store;

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex = acc1.CreateVertex();
  auto gid = vertex.Gid();

  ASSERT_FALSE(acc2.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
  ASSERT_FALSE(acc2.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 1U);
  EXPECT_EQ(CountVertices(acc2, View::NEW), 0U);

  ASSERT_FALSE(acc1.Commit().HasError());

  ASSERT_FALSE(acc2.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
  ASSERT_FALSE(acc2.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc2, View::NEW), 0U);

  acc2.Abort();

  auto acc3 = store.Access();
  ASSERT_TRUE(acc3.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);
  acc3.Abort();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, AccessorMove) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);

    Storage::Accessor moved(std::move(acc));

    ASSERT_FALSE(moved.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(moved, View::OLD), 0U);
    ASSERT_TRUE(moved.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(moved, View::NEW), 1U);

    ASSERT_FALSE(moved.Commit().HasError());
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexDeleteCommit) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = store.Access();  // read transaction
  auto acc2 = store.Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
    ASSERT_TRUE(acc2.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  auto acc3 = store.Access();  // read transaction
  auto acc4 = store.Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Delete the vertex in transaction 4
  {
    auto vertex = acc4.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 1U);

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc4, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    ASSERT_FALSE(acc4.Commit().HasError());
  }

  auto acc5 = store.Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_FALSE(acc5.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, View::OLD), 0U);
  ASSERT_FALSE(acc5.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, View::NEW), 0U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexDeleteAbort) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = store.Access();  // read transaction
  auto acc2 = store.Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2.FindVertex(gid, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
    ASSERT_TRUE(acc2.FindVertex(gid, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  auto acc3 = store.Access();  // read transaction
  auto acc4 = store.Access();  // write transaction (aborted)

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Delete the vertex in transaction 4, but abort the transaction
  {
    auto vertex = acc4.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 1U);

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc4, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.Abort();
  }

  auto acc5 = store.Access();  // read transaction
  auto acc6 = store.Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, View::OLD), 1U);
  ASSERT_TRUE(acc5.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, View::NEW), 1U);

  // Delete the vertex in transaction 6
  {
    auto vertex = acc6.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc6, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc6, View::NEW), 1U);

    auto res = acc6.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc6, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc6, View::NEW), 0U);

    acc6.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc6, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc6, View::NEW), 0U);

    ASSERT_FALSE(acc6.Commit().HasError());
  }

  auto acc7 = store.Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, View::OLD), 1U);
  ASSERT_TRUE(acc5.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, View::NEW), 1U);

  // Check whether the vertex exists in transaction 7
  ASSERT_FALSE(acc7.FindVertex(gid, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc7, View::OLD), 0U);
  ASSERT_FALSE(acc7.FindVertex(gid, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc7, View::NEW), 0U);

  // Commit all accessors
  ASSERT_FALSE(acc1.Commit().HasError());
  ASSERT_FALSE(acc3.Commit().HasError());
  ASSERT_FALSE(acc5.Commit().HasError());
  ASSERT_FALSE(acc7.Commit().HasError());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexDeleteSerializationError) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

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
    auto vertex = acc1.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc1, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc1, View::NEW), 1U);

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
      EXPECT_EQ(CountVertices(acc1, View::OLD), 1U);
      EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);
    }

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
      EXPECT_EQ(CountVertices(acc1, View::OLD), 1U);
      EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);
    }

    acc1.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);
  }

  // Delete vertex in accessor 2
  {
    auto vertex = acc2.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc2, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    auto res = acc2.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasError());
    ASSERT_EQ(res.GetError(), Error::SERIALIZATION_ERROR);
    EXPECT_EQ(CountVertices(acc2, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    acc2.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc2, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
  }

  // Finalize both accessors
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check whether the vertex exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_FALSE(vertex);
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexDeleteSpecialCases) {
  Storage store;
  Gid gid1 = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  Gid gid2 = Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex and delete it in the same transaction, but abort the
  // transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid1 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid1, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid2 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(gid2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the vertices exist
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid1, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid1, View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexDeleteLabel) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Add label, delete the vertex and check the label API (same command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Add label, delete the vertex and check the label API (different command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(label).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label, View::NEW).GetError(), Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }
    ASSERT_EQ(vertex->Labels(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_EQ(vertex->HasLabel(label, View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->HasLabel(label, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(label);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexDeleteProperty) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, View::NEW).has_value());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Set property, delete the vertex and check the property API (same command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetProperty(property, PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Set property, delete the vertex and check the property API (different
  // command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetProperty(property, PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property, View::NEW).GetError(), Error::DELETED_OBJECT);
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }
    ASSERT_EQ(vertex->Properties(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property, View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->GetProperty(property, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(property, PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexLabelCommit) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);

    {
      auto res = vertex.AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex.Labels(View::NEW).GetValue();
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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexLabelAbort) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }

  // Add label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    {
      auto res = vertex->RemoveLabel(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = acc.NameToLabel("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    auto other_label = acc.NameToLabel("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexLabelSerializationError) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
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
    auto vertex = acc1.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc1.NameToLabel("label1");
    auto label2 = acc1.NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_FALSE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
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
    auto vertex = acc2.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc2.NameToLabel("label1");
    auto label2 = acc2.NameToLabel("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(label1);
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check which labels exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = acc.NameToLabel("label1");
    auto label2 = acc.NameToLabel("label2");

    ASSERT_TRUE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    ASSERT_TRUE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexPropertyCommit) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex.SetProperty(property, PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex.Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex.SetProperty(property, PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex.Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    {
      auto old_value = vertex->SetProperty(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexPropertyAbort) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

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
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc.Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property, PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetProperty(property, PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    acc.Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetProperty(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = acc.NameToProperty("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    auto other_property = acc.NameToProperty("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexPropertySerializationError) {
  Storage store;
  Gid gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
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
    auto vertex = acc1.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc1.NameToProperty("property1");
    auto property2 = acc1.NameToProperty("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetProperty(property1, PropertyValue(123));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property1, View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc2.NameToProperty("property1");
    auto property2 = acc2.NameToProperty("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(property2, PropertyValue("nandare"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1.Commit().HasError());
  acc2.Abort();

  // Check which properties exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = acc.NameToProperty("property1");
    auto property2 = acc.NameToProperty("property2");

    ASSERT_EQ(vertex->GetProperty(property1, View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(vertex->GetProperty(property1, View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, VertexLabelPropertyMixed) {
  Storage store;
  auto acc = store.Access();
  auto vertex = acc.CreateVertex();

  auto label = acc.NameToLabel("label5");
  auto property = acc.NameToProperty("property5");

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Add label 5
  ASSERT_TRUE(vertex.AddLabel(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, View::OLD)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Set property 5 to "nandare"
  ASSERT_TRUE(vertex.SetProperty(property, PropertyValue("nandare"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, View::OLD)->IsNull());
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "nandare");
  ASSERT_EQ(vertex.Properties(View::OLD)->size(), 0);
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "nandare");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Set property 5 to "haihai"
  ASSERT_FALSE(vertex.SetProperty(property, PropertyValue("haihai"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Remove label 5
  ASSERT_TRUE(vertex.RemoveLabel(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Set property 5 to null
  ASSERT_FALSE(vertex.SetProperty(property, PropertyValue())->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  ASSERT_FALSE(acc.Commit().HasError());
}

TEST(StorageV3, VertexPropertyClear) {
  Storage store;
  Gid gid;
  auto property1 = store.NameToProperty("property1");
  auto property2 = store.NameToProperty("property2");
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    auto old_value = vertex.SetProperty(property1, PropertyValue("value"));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, View::OLD)->ValueString(), "value");
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_THAT(vertex->Properties(View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, PropertyValue("value"))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    auto old_value = vertex->SetProperty(property2, PropertyValue(42));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, View::OLD)->ValueString(), "value");
    ASSERT_EQ(vertex->GetProperty(property2, View::OLD)->ValueInt(), 42);
    ASSERT_THAT(
        vertex->Properties(View::OLD).GetValue(),
        UnorderedElementsAre(std::pair(property1, PropertyValue("value")), std::pair(property2, PropertyValue(42))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
}

TEST(StorageV3, VertexNonexistentLabelPropertyEdgeAPI) {
  Storage store;

  auto label = store.NameToLabel("label");
  auto property = store.NameToProperty("property");

  auto acc = store.Access();
  auto vertex = acc.CreateVertex();

  // Check state before (OLD view).
  ASSERT_EQ(vertex.Labels(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);

  // Check state before (NEW view).
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.HasLabel(label, View::NEW), false);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.GetProperty(property, View::NEW), PropertyValue());
  ASSERT_EQ(vertex.InEdges(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.OutEdges(View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.InDegree(View::NEW), 0);
  ASSERT_EQ(*vertex.OutDegree(View::NEW), 0);

  // Modify vertex.
  ASSERT_TRUE(vertex.AddLabel(label).HasValue());
  ASSERT_TRUE(vertex.SetProperty(property, PropertyValue("value")).HasValue());
  ASSERT_TRUE(acc.CreateEdge(&vertex, &vertex, acc.NameToEdgeType("edge")).HasValue());

  // Check state after (OLD view).
  ASSERT_EQ(vertex.Labels(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.HasLabel(label, View::NEW), true);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.GetProperty(property, View::NEW), PropertyValue("value"));
  ASSERT_EQ(vertex.InEdges(View::NEW)->size(), 1);
  ASSERT_EQ(vertex.OutEdges(View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.InDegree(View::NEW), 1);
  ASSERT_EQ(*vertex.OutDegree(View::NEW), 1);

  ASSERT_FALSE(acc.Commit().HasError());
}

TEST(StorageV3, VertexVisibilitySingleTransaction) {
  Storage store;

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex = acc1.CreateVertex();
  auto gid = vertex.Gid();

  EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
  EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));

  ASSERT_TRUE(vertex.AddLabel(acc1.NameToLabel("label")).HasValue());

  EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
  EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));

  ASSERT_TRUE(vertex.SetProperty(acc1.NameToProperty("meaning"), PropertyValue(42)).HasValue());

  auto acc3 = store.Access();

  EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
  EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc3.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc3.FindVertex(gid, View::NEW));

  ASSERT_TRUE(acc1.DeleteVertex(&vertex).HasValue());

  EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc3.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc3.FindVertex(gid, View::NEW));

  acc1.AdvanceCommand();
  acc3.AdvanceCommand();

  EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));
  EXPECT_FALSE(acc3.FindVertex(gid, View::OLD));
  EXPECT_FALSE(acc3.FindVertex(gid, View::NEW));

  acc1.Abort();
  acc2.Abort();
  acc3.Abort();
}

TEST(StorageV3, VertexVisibilityMultipleTransactions) {
  Storage store;
  Gid gid;

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.CreateVertex();
    gid = vertex.Gid();

    EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_FALSE(acc2.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid, View::NEW));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));

    ASSERT_TRUE(vertex->AddLabel(acc1.NameToLabel("label")).HasValue());

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));

    ASSERT_TRUE(vertex->SetProperty(acc1.NameToProperty("meaning"), PropertyValue(42)).HasValue());

    auto acc3 = store.Access();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc3.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
    ASSERT_FALSE(acc3.Commit().HasError());
  }

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1.DeleteVertex(&*vertex).HasValue());

    auto acc3 = store.Access();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc3.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc1.Abort();
    acc2.Abort();
    acc3.Abort();
  }

  {
    auto acc = store.Access();

    EXPECT_TRUE(acc.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc.FindVertex(gid, View::NEW));

    acc.AdvanceCommand();

    EXPECT_TRUE(acc.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc.FindVertex(gid, View::NEW));

    acc.Abort();
  }

  {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex = acc1.FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1.DeleteVertex(&*vertex).HasValue());

    auto acc3 = store.Access();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    acc3.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(gid, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(gid, View::NEW));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
    ASSERT_FALSE(acc3.Commit().HasError());
  }

  {
    auto acc = store.Access();

    EXPECT_FALSE(acc.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc.FindVertex(gid, View::NEW));

    acc.AdvanceCommand();

    EXPECT_FALSE(acc.FindVertex(gid, View::OLD));
    EXPECT_FALSE(acc.FindVertex(gid, View::NEW));

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV3, DeletedVertexAccessor) {
  Storage store;

  const auto property = store.NameToProperty("property");
  const PropertyValue property_value{"property_value"};

  std::optional<Gid> gid;
  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(vertex.SetProperty(property, property_value).HasError());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  auto acc = store.Access();
  auto vertex = acc.FindVertex(*gid, View::OLD);
  ASSERT_TRUE(vertex);
  auto maybe_deleted_vertex = acc.DeleteVertex(&*vertex);
  ASSERT_FALSE(maybe_deleted_vertex.HasError());

  auto deleted_vertex = maybe_deleted_vertex.GetValue();
  ASSERT_TRUE(deleted_vertex);
  // you cannot modify deleted vertex
  ASSERT_TRUE(deleted_vertex->ClearProperties().HasError());

  // you can call read only methods
  const auto maybe_property = deleted_vertex->GetProperty(property, View::OLD);
  ASSERT_FALSE(maybe_property.HasError());
  ASSERT_EQ(property_value, *maybe_property);
  ASSERT_FALSE(acc.Commit().HasError());

  {
    // you can call read only methods and get valid results even after the
    // transaction which deleted the vertex committed, but only if the transaction
    // accessor is still alive
    const auto maybe_property = deleted_vertex->GetProperty(property, View::OLD);
    ASSERT_FALSE(maybe_property.HasError());
    ASSERT_EQ(property_value, *maybe_property);
  }
}
}  // namespace memgraph::storage::v3::tests
