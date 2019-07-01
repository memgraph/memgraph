#include <gtest/gtest.h>

#include <limits>

#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, Commit) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Commit();
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = vertex->Delete();
    ASSERT_FALSE(res.IsError());

    acc.Commit();
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, Abort) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Abort();
  }
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, AdvanceCommandCommit) {
  storage::Storage store;
  storage::Gid gid1 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid2 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();

    auto vertex1 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());

    acc.AdvanceCommand();

    auto vertex2 = acc.CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());

    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());

    acc.Commit();
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, AdvanceCommandAbort) {
  storage::Storage store;
  storage::Gid gid1 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid2 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();

    auto vertex1 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());

    acc.AdvanceCommand();

    auto vertex2 = acc.CreateVertex();
    gid2 = vertex2.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());

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
  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::NEW).has_value());

  acc1.Commit();

  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc2.FindVertex(gid, storage::View::NEW).has_value());

  acc2.Abort();

  auto acc3 = store.Access();
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());
  acc3.Abort();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, AccessorMove) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());

    storage::Storage::Accessor moved(std::move(acc));

    ASSERT_FALSE(moved.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(moved.FindVertex(gid, storage::View::NEW).has_value());

    moved.Commit();
  }
  {
    auto acc = store.Access();
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteCommit) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = store.Access();  // read transaction
  auto acc2 = store.Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc2.FindVertex(gid, storage::View::NEW).has_value());
    acc2.Commit();
  }

  auto acc3 = store.Access();  // read transaction
  auto acc4 = store.Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());

  // Delete the vertex in transaction 4
  {
    auto vertex = acc4.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = vertex->Delete();
    ASSERT_TRUE(res.IsReturn());

    acc4.Commit();
  }

  auto acc5 = store.Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 5
  ASSERT_FALSE(acc5.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc5.FindVertex(gid, storage::View::NEW).has_value());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteAbort) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  auto acc1 = store.Access();  // read transaction
  auto acc2 = store.Access();  // write transaction

  // Create the vertex in transaction 2
  {
    auto vertex = acc2.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc2.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc2.FindVertex(gid, storage::View::NEW).has_value());
    acc2.Commit();
  }

  auto acc3 = store.Access();  // read transaction
  auto acc4 = store.Access();  // write transaction (aborted)

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());

  // Delete the vertex in transaction 4, but abort the transaction
  {
    auto vertex = acc4.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = vertex->Delete();
    ASSERT_TRUE(res.IsReturn());

    acc4.Abort();
  }

  auto acc5 = store.Access();  // read transaction
  auto acc6 = store.Access();  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::NEW).has_value());

  // Delete the vertex in transaction 6
  {
    auto vertex = acc6.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    auto res = vertex->Delete();
    ASSERT_TRUE(res.IsReturn());

    acc6.Commit();
  }

  auto acc7 = store.Access();  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc1.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc3.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_TRUE(acc5.FindVertex(gid, storage::View::NEW).has_value());

  // Check whether the vertex exists in transaction 7
  ASSERT_FALSE(acc7.FindVertex(gid, storage::View::OLD).has_value());
  ASSERT_FALSE(acc7.FindVertex(gid, storage::View::NEW).has_value());

  // Commit all accessors
  acc1.Commit();
  acc3.Commit();
  acc5.Commit();
  acc7.Commit();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteSerializationError) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    acc.Commit();
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Delete vertex in accessor 1
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->Delete();
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    {
      auto res = vertex->Delete();
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }
  }

  // Delete vertex in accessor 2
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto res = vertex->Delete();
    ASSERT_TRUE(res.IsError());
    ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
  }

  // Finalize both accessors
  acc1.Commit();
  acc2.Abort();

  // Check whether the vertex exists
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_FALSE(vertex);
    acc.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteSpecialCases) {
  storage::Storage store;
  storage::Gid gid1 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  storage::Gid gid2 =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex and delete it in the same transaction, but abort the
  // transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid1 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    auto res = vertex.Delete();
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());
    acc.Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid2 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    auto res = vertex.Delete();
    ASSERT_TRUE(res.IsReturn());
    ASSERT_TRUE(res.GetReturn());
    acc.Commit();
  }

  // Check whether the vertices exist
  {
    auto acc = store.Access();
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid1, storage::View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteLabel) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid, storage::View::NEW).has_value());
    acc.Commit();
  }

  // Add label, delete the vertex and check the label API (same command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(5).GetReturn());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    // Delete the vertex
    ASSERT_TRUE(vertex->Delete().GetReturn());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_EQ(vertex->HasLabel(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(5);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(5);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Add label, delete the vertex and check the label API (different command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(5).GetReturn());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    // Delete the vertex
    ASSERT_TRUE(vertex->Delete().GetReturn());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_EQ(vertex->HasLabel(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_EQ(vertex->HasLabel(5, storage::View::OLD).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->HasLabel(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(5);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(5);
      ASSERT_TRUE(ret.IsError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelCommit) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex.Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex.AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex.Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex.AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetReturn());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetReturn());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelAbort) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    acc.Commit();
  }

  // Add label 5, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Abort();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetReturn());

    acc.Abort();
  }

  // Add label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Commit();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetReturn());

    acc.Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Abort();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetReturn());

    acc.Abort();
  }

  // Remove label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }

    acc.Commit();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetReturn());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelSerializationError) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    acc.Commit();
  }

  auto acc1 = store.Access();
  auto acc2 = store.Access();

  // Add label 10 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(1, storage::View::NEW).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(res.GetReturn());
    }

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD).GetReturn());
    ASSERT_TRUE(vertex->HasLabel(1, storage::View::NEW).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(res.GetReturn());
    }
  }

  // Add label 2 in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(1, storage::View::NEW).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetReturn());
    ASSERT_EQ(vertex->Labels(storage::View::OLD).GetReturn().size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetReturn().size(), 0);

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.IsError());
      ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  acc1.Commit();
  acc2.Abort();

  // Check which labels exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(1, storage::View::OLD).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    ASSERT_TRUE(vertex->HasLabel(1, storage::View::NEW).GetReturn());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetReturn());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetReturn();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    acc.Abort();
  }
}
