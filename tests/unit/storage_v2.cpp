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

    auto res = acc.DeleteVertex(&*vertex);
    ASSERT_FALSE(res.HasError());

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

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());

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

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());

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

    auto res = acc6.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());

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
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }
  }

  // Delete vertex in accessor 2
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto res = acc2.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasError());
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
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    acc.Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid2 = vertex.Gid();
    ASSERT_FALSE(acc.FindVertex(gid2, storage::View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(gid2, storage::View::NEW).has_value());
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
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
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(5).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabel(5);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(5);
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

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabel(5).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
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
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabel(5);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), storage::Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexDeleteProperty) {
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

  // Set property, delete the vertex and check the property API (same command)
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::NEW);
    ASSERT_TRUE(vertex);

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_FALSE(
        vertex->SetProperty(5, storage::PropertyValue("nandare")).GetValue());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(5, storage::PropertyValue("haihai"));
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

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_FALSE(
        vertex->SetProperty(5, storage::PropertyValue("nandare")).GetValue());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(storage::View::OLD).GetError(),
              storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(storage::View::NEW).GetError(),
              storage::Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetProperty(5, storage::PropertyValue("haihai"));
      ASSERT_TRUE(ret.HasError());
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

    ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex.AddLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex.Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex.AddLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetValue());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetValue());

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

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex->AddLabel(5);
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

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetValue());

    acc.Abort();
  }

  // Add label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
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

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW).GetValue());

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

  // Add label 1 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }
  }

  // Add label 2 in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.HasError());
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

    ASSERT_TRUE(vertex->HasLabel(1, storage::View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD).GetValue());
    {
      auto labels = vertex->Labels(storage::View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    ASSERT_TRUE(vertex->HasLabel(1, storage::View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW).GetValue());
    {
      auto labels = vertex->Labels(storage::View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexPropertyCommit) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex.SetProperty(5, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(),
              "temporary");
    {
      auto properties = vertex.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "temporary");
    }

    {
      auto res = vertex.SetProperty(5, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex.Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(10, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(10, storage::View::NEW)->IsNull());

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    ASSERT_TRUE(vertex->GetProperty(10, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(10, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexPropertyAbort) {
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

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "temporary");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "temporary");
    }

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    acc.Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    ASSERT_TRUE(vertex->GetProperty(10, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(10, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "temporary");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "temporary");
    }

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    acc.Commit();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(10, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(10, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    acc.Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(10, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(10, storage::View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::NEW)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    {
      auto res = vertex->SetProperty(5, storage::PropertyValue());
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_EQ(vertex->GetProperty(5, storage::View::OLD)->ValueString(),
              "nandare");
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[5].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    acc.Commit();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(5, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(5, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    ASSERT_TRUE(vertex->GetProperty(10, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(10, storage::View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexPropertySerializationError) {
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

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(1, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(1, storage::PropertyValue(123));
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    ASSERT_TRUE(vertex->GetProperty(1, storage::View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(1, storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(1, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(1, storage::View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(storage::View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(storage::View::NEW)->size(), 0);

    {
      auto res = vertex->SetProperty(2, storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), storage::Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  acc1.Commit();
  acc2.Abort();

  // Check which properties exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(1, storage::View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::OLD)->IsNull());
    {
      auto properties = vertex->Properties(storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[1].ValueInt(), 123);
    }

    ASSERT_EQ(vertex->GetProperty(1, storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(2, storage::View::NEW)->IsNull());
    {
      auto properties = vertex->Properties(storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[1].ValueInt(), 123);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2, VertexLabelPropertyMixed) {
  storage::Storage store;
  auto acc = store.Access();
  auto vertex = acc.CreateVertex();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Add label 5
  ASSERT_TRUE(vertex.AddLabel(5).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::OLD)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Set property 5 to "nandare"
  ASSERT_FALSE(
      vertex.SetProperty(5, storage::PropertyValue("nandare")).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::OLD)->IsNull());
  ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(),
            "nandare");
  ASSERT_EQ(vertex.Properties(storage::View::OLD)->size(), 0);
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "nandare");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_EQ(vertex.GetProperty(5, storage::View::OLD)->ValueString(),
            "nandare");
  ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(),
            "nandare");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "nandare");
  }

  // Set property 5 to "haihai"
  ASSERT_TRUE(
      vertex.SetProperty(5, storage::PropertyValue("haihai")).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_EQ(vertex.GetProperty(5, storage::View::OLD)->ValueString(),
            "nandare");
  ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  {
    auto labels = vertex.Labels(storage::View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_EQ(vertex.GetProperty(5, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }

  // Remove label 5
  ASSERT_TRUE(vertex.RemoveLabel(5).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  {
    auto labels = vertex.Labels(storage::View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], 5);
  }
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(5, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(5, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(5, storage::View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(storage::View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }

  // Set property 5 to null
  ASSERT_TRUE(vertex.SetProperty(5, storage::PropertyValue()).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(5, storage::View::OLD)->ValueString(), "haihai");
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
  {
    auto properties = vertex.Properties(storage::View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[5].ValueString(), "haihai");
  }
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(storage::View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(5, storage::View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(storage::View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(storage::View::NEW)->size(), 0);

  acc.Commit();
}
