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
TEST(StorageV2, VertexLabelCommit) {
  storage::Storage store;
  storage::Gid gid =
      storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();

    ASSERT_FALSE(vertex.HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex.Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex.AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_TRUE(vertex.HasLabel(5, storage::View::NEW));
    {
      auto labels = vertex.Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex.AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW));
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW));

    acc.Abort();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }

    acc.Commit();
  }
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::OLD).size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW));

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

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW));
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }

    acc.Abort();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::OLD).size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW));

    acc.Abort();
  }

  // Add label 5.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW));
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    {
      auto res = vertex->AddLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }

    acc.Commit();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW));
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW));

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
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }

    acc.Abort();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::NEW));
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW));

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
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_TRUE(vertex->HasLabel(5, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 5);
    }

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex->RemoveLabel(5);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }

    acc.Commit();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(5, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(5, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::OLD).size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    ASSERT_FALSE(vertex->HasLabel(10, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(10, storage::View::NEW));

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

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(1, storage::View::NEW));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::OLD).size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_TRUE(*res.GetReturn());
    }

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD));
    ASSERT_TRUE(vertex->HasLabel(1, storage::View::NEW));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::OLD).size(), 0);
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    {
      auto res = vertex->AddLabel(1);
      ASSERT_TRUE(res.IsReturn());
      ASSERT_FALSE(*res.GetReturn());
    }
  }

  // Add label 2 in accessor 2.
  {
    auto vertex = acc2.FindVertex(gid, storage::View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_FALSE(vertex->HasLabel(1, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(1, storage::View::NEW));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW));
    ASSERT_EQ(vertex->Labels(storage::View::OLD).size(), 0);
    ASSERT_EQ(vertex->Labels(storage::View::NEW).size(), 0);

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

    ASSERT_TRUE(vertex->HasLabel(1, storage::View::OLD));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::OLD));
    {
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    ASSERT_TRUE(vertex->HasLabel(1, storage::View::NEW));
    ASSERT_FALSE(vertex->HasLabel(2, storage::View::NEW));
    {
      auto labels = vertex->Labels(storage::View::NEW);
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], 1);
    }

    acc.Abort();
  }
}
