// Copyright 2025 Memgraph Ltd.
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
#include <vector>

#include "query/db_accessor.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace memgraph::query::test {

using storage::Gid;
using storage::LabelId;
using storage::PropertyId;
using storage::PropertyValue;
using storage::View;

class DbAccessorChunkedTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);

    // Scoped setup of indices
    {
      auto unique_acc = storage_->UniqueAccess();
      auto dba = DbAccessor(unique_acc.get());

      label_id_ = dba.NameToLabel("Label");
      prop_id_ = dba.NameToProperty("prop");
      type_id_ = dba.NameToEdgeType("Type");

      ASSERT_TRUE(unique_acc->CreateIndex(label_id_).has_value());
    }
    {
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(label_id_, {prop_id_}).has_value());
    }
    {
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(type_id_).has_value());
    }
    {
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(type_id_, prop_id_).has_value());
    }
    {
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateGlobalEdgeIndex(prop_id_).has_value());
    }
    {
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  std::unique_ptr<storage::Storage> storage_;
  storage::Config config_{.salient = {.items = {.properties_on_edges = true}}};

  LabelId label_id_;
  PropertyId prop_id_;
  storage::EdgeTypeId type_id_;
};

TEST_F(DbAccessorChunkedTest, AllVerticesChunkIterator) {
  std::vector<Gid> gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    for (int i = 0; i < 100; ++i) {
      auto v = dba.InsertVertex();
      gids.push_back(v.Gid());
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto chunks = dba.ChunkedVertices(View::OLD, 4);
  ASSERT_EQ(chunks.size(), 4);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto v : chunk) {
      read_gids.push_back(v.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(gids.begin(), gids.end());
  EXPECT_EQ(read_gids, gids);
}

TEST_F(DbAccessorChunkedTest, LabeledVerticesChunkIterator) {
  std::vector<Gid> labeled_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    for (int i = 0; i < 100; ++i) {
      auto v = dba.InsertVertex();
      if (i % 2 == 0) {
        ASSERT_TRUE(v.AddLabel(label_id_).has_value());
        labeled_gids.push_back(v.Gid());
      }
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto chunks = dba.ChunkedVertices(View::OLD, label_id_, 4);
  ASSERT_EQ(chunks.size(), 4);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto v : chunk) {
      read_gids.push_back(v.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(labeled_gids.begin(), labeled_gids.end());
  EXPECT_EQ(read_gids, labeled_gids);
}

TEST_F(DbAccessorChunkedTest, LabeledPropertyVerticesChunkIterator) {
  std::vector<Gid> matching_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    for (int i = 0; i < 100; ++i) {
      auto v = dba.InsertVertex();
      ASSERT_TRUE(v.AddLabel(label_id_).has_value());
      ASSERT_TRUE(v.SetProperty(prop_id_, PropertyValue(static_cast<int64_t>(i))).has_value());
      if (i >= 20 && i < 50) {
        matching_gids.push_back(v.Gid());
      }
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  std::vector<storage::PropertyPath> props = {{prop_id_}};
  std::vector<storage::PropertyValueRange> ranges = {storage::PropertyValueRange::Bounded(
      utils::MakeBoundInclusive(PropertyValue(int64_t{20})), utils::MakeBoundExclusive(PropertyValue(int64_t{50})))};

  auto chunks = dba.ChunkedVertices(View::OLD, label_id_, props, ranges, 4);
  ASSERT_GT(chunks.size(), 0);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto v : chunk) {
      read_gids.push_back(v.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(matching_gids.begin(), matching_gids.end());
  EXPECT_EQ(read_gids, matching_gids);
}

TEST_F(DbAccessorChunkedTest, EdgeTypeChunkIterator) {
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    for (int i = 0; i < 100; ++i) {
      auto e = dba.InsertEdge(&v1, &v2, type_id_);
      ASSERT_TRUE(e.has_value());
      edge_gids.push_back(e->Gid());
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto chunks = dba.ChunkedEdges(View::OLD, type_id_, 4);
  ASSERT_EQ(chunks.size(), 4);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto e : chunk) {
      read_gids.push_back(e.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(edge_gids.begin(), edge_gids.end());
  EXPECT_EQ(read_gids, edge_gids);
}

TEST_F(DbAccessorChunkedTest, EdgeTypePropertyChunkIterator) {
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    for (int i = 0; i < 100; ++i) {
      auto e = dba.InsertEdge(&v1, &v2, type_id_);
      ASSERT_TRUE(e.has_value());
      ASSERT_TRUE(e->SetProperty(prop_id_, PropertyValue(static_cast<int64_t>(i))).has_value());
      edge_gids.push_back(e->Gid());
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto chunks = dba.ChunkedEdges(View::OLD, type_id_, prop_id_, 4);
  ASSERT_EQ(chunks.size(), 4);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto e : chunk) {
      read_gids.push_back(e.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(edge_gids.begin(), edge_gids.end());
  EXPECT_EQ(read_gids, edge_gids);
}

TEST_F(DbAccessorChunkedTest, EdgeTypePropertyRangeChunkIterator) {
  std::vector<Gid> matching_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    for (int i = 0; i < 100; ++i) {
      auto e = dba.InsertEdge(&v1, &v2, type_id_);
      ASSERT_TRUE(e.has_value());
      ASSERT_TRUE(e->SetProperty(prop_id_, PropertyValue(static_cast<int64_t>(i))).has_value());
      if (i >= 20 && i < 50) {
        matching_gids.push_back(e->Gid());
      }
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto lower = utils::MakeBoundInclusive(PropertyValue(int64_t{20}));
  auto upper = utils::MakeBoundExclusive(PropertyValue(int64_t{50}));

  auto chunks = dba.ChunkedEdges(View::OLD, type_id_, prop_id_, lower, upper, 4);
  ASSERT_GT(chunks.size(), 0);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto e : chunk) {
      read_gids.push_back(e.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(matching_gids.begin(), matching_gids.end());
  EXPECT_EQ(read_gids, matching_gids);
}

TEST_F(DbAccessorChunkedTest, PropertyChunkIterator) {
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    for (int i = 0; i < 100; ++i) {
      auto e = dba.InsertEdge(&v1, &v2, type_id_);
      ASSERT_TRUE(e.has_value());
      ASSERT_TRUE(e->SetProperty(prop_id_, PropertyValue(static_cast<int64_t>(i))).has_value());
      edge_gids.push_back(e->Gid());
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto chunks = dba.ChunkedEdges(View::OLD, prop_id_, 4);
  ASSERT_EQ(chunks.size(), 4);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto e : chunk) {
      read_gids.push_back(e.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(edge_gids.begin(), edge_gids.end());
  EXPECT_EQ(read_gids, edge_gids);
}

TEST_F(DbAccessorChunkedTest, PropertyExactValueChunkIterator) {
  std::vector<Gid> matching_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    for (int i = 0; i < 100; ++i) {
      auto e = dba.InsertEdge(&v1, &v2, type_id_);
      ASSERT_TRUE(e.has_value());
      int64_t val = (i < 50) ? 42 : i;
      ASSERT_TRUE(e->SetProperty(prop_id_, PropertyValue(val)).has_value());
      if (val == 42) {
        matching_gids.push_back(e->Gid());
      }
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto chunks = dba.ChunkedEdges(View::OLD, prop_id_, PropertyValue(int64_t{42}), 4);
  ASSERT_GT(chunks.size(), 0);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto e : chunk) {
      read_gids.push_back(e.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(matching_gids.begin(), matching_gids.end());
  EXPECT_EQ(read_gids, matching_gids);
}

TEST_F(DbAccessorChunkedTest, PropertyRangeChunkIterator) {
  std::vector<Gid> matching_gids;
  {
    auto acc = storage_->Access();
    auto dba = DbAccessor(acc.get());
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    for (int i = 0; i < 100; ++i) {
      auto e = dba.InsertEdge(&v1, &v2, type_id_);
      ASSERT_TRUE(e.has_value());
      ASSERT_TRUE(e->SetProperty(prop_id_, PropertyValue(static_cast<int64_t>(i))).has_value());
      if (i >= 20 && i < 50) {
        matching_gids.push_back(e->Gid());
      }
    }
    ASSERT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  auto dba = DbAccessor(acc.get());
  auto lower = utils::MakeBoundInclusive(PropertyValue(int64_t{20}));
  auto upper = utils::MakeBoundExclusive(PropertyValue(int64_t{50}));

  auto chunks = dba.ChunkedEdges(View::OLD, prop_id_, lower, upper, 4);
  ASSERT_GT(chunks.size(), 0);

  std::vector<Gid> read_gids;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto chunk = chunks.get_chunk(i);
    for (auto e : chunk) {
      read_gids.push_back(e.Gid());
    }
  }
  std::sort(read_gids.begin(), read_gids.end());
  std::sort(matching_gids.begin(), matching_gids.end());
  EXPECT_EQ(read_gids, matching_gids);
}

}  // namespace memgraph::query::test
