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
#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-type-util.h>

#include "disk_test_utils.hpp"
#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/rocksdb_serialization.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;
using memgraph::replication_coordination_glue::ReplicationRole;
using testing::IsEmpty;
using testing::Types;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

template <typename StorageType>
class IndexTest : public testing::Test {
 protected:
  void SetUp() override {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    this->storage = std::make_unique<StorageType>(config_);
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    this->prop_id = acc->NameToProperty("id");
    this->prop_val = acc->NameToProperty("val");
    this->label1 = acc->NameToLabel("label1");
    this->label2 = acc->NameToLabel("label2");
    this->edge_type_id1 = acc->NameToEdgeType("edge_type_1");
    this->edge_type_id2 = acc->NameToEdgeType("edge_type_2");
    vertex_id = 0;
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    this->storage.reset(nullptr);
  }

  const std::string testSuite = "storage_v2_indices";
  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  PropertyId prop_id;
  PropertyId prop_val;
  LabelId label1;
  LabelId label2;
  EdgeTypeId edge_type_id1;
  EdgeTypeId edge_type_id2;

  VertexAccessor CreateVertex(Storage::Accessor *accessor) {
    VertexAccessor vertex = accessor->CreateVertex();
    MG_ASSERT(!vertex.SetProperty(this->prop_id, PropertyValue(vertex_id++)).HasError());
    return vertex;
  }

  VertexAccessor CreateVertexWithoutProperties(Storage::Accessor *accessor) {
    VertexAccessor vertex = accessor->CreateVertex();
    return vertex;
  }

  EdgeAccessor CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, Storage::Accessor *accessor) {
    auto edge = accessor->CreateEdge(from, to, edge_type);
    MG_ASSERT(!edge.HasError());
    MG_ASSERT(!edge->SetProperty(this->prop_id, PropertyValue(vertex_id++)).HasError());
    return edge.GetValue();
  }

  template <class TIterable>
  std::vector<int64_t> GetIds(TIterable iterable, View view = View::OLD) {
    std::vector<int64_t> ret;
    for (auto item : iterable) {
      ret.push_back(item.GetProperty(this->prop_id, view)->ValueInt());
    }
    return ret;
  }

 private:
  int vertex_id;
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;

TYPED_TEST_CASE(IndexTest, StorageTypes);
// TYPED_TEST_CASE(IndexTest, InMemoryStorageType);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexCreate) {
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 10; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }

    /// Vertices needs to go to label index rocksdb
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc->Abort();
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 10; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexDrop) {
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->DropIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_TRUE(unique_acc->DropIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 10; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_TRUE(acc->LabelIndexExists(this->label1));
    EXPECT_THAT(acc->ListAllIndices().label, UnorderedElementsAre(this->label1));
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexBasic) {
  // The following steps are performed and index correctness is validated after
  // each step:
  // 1. Create 10 vertices numbered from 0 to 9.
  // 2. Add Label1 to odd numbered, and Label2 to even numbered vertices.
  // 3. Remove Label1 from odd numbered vertices, and add it to even numbered
  //    vertices.
  // 4. Delete even numbered vertices.
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label2).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  auto acc = this->storage->Access(ReplicationRole::MAIN);
  EXPECT_THAT(acc->ListAllIndices().label, UnorderedElementsAre(this->label1, this->label2));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::NEW), View::NEW), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  acc->AdvanceCommand();
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.RemoveLabel(this->label1));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::NEW), View::NEW), IsEmpty());

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexDuplicateVersions) {
  // By removing labels and adding them again we create duplicate entries for
  // the same vertex in the index (they only differ by the timestamp). This test
  // checks that duplicates are properly filtered out.
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label2).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 5; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.RemoveLabel(this->label1));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
// passes
TYPED_TEST(IndexTest, LabelIndexTransactionalIsolation) {
  // Check that transactions only see entries they are supposed to see.
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label2).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  auto acc_before = this->storage->Access(ReplicationRole::MAIN);
  auto acc = this->storage->Access(ReplicationRole::MAIN);
  auto acc_after = this->storage->Access(ReplicationRole::MAIN);

  for (int i = 0; i < 5; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  ASSERT_NO_ERROR(acc->Commit());

  auto acc_after_commit = this->storage->Access(ReplicationRole::MAIN);

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after_commit->Vertices(this->label1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexCountEstimate) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label2).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 3 ? this->label1 : this->label2));
    }

    EXPECT_EQ(acc->ApproximateVertexCount(this->label1), 13);
    EXPECT_EQ(acc->ApproximateVertexCount(this->label2), 7);
  }
}

TYPED_TEST(IndexTest, LabelIndexDeletedVertex) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->Commit());
    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = acc2->DeleteVertex(&*vertex_to_delete);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->Commit());
    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1));
  }
}

TYPED_TEST(IndexTest, LabelIndexRemoveIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc1->Commit());
    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc2->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = vertex_to_delete->RemoveLabel(this->label1);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->Commit());
    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1));
  }
}

TYPED_TEST(IndexTest, LabelIndexRemoveAndAddIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc1->Commit());
    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc2->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res_remove = vertex_to_delete->RemoveLabel(this->label1);
    ASSERT_FALSE(res_remove.HasError());
    auto res_add = vertex_to_delete->AddLabel(this->label1);
    ASSERT_FALSE(res_add.HasError());
    ASSERT_NO_ERROR(acc2->Commit());
    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
  }
}

TYPED_TEST(IndexTest, LabelIndexClearOldDataFromDisk) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    auto *disk_label_index =
        static_cast<memgraph::storage::DiskLabelIndex *>(this->storage->indices_.label_index_.get());

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(10)));
    ASSERT_NO_ERROR(acc1->Commit());

    auto *tx_db = disk_label_index->GetRocksDBStorage()->db_;
    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex2 = acc2->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex2.SetProperty(this->prop_val, memgraph::storage::PropertyValue(10)).HasValue());
    ASSERT_FALSE(acc2->Commit().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex3 = acc3->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex3.SetProperty(this->prop_val, memgraph::storage::PropertyValue(15)).HasValue());
    ASSERT_FALSE(acc3->Commit().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexCreateAndDrop) {
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_EQ(acc->ListAllIndices().label_property.size(), 0);
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_id).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_TRUE(acc->LabelPropertyIndexExists(this->label1, this->prop_id));
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(acc->ListAllIndices().label_property,
                UnorderedElementsAre(std::make_pair(this->label1, this->prop_id)));
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelPropertyIndexExists(this->label2, this->prop_id));
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_TRUE(unique_acc->CreateIndex(this->label1, this->prop_id).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(acc->ListAllIndices().label_property,
                UnorderedElementsAre(std::make_pair(this->label1, this->prop_id)));
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label2, this->prop_id).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_TRUE(acc->LabelPropertyIndexExists(this->label2, this->prop_id));
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(
        acc->ListAllIndices().label_property,
        UnorderedElementsAre(std::make_pair(this->label1, this->prop_id), std::make_pair(this->label2, this->prop_id)));
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->DropIndex(this->label1, this->prop_id).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelPropertyIndexExists(this->label1, this->prop_id));
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(acc->ListAllIndices().label_property,
                UnorderedElementsAre(std::make_pair(this->label2, this->prop_id)));
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_TRUE(unique_acc->DropIndex(this->label1, this->prop_id).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->DropIndex(this->label2, this->prop_id).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_FALSE(acc->LabelPropertyIndexExists(this->label2, this->prop_id));
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_EQ(acc->ListAllIndices().label_property.size(), 0);
  }
}

// The following three tests are almost an exact copy-paste of the corresponding
// label index tests. We request all vertices with given label and property from
// the index, without range filtering. Range filtering is tested in a separate
// test.

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexBasic) {
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label2, this->prop_val).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  auto acc = this->storage->Access(ReplicationRole::MAIN);
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD), IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::OLD), View::OLD), IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue()));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::NEW), View::NEW), IsEmpty());

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD), IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::OLD), View::OLD), IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, this->prop_val, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexDuplicateVersions) {
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 5; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue()));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(42)));
    }
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexTransactionalIsolation) {
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  auto acc_before = this->storage->Access(ReplicationRole::MAIN);
  auto acc = this->storage->Access(ReplicationRole::MAIN);
  auto acc_after = this->storage->Access(ReplicationRole::MAIN);

  for (int i = 0; i < 5; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

  ASSERT_NO_ERROR(acc->Commit());

  auto acc_after_commit = this->storage->Access(ReplicationRole::MAIN);

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, this->prop_val, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after_commit->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexFiltering) {
  // We insert vertices with values:
  // 0 0.0 1 1.0 2 2.0 3 3.0 4 4.0
  // Then we check all combinations of inclusive and exclusive bounds.
  // We also have a mix of doubles and integers to verify that they are sorted
  // properly.

  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);

    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, i % 2 ? PropertyValue(i / 2) : PropertyValue(i / 2.0)));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 5; ++i) {
      EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, PropertyValue(i), View::OLD)),
                  UnorderedElementsAre(2 * i, 2 * i + 1));
    }

    // [1, +inf>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, this->prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                   std::nullopt, View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5, 6, 7, 8, 9));

    // <1, +inf>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, this->prop_val, memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                   std::nullopt, View::OLD)),
        UnorderedElementsAre(4, 5, 6, 7, 8, 9));

    // <-inf, 3]
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, std::nullopt,
                                           memgraph::utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7));

    // <-inf, 3>
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, this->prop_val, std::nullopt,
                                           memgraph::utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5));

    // [1, 3]
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, this->prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5, 6, 7));

    // <1, 3]
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, this->prop_val, memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(4, 5, 6, 7));

    // [1, 3>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, this->prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5));

    // <1, 3>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, this->prop_val, memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(4, 5));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexCountEstimate) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 1; i <= 10; ++i) {
      for (int j = 0; j < i; ++j) {
        auto vertex = this->CreateVertex(acc.get());
        ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
        ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
      }
    }

    EXPECT_EQ(acc->ApproximateVertexCount(this->label1, this->prop_val), 55);
    for (int i = 1; i <= 10; ++i) {
      EXPECT_EQ(acc->ApproximateVertexCount(this->label1, this->prop_val, PropertyValue(i)), i);
    }

    EXPECT_EQ(
        acc->ApproximateVertexCount(this->label1, this->prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(2)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(6))),
        2 + 3 + 4 + 5 + 6);
  }
}

TYPED_TEST(IndexTest, LabelPropertyIndexMixedIteration) {
  {
    auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
    EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
    ASSERT_NO_ERROR(unique_acc->Commit());
  }

  const std::array temporals{TemporalData{TemporalType::Date, 23}, TemporalData{TemporalType::Date, 28},
                             TemporalData{TemporalType::LocalDateTime, 20}};
  const std::array zoned_temporals{
      ZonedTemporalData{ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(20000),
                        memgraph::utils::DefaultTimezone()},
      ZonedTemporalData{ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(30000),
                        memgraph::utils::DefaultTimezone()},
      ZonedTemporalData{ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(40000),
                        memgraph::utils::DefaultTimezone()}};

  std::vector<PropertyValue> values = {
      PropertyValue(false),
      PropertyValue(true),
      PropertyValue(-std::numeric_limits<double>::infinity()),
      PropertyValue(std::numeric_limits<int64_t>::min()),
      PropertyValue(-1),
      PropertyValue(-0.5),
      PropertyValue(0),
      PropertyValue(0.5),
      PropertyValue(1),
      PropertyValue(1.5),
      PropertyValue(2),
      PropertyValue(std::numeric_limits<int64_t>::max()),
      PropertyValue(std::numeric_limits<double>::infinity()),
      PropertyValue(""),
      PropertyValue("a"),
      PropertyValue("b"),
      PropertyValue("c"),
      PropertyValue(std::vector<PropertyValue>()),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(2)}),
      PropertyValue(std::map<std::string, PropertyValue>()),
      PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
      PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}}),
      PropertyValue(temporals[0]),
      PropertyValue(temporals[1]),
      PropertyValue(temporals[2]),
      PropertyValue(zoned_temporals[0]),
      PropertyValue(zoned_temporals[1]),
      PropertyValue(zoned_temporals[2]),
  };

  // Create vertices, each with one of the values above.
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (const auto &value : values) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(this->label1).HasValue());
      ASSERT_TRUE(v.SetProperty(this->prop_val, value).HasValue());
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Verify that all nodes are in the index.
  {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    auto iterable = acc->Vertices(this->label1, this->prop_val, View::OLD);
    auto it = iterable.begin();
    for (const auto &value : values) {
      ASSERT_NE(it, iterable.end());
      auto vertex = *it;
      auto maybe_value = vertex.GetProperty(this->prop_val, View::OLD);
      ASSERT_TRUE(maybe_value.HasValue());
      ASSERT_EQ(value, *maybe_value);
      ++it;
    }
    ASSERT_EQ(it, iterable.end());
  }

  auto verify = [&](const std::optional<memgraph::utils::Bound<PropertyValue>> &from,
                    const std::optional<memgraph::utils::Bound<PropertyValue>> &to,
                    const std::vector<PropertyValue> &expected) {
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    auto iterable = acc->Vertices(this->label1, this->prop_val, from, to, View::OLD);
    size_t i = 0;
    for (auto it = iterable.begin(); it != iterable.end(); ++it, ++i) {
      auto vertex = *it;
      auto maybe_value = vertex.GetProperty(this->prop_val, View::OLD);
      ASSERT_TRUE(maybe_value.HasValue());
      ASSERT_EQ(*maybe_value, expected[i]);
    }
    ASSERT_EQ(i, expected.size());
  };

  // Range iteration with two specified bounds that have the same type should
  // yield the naturally expected items.
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundExclusive(PropertyValue(true)), {});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundInclusive(PropertyValue(true)), {PropertyValue(true)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundExclusive(PropertyValue(true)), {PropertyValue(false)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundInclusive(PropertyValue(true)), {PropertyValue(false), PropertyValue(true)});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(0)), memgraph::utils::MakeBoundExclusive(PropertyValue(1.8)),
         {PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(0)), memgraph::utils::MakeBoundInclusive(PropertyValue(1.8)),
         {PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(0)), memgraph::utils::MakeBoundExclusive(PropertyValue(1.8)),
         {PropertyValue(0), PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(0)), memgraph::utils::MakeBoundInclusive(PropertyValue(1.8)),
         {PropertyValue(0), PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundExclusive(PropertyValue("memgraph")), {PropertyValue("c")});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundInclusive(PropertyValue("memgraph")), {PropertyValue("c")});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundExclusive(PropertyValue("memgraph")), {PropertyValue("b"), PropertyValue("c")});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundInclusive(PropertyValue("memgraph")), {PropertyValue("b"), PropertyValue("c")});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
          PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
          PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});

  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(TemporalData{TemporalType::Date, 200})),
         // LocalDateTime has a "higher" type number so it is not part of the range
         {PropertyValue(temporals[1])});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[2])),
         {PropertyValue(temporals[1]), PropertyValue(temporals[2])});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundExclusive(PropertyValue(temporals[2])),
         {PropertyValue(temporals[0]), PropertyValue(temporals[1])});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[2])),
         {PropertyValue(temporals[0]), PropertyValue(temporals[1]), PropertyValue(temporals[2])});

  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(zoned_temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(zoned_temporals[2])),
         {PropertyValue(zoned_temporals[0]), PropertyValue(zoned_temporals[1]), PropertyValue(zoned_temporals[2])});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(zoned_temporals[0])),
         memgraph::utils::MakeBoundExclusive(PropertyValue(zoned_temporals[2])), {PropertyValue(zoned_temporals[1])});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(zoned_temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(zoned_temporals[2])),
         {PropertyValue(zoned_temporals[1]), PropertyValue(zoned_temporals[2])});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(zoned_temporals[0])),
         memgraph::utils::MakeBoundExclusive(PropertyValue(zoned_temporals[2])),
         {PropertyValue(zoned_temporals[0]), PropertyValue(zoned_temporals[1])});

  // Range iteration with one unspecified bound should only yield items that
  // have the same type as the specified bound.
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(false)), std::nullopt,
         {PropertyValue(false), PropertyValue(true)});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(true)), {PropertyValue(false)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(1)), std::nullopt,
         {PropertyValue(1), PropertyValue(1.5), PropertyValue(2), PropertyValue(std::numeric_limits<int64_t>::max()),
          PropertyValue(std::numeric_limits<double>::infinity())});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(0)),
         {PropertyValue(-std::numeric_limits<double>::infinity()), PropertyValue(std::numeric_limits<int64_t>::min()),
          PropertyValue(-1), PropertyValue(-0.5)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue("b")), std::nullopt,
         {PropertyValue("b"), PropertyValue("c")});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue("b")),
         {PropertyValue(""), PropertyValue("a")});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(false)})),
         std::nullopt,
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
          PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(1)})),
         {PropertyValue(std::vector<PropertyValue>()), PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(false)}})),
         std::nullopt,
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(std::nullopt,
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(7.5)}})),
         {PropertyValue(std::map<std::string, PropertyValue>()),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}})});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(TemporalData(TemporalType::Date, 10))), std::nullopt,
         {PropertyValue(temporals[0]), PropertyValue(temporals[1]), PropertyValue(temporals[2])});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(TemporalData(TemporalType::Duration, 0))),
         {PropertyValue(temporals[0]), PropertyValue(temporals[1]), PropertyValue(temporals[2])});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(zoned_temporals[0])), std::nullopt,
         {PropertyValue(zoned_temporals[0]), PropertyValue(zoned_temporals[1]), PropertyValue(zoned_temporals[2])});
  verify(std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(zoned_temporals[0])),
         {PropertyValue(zoned_temporals[0])});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(zoned_temporals[0])), std::nullopt,
         {PropertyValue(zoned_temporals[1]), PropertyValue(zoned_temporals[2])});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(zoned_temporals[0])), {});

  // Range iteration with two specified bounds that don't have the same type
  // should yield no items.
  for (size_t i = 0; i < values.size(); ++i) {
    for (size_t j = i; j < values.size(); ++j) {
      if (PropertyValue::AreComparableTypes(values[i].type(), values[j].type())) {
        verify(memgraph::utils::MakeBoundInclusive(values[i]), memgraph::utils::MakeBoundInclusive(values[j]),
               {values.begin() + i, values.begin() + j + 1});
      } else {
        verify(memgraph::utils::MakeBoundInclusive(values[i]), memgraph::utils::MakeBoundInclusive(values[j]), {});
      }
    }
  }

  // Iteration without any bounds should return all items of the index.
  verify(std::nullopt, std::nullopt, values);
}

TYPED_TEST(IndexTest, LabelPropertyIndexDeletedVertex) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);

    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop_val, PropertyValue(0)));

    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop_val, PropertyValue(1)));

    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = acc2->DeleteVertex(&*vertex_to_delete);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->Commit());

    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
                UnorderedElementsAre(1));
  }
}

/// TODO: empty lines, so it is easier to read what is actually going on here
TYPED_TEST(IndexTest, LabelPropertyIndexRemoveIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);

    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop_val, PropertyValue(0)));

    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop_val, PropertyValue(1)));

    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = vertex_to_delete->RemoveLabel(this->label1);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->Commit());

    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, this->prop_val, View::NEW), View::NEW),
                UnorderedElementsAre(1));
  }
}

TYPED_TEST(IndexTest, LabelPropertyIndexRemoveAndAddIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);

    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop_val, PropertyValue(0)));

    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop_val, PropertyValue(1)));

    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    auto target_vertex = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto remove_res = target_vertex->RemoveLabel(this->label1);
    ASSERT_FALSE(remove_res.HasError());
    auto add_res = target_vertex->AddLabel(this->label1);
    ASSERT_FALSE(add_res.HasError());
    ASSERT_NO_ERROR(acc2->Commit());
  }
}

TYPED_TEST(IndexTest, LabelPropertyIndexClearOldDataFromDisk) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    auto *disk_label_property_index =
        static_cast<memgraph::storage::DiskLabelPropertyIndex *>(this->storage->indices_.label_property_index_.get());

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->label1, this->prop_val).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    auto acc1 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(10)));
    ASSERT_NO_ERROR(acc1->Commit());

    auto *tx_db = disk_label_property_index->GetRocksDBStorage()->db_;
    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc2 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex2 = acc2->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex2.SetProperty(this->prop_val, memgraph::storage::PropertyValue(10)).HasValue());
    ASSERT_FALSE(acc2->Commit().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc3 = this->storage->Access(ReplicationRole::MAIN);
    auto vertex3 = acc3->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex3.SetProperty(this->prop_val, memgraph::storage::PropertyValue(15)).HasValue());
    ASSERT_FALSE(acc3->Commit().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexCreate) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_FALSE(acc->EdgeTypeIndexExists(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().edge_type.size(), 0);
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      for (int i = 0; i < 10; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }
      ASSERT_NO_ERROR(acc->Commit());
    }

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      for (int i = 10; i < 20; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

      acc->AdvanceCommand();

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

      acc->Abort();
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      for (int i = 10; i < 20; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

      acc->AdvanceCommand();

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

      ASSERT_NO_ERROR(acc->Commit());
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

      acc->AdvanceCommand();

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

      ASSERT_NO_ERROR(acc->Commit());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexDrop) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_FALSE(acc->EdgeTypeIndexExists(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().edge_type.size(), 0);
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      for (int i = 0; i < 10; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }
      ASSERT_NO_ERROR(acc->Commit());
    }

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
    }

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->DropIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_FALSE(acc->EdgeTypeIndexExists(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
    }

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_TRUE(unique_acc->DropIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_FALSE(acc->EdgeTypeIndexExists(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      for (int i = 10; i < 20; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }
      ASSERT_NO_ERROR(acc->Commit());
    }

    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);
      EXPECT_TRUE(acc->EdgeTypeIndexExists(this->edge_type_id1));
      EXPECT_THAT(acc->ListAllIndices().edge_type, UnorderedElementsAre(this->edge_type_id1));
    }

    {
      auto acc = this->storage->Access(ReplicationRole::MAIN);

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

      acc->AdvanceCommand();

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexBasic) {
  // The following steps are performed and index correctness is validated after
  // each step:
  // 1. Create 10 edges numbered from 0 to 9.
  // 2. Add EdgeType1 to odd numbered, and EdgeType2 to even numbered edges.
  // 3. Delete even numbered edges.
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id2).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    auto acc = this->storage->Access(ReplicationRole::MAIN);
    EXPECT_THAT(acc->ListAllIndices().edge_type, UnorderedElementsAre(this->edge_type_id1, this->edge_type_id2));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::NEW), View::NEW), IsEmpty());

    for (int i = 0; i < 10; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::NEW), View::NEW),
                UnorderedElementsAre(0, 2, 4, 6, 8));

    acc->AdvanceCommand();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::OLD), View::OLD),
                UnorderedElementsAre(0, 2, 4, 6, 8));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::NEW), View::NEW),
                UnorderedElementsAre(0, 2, 4, 6, 8));

    for (auto vertex : acc->Vertices(View::OLD)) {
      auto edges = vertex.OutEdges(View::OLD)->edges;
      for (auto &edge : edges) {
        int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
        if (id % 2 == 0) {
          ASSERT_NO_ERROR(acc->DetachDelete({}, {&edge}, false));
        }
      }
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::OLD), View::OLD),
                UnorderedElementsAre(0, 2, 4, 6, 8));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::NEW), View::NEW), IsEmpty());

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, View::NEW), View::NEW), IsEmpty());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexTransactionalIsolation) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    // Check that transactions only see entries they are supposed to see.
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id2).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    auto acc_before = this->storage->Access(ReplicationRole::MAIN);
    auto acc = this->storage->Access(ReplicationRole::MAIN);
    auto acc_after = this->storage->Access(ReplicationRole::MAIN);

    for (int i = 0; i < 5; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());

    EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());

    ASSERT_NO_ERROR(acc->Commit());

    auto acc_after_commit = this->storage->Access(ReplicationRole::MAIN);

    EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());

    EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());

    EXPECT_THAT(this->GetIds(acc_after_commit->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexCountEstimate) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id2).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    auto acc = this->storage->Access(ReplicationRole::MAIN);
    for (int i = 0; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      this->CreateEdge(&vertex_from, &vertex_to, i % 3 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
    }

    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 13);
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id2), 7);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexRepeatingEdgeTypesBetweenSameVertices) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto unique_acc = this->storage->UniqueAccess(ReplicationRole::MAIN);
      EXPECT_FALSE(unique_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(unique_acc->Commit());
    }

    auto acc = this->storage->Access(ReplicationRole::MAIN);
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());

    for (int i = 0; i < 5; ++i) {
      this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    }

    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 5);

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}
