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

#include <gmock/gmock.h>
#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-type-util.h>

#include "disk_test_utils.hpp"
#include "flags/general.hpp"
#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "storage/v2/temporal.hpp"
#include "storage_test_utils.hpp"
#include "utils/rocksdb_serialization.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;
using testing::IsEmpty;
using testing::Types;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

namespace pvr {
// Create a PropertyValueRange for testing against property equality
PropertyValueRange Equal(PropertyValue val) {
  auto lower = memgraph::utils::MakeBoundInclusive(std::move(val));
  auto upper = lower;
  return PropertyValueRange::Bounded(std::move(lower), std::move(upper));
}

// Create a PropertyValueRange for testing a property against a range
PropertyValueRange Range(std::optional<memgraph::utils::Bound<PropertyValue>> lower,
                         std::optional<memgraph::utils::Bound<PropertyValue>> upper) {
  return PropertyValueRange::Bounded(std::move(lower), std::move(upper));
}

// Create a PropertyValueRange for testing property existence
PropertyValueRange IsNotNull() { return PropertyValueRange::IsNotNull(); }

}  // namespace pvr

/** Type for  a key-value pair.
 */
using KVPair = std::tuple<PropertyId, PropertyValue>;

/** Creates a map from a (possibly nested) list of `KVPair`s.
 */
template <typename... Ts>
auto MakeMap(Ts &&...values) -> PropertyValue requires(std::is_same_v<std::decay_t<Ts>, KVPair> &&...) {
  return PropertyValue{PropertyValue::map_t{
      {std::get<0>(values),
       std::forward<std::tuple_element_t<1, std::decay_t<Ts>>>(std::get<1>(std::forward<Ts>(values)))}...}};
};

template <typename StorageType>
class IndexTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_storage_properties_on_edges = true;
    config_.salient.items.properties_on_edges = true;
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    this->storage = std::make_unique<StorageType>(config_);
    auto acc = this->storage->Access();
    this->prop_id = acc->NameToProperty("id");
    this->prop_val = acc->NameToProperty("val");
    this->label1 = acc->NameToLabel("label1");
    this->label2 = acc->NameToLabel("label2");
    this->edge_type_id1 = acc->NameToEdgeType("edge_type_1");
    this->edge_type_id2 = acc->NameToEdgeType("edge_type_2");
    this->edge_prop_id1 = acc->NameToProperty("edge_prop_id1");
    this->edge_prop_id2 = acc->NameToProperty("edge_prop_id2");
    this->prop_a = acc->NameToProperty("prop_a");
    this->prop_b = acc->NameToProperty("prop_b");
    this->prop_c = acc->NameToProperty("prop_c");
    this->prop_d = acc->NameToProperty("prop_d");
    vertex_id = 0;
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    this->storage.reset(nullptr);
  }

  auto CreateIndexAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      return this->storage->ReadOnlyAccess();
    } else {
      return this->storage->UniqueAccess();
    }
  }

  auto DropIndexAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      return this->storage->Access(memgraph::storage::Storage::Accessor::Type::READ);
    } else {
      return this->storage->UniqueAccess();
    }
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
  PropertyId edge_prop_id1;
  PropertyId edge_prop_id2;
  PropertyId prop_a;
  PropertyId prop_b;
  PropertyId prop_c;
  PropertyId prop_d;

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

TYPED_TEST_SUITE(IndexTest, StorageTypes);
// TYPED_TEST_SUITE(IndexTest, InMemoryStorageType);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexCreate) {
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexReady(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = this->storage->Access();
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
    auto acc = this->storage->Access();
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

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexDrop) {
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexReady(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexReady(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_TRUE(acc->DropIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexReady(this->label1));
    EXPECT_EQ(acc->ListAllIndices().label.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelIndexReady(this->label1));
    EXPECT_THAT(acc->ListAllIndices().label, UnorderedElementsAre(this->label1));
  }

  {
    auto acc = this->storage->Access();

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
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
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
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 5; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
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
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc_before = this->storage->Access();
  auto acc = this->storage->Access();
  auto acc_after = this->storage->Access();

  for (int i = 0; i < 5; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  auto acc_after_commit = this->storage->Access();

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after_commit->Vertices(this->label1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexCountEstimate) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label2).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }

    auto acc = this->storage->Access();
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
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();
    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());
    auto acc2 = this->storage->Access();
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = acc2->DeleteVertex(&*vertex_to_delete);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase());
    auto acc3 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1));
  }
}

TYPED_TEST(IndexTest, LabelIndexRemoveIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();
    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());
    auto acc2 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc2->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = vertex_to_delete->RemoveLabel(this->label1);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase());
    auto acc3 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1));
  }
}

TYPED_TEST(IndexTest, LabelIndexRemoveAndAddIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();
    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());
    auto acc2 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc2->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res_remove = vertex_to_delete->RemoveLabel(this->label1);
    ASSERT_FALSE(res_remove.HasError());
    auto res_add = vertex_to_delete->AddLabel(this->label1);
    ASSERT_FALSE(res_add.HasError());
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase());
    auto acc3 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
  }
}

TYPED_TEST(IndexTest, LabelIndexClearOldDataFromDisk) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    auto *disk_label_index =
        static_cast<memgraph::storage::DiskLabelIndex *>(this->storage->indices_.label_index_.get());

    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();
    auto vertex = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(10)));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());

    auto *tx_db = disk_label_index->GetRocksDBStorage()->db_;
    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc2 = this->storage->Access();
    auto vertex2 = acc2->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex2.SetProperty(this->prop_val, memgraph::storage::PropertyValue(10)).HasValue());
    ASSERT_FALSE(acc2->PrepareForCommitPhase().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc3 = this->storage->Access();
    auto vertex3 = acc3->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex3.SetProperty(this->prop_val, memgraph::storage::PropertyValue(15)).HasValue());
    ASSERT_FALSE(acc3->PrepareForCommitPhase().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexCreateAndDrop) {
  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllIndices().label_properties.size(), 0);
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_id}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelPropertyIndexReady(this->label1, std::array{PropertyPath{this->prop_id}}));
  }
  {
    auto acc = this->storage->Access();
    EXPECT_THAT(
        acc->ListAllIndices().label_properties,
        UnorderedElementsAre(std::make_pair(this->label1, std::vector<PropertyPath>{PropertyPath{this->prop_id}})));
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexReady(this->label2, std::array{PropertyPath{this->prop_id}}));
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_TRUE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_id}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(
        acc->ListAllIndices().label_properties,
        UnorderedElementsAre(std::make_pair(this->label1, std::vector<PropertyPath>{PropertyPath{this->prop_id}})));
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2, {PropertyPath{this->prop_id}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelPropertyIndexReady(this->label2, std::array{PropertyPath{this->prop_id}}));
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(
        acc->ListAllIndices().label_properties,
        UnorderedElementsAre(std::make_pair(this->label1, std::vector<PropertyPath>{PropertyPath{this->prop_id}}),
                             std::make_pair(this->label2, std::vector<PropertyPath>{PropertyPath{this->prop_id}})));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropIndex(this->label1, {PropertyPath{this->prop_id}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexReady(this->label1, std::array{PropertyPath{this->prop_id}}));
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(
        acc->ListAllIndices().label_properties,
        UnorderedElementsAre(std::make_pair(this->label2, std::vector<PropertyPath>{PropertyPath{this->prop_id}})));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_TRUE(acc->DropIndex(this->label1, {PropertyPath{this->prop_id}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropIndex(this->label2, {PropertyPath{this->prop_id}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexReady(this->label2, std::array{PropertyPath{this->prop_id}}));
  }

  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllIndices().label_properties.size(), 0);
  }
}

TYPED_TEST(IndexTest, LabelPropertyCompositeIndexCreateAndDrop) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    GTEST_SKIP();
  }

  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllIndices().label_properties.size(), 0);
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1,
                                  {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelPropertyIndexReady(
        this->label1, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}}));
  }
  {
    auto acc = this->storage->Access();
    EXPECT_THAT(acc->ListAllIndices().label_properties,
                UnorderedElementsAre(std::make_pair(
                    this->label1,
                    std::vector{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})));
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexReady(
        this->label2, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}}));
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_TRUE(acc->CreateIndex(this->label1,
                                 {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})
                    .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(acc->ListAllIndices().label_properties,
                UnorderedElementsAre(std::make_pair(
                    this->label1,
                    std::vector{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})));
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2,
                                  {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelPropertyIndexReady(
        this->label2, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}}));
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(acc->ListAllIndices().label_properties,
                UnorderedElementsAre(
                    std::make_pair(this->label1, std::vector{PropertyPath{this->prop_a}, PropertyPath{this->prop_b},
                                                             PropertyPath{this->prop_c}}),
                    std::make_pair(this->label2, std::vector{PropertyPath{this->prop_a}, PropertyPath{this->prop_b},
                                                             PropertyPath{this->prop_c}})));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropIndex(this->label1,
                                {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexReady(
        this->label1, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}}));
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(acc->ListAllIndices().label_properties,
                UnorderedElementsAre(std::make_pair(
                    this->label2,
                    std::vector{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_TRUE(acc->DropIndex(this->label1,
                               {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})
                    .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropIndex(this->label2,
                                {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexReady(
        this->label2, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}, PropertyPath{this->prop_c}}));
  }

  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllIndices().label_properties.size(), 0);
  }
}

// The following three tests are almost an exact copy-paste of the corresponding
// label index tests. We request all vertices with given label and property from
// the index, without range filtering. Range filtering is tested in a separate
// test.

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexBasic) {
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue()));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());
}

TYPED_TEST(IndexTest, LabelPropertyCompositeIndexBasic) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    GTEST_SKIP();
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1,
                                  {PropertyPath{this->prop_b}, PropertyPath{this->prop_a}, PropertyPath{this->prop_c}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label2, {PropertyPath{this->prop_c}, PropertyPath{this->prop_b}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  for (int i = 0; i < 10; ++i) {
    // Populates the vertices labels and properties as follows:
    // vertex: 0 1 2 3 4 5 6 7 8 9
    //  label: 2 1 2 1 2 1 2 1 2 1
    // prop_a: 0 1 2 3 4 5 6 7 8 9
    // prop_b: 0     3     6     9
    // prop_c:           5 6 7 8 9

    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, PropertyValue(i)));
    if (i % 3 == 0) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, PropertyValue(i)));
    }
    if (i >= 5) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, PropertyValue(i)));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              IsEmpty());

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull()}, View::OLD),
                   View::OLD),
      IsEmpty());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(3, 9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull()}, View::NEW),
                   View::NEW),
      UnorderedElementsAre(6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull(), pvr::IsNotNull(), pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull(), pvr::IsNotNull()}, View::NEW),
                   View::NEW),
      UnorderedElementsAre(6));

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(3, 9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull()}, View::OLD),
                   View::OLD),
      UnorderedElementsAre(6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull(), pvr::IsNotNull(), pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull(), pvr::IsNotNull()}, View::OLD),
                   View::OLD),
      UnorderedElementsAre(6));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(3, 9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull()}, View::NEW),
                   View::NEW),
      UnorderedElementsAre(6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull(), pvr::IsNotNull(), pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull(), pvr::IsNotNull()}, View::NEW),
                   View::NEW),
      UnorderedElementsAre(6));

  for (auto vertex : acc->Vertices(View::OLD)) {
    if (vertex.Gid().AsUint() % 2 == 0) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(3, 9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull()}, View::OLD),
                   View::OLD),
      UnorderedElementsAre(6, 8));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull(), pvr::IsNotNull(), pvr::IsNotNull()}, View::OLD),
                           View::OLD),
              UnorderedElementsAre(9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull(), pvr::IsNotNull()}, View::OLD),
                   View::OLD),
      UnorderedElementsAre(6));

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(3, 9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull()}, View::NEW),
                   View::NEW),
      UnorderedElementsAre());

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1,
                                         std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a},
                                                    PropertyPath{this->prop_c}},
                                         std::array{pvr::IsNotNull(), pvr::IsNotNull(), pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(9));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label2, std::array{PropertyPath{this->prop_c}, PropertyPath{this->prop_b}},
                                 std::array{pvr::IsNotNull(), pvr::IsNotNull()}, View::NEW),
                   View::NEW),
      UnorderedElementsAre());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexDuplicateVersions) {
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 5; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                           std::array{pvr::IsNotNull()}, View::NEW),
                             View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                           std::array{pvr::IsNotNull()}, View::OLD),
                             View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue()));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                           std::array{pvr::IsNotNull()}, View::OLD),
                             View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                           std::array{pvr::IsNotNull()}, View::NEW),
                             View::NEW),
                IsEmpty());

    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(42)));
    }
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                           std::array{pvr::IsNotNull()}, View::OLD),
                             View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                           std::array{pvr::IsNotNull()}, View::NEW),
                             View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexStrictInsert) {
  if (this->storage->storage_mode_ == StorageMode::ON_DISK_TRANSACTIONAL) {
    GTEST_SKIP_("Skip for ON_DISK_TRANSACTIONAL, we currently can not get the count of the index");
  }

  {
    auto acc = this->CreateIndexAccessor();
    ASSERT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();

  ASSERT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_val}}), 0);
  {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(42)));
    ASSERT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_val}}), 1);
  }

  {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label2));  // NOTE: this is not label1
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(42)));
    // We expect index for label1+id to be uneffected
    ASSERT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_val}}), 1);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexTransactionalIsolation) {
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc_before = this->storage->Access();
  auto acc = this->storage->Access();
  auto acc_after = this->storage->Access();

  for (int i = 0; i < 5; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                         std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                                std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                               std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  auto acc_after_commit = this->storage->Access();

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                                std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                               std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after_commit->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                                      std::array{pvr::IsNotNull()}, View::NEW),
                           View::NEW),
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
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();

    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, i % 2 ? PropertyValue(i / 2) : PropertyValue(i / 2.0)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 5; ++i) {
      EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                             std::array{pvr::Equal(PropertyValue(i))}, View::OLD)),
                  UnorderedElementsAre(2 * i, 2 * i + 1));
    }

    // [1, +inf>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(
            this->label1, std::array{PropertyPath{this->prop_val}},
            std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(1)), std::nullopt)}, View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5, 6, 7, 8, 9));

    // <1, +inf>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(
            this->label1, std::array{PropertyPath{this->prop_val}},
            std::array{pvr::Range(memgraph::utils::MakeBoundExclusive(PropertyValue(1)), std::nullopt)}, View::OLD)),
        UnorderedElementsAre(4, 5, 6, 7, 8, 9));

    // <-inf, 3]
    EXPECT_THAT(
        this->GetIds(acc->Vertices(
            this->label1, std::array{PropertyPath{this->prop_val}},
            std::array{pvr::Range(std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(3)))}, View::OLD)),
        UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7));

    // <-inf, 3>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(
            this->label1, std::array{PropertyPath{this->prop_val}},
            std::array{pvr::Range(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(3)))}, View::OLD)),
        UnorderedElementsAre(0, 1, 2, 3, 4, 5));

    // [1, 3]
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                   std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                                         memgraph::utils::MakeBoundInclusive(PropertyValue(3)))},
                                   View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5, 6, 7));

    // <1, 3]
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                   std::array{pvr::Range(memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                                         memgraph::utils::MakeBoundInclusive(PropertyValue(3)))},
                                   View::OLD)),
        UnorderedElementsAre(4, 5, 6, 7));

    // [1, 3>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                   std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                                         memgraph::utils::MakeBoundExclusive(PropertyValue(3)))},
                                   View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5));

    // <1, 3>
    EXPECT_THAT(
        this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                   std::array{pvr::Range(memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                                         memgraph::utils::MakeBoundExclusive(PropertyValue(3)))},
                                   View::OLD)),
        UnorderedElementsAre(4, 5));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexCountEstimate) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Not supported on disk";
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  for (int i = 1; i <= 10; ++i) {
    for (int j = 0; j < i; ++j) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(i)));
    }
  }

  EXPECT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_val}}), 55);
  for (int i = 1; i <= 10; ++i) {
    EXPECT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_val}},
                                          std::array{PropertyValue(i)}),
              i);
  }

  EXPECT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_val}},
                                        std::array{memgraph::storage::PropertyValueRange::Bounded(
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(2)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(6)))}),
            2 + 3 + 4 + 5 + 6);
}

TYPED_TEST(IndexTest, LabelPropertyCompositeIndexCountEstimate) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Not supported on disk";
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_a}, PropertyPath{this->prop_b}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  for (int i = 1; i <= 10; ++i) {
    for (int j = 0; j < i; ++j) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, PropertyValue{i}));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, PropertyValue{i * 2}));
    }
  }

  // `110` is because each `SetProperty` above adds another entry into the
  // skip list, resulting in an approximation that is twice the actual count.
  EXPECT_EQ(
      acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}}),
      110);
  for (int i = 1; i <= 10; ++i) {
    EXPECT_EQ(
        acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}},
                                    std::array{PropertyValue{i}, PropertyValue{i * 2}}),
        i);
  }

  EXPECT_EQ(
      acc->ApproximateVertexCount(
          this->label1, std::array{PropertyPath{this->prop_a}, PropertyPath{this->prop_b}},
          std::array{
              memgraph::storage::PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(2)),
                                                             memgraph::utils::MakeBoundInclusive(PropertyValue(6))),

              memgraph::storage::PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(4)),
                                                             memgraph::utils::MakeBoundInclusive(PropertyValue(10)))}),
      14);
}

TYPED_TEST(IndexTest, LabelPropertyNestedIndexCountEstimate) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Not supported on disk";
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_a, this->prop_b}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  for (int i = 1; i <= 10; ++i) {
    for (int j = 0; j < i; ++j) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(
          vertex.SetProperty(this->prop_a, PropertyValue{PropertyValue::map_t{{this->prop_b, PropertyValue{i}}}}));
    }
  }

  EXPECT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b}}), 55);
  for (int i = 1; i <= 10; ++i) {
    EXPECT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b}},
                                          std::array{PropertyValue(i)}),
              i);
  }

  EXPECT_EQ(acc->ApproximateVertexCount(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b}},
                                        std::array{memgraph::storage::PropertyValueRange::Bounded(
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(2)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(6)))}),
            2 + 3 + 4 + 5 + 6);
}

TYPED_TEST(IndexTest, LabelPropertyIndexMixedIteration) {
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
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
      PropertyValue(PropertyValue::map_t()),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5)}}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}}),
      PropertyValue(temporals[0]),
      PropertyValue(temporals[1]),
      PropertyValue(temporals[2]),
      PropertyValue(zoned_temporals[0]),
      PropertyValue(zoned_temporals[1]),
      PropertyValue(zoned_temporals[2]),
  };

  // Create vertices, each with one of the values above.
  {
    auto acc = this->storage->Access();
    for (const auto &value : values) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(this->label1).HasValue());
      ASSERT_TRUE(v.SetProperty(this->prop_val, value).HasValue());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Verify that all nodes are in the index.
  {
    auto acc = this->storage->Access();
    auto iterable =
        acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}}, std::array{pvr::IsNotNull()}, View::OLD);
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
    auto acc = this->storage->Access();
    auto iterable = acc->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                  std::array{pvr::Range(from, to)}, View::OLD);
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
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue("b")}})),
         {PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundExclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundInclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue("b")}})),
         {PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue("b")}})),
         {PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5)}}),
          PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundInclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue("b")}})),
         {PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5)}}),
          PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}})});

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
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(false)}})),
         std::nullopt,
         {PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5)}}),
          PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}})});
  verify(std::nullopt,
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(7.5)}})),
         {PropertyValue(PropertyValue::map_t()),
          PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5)}})});
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
      if (AreComparableTypes(values[i].type(), values[j].type())) {
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

TYPED_TEST(IndexTest, LabelPropertyCompositeIndexMixedIteration) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    GTEST_SKIP() << "DiskStorage does not support label/property composite indices";
  }

  PropertyId prop_a;
  PropertyId prop_b;

  {
    auto acc = this->storage->Access();
    prop_a = acc->NameToProperty("a");
    prop_b = acc->NameToProperty("b");
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{prop_a}, PropertyPath{prop_b}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto a_values = ranges::views::iota(0, 5) | ranges::views::transform([](int val) { return PropertyValue(val); }) |
                  ranges::to_vector;
  auto b_values =
      std::vector{PropertyValue(2),      PropertyValue(3.0),     PropertyValue(4),         PropertyValue(6.0),
                  PropertyValue("alfa"), PropertyValue("bravo"), PropertyValue("charlie"), PropertyValue()};

  // Create vertices with every cartesian product of a and b values
  {
    auto acc = this->storage->Access();
    for (auto &&[a_val, b_val] : ranges::views::cartesian_product(a_values, b_values)) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(this->label1).HasValue());
      ASSERT_TRUE(v.SetProperty(prop_a, a_val).HasValue());
      ASSERT_TRUE(v.SetProperty(prop_b, b_val).HasValue());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  auto const inclusive_bound = [](PropertyValue val) { return memgraph::utils::MakeBoundInclusive(val); };

  auto bounded = [&](auto &&lower, auto &&upper) {
    return PropertyValueRange::Bounded(inclusive_bound(PropertyValue(lower)), inclusive_bound(PropertyValue(upper)));
  };

  using enum PropertyValueType;

  using PropertyPath = memgraph::storage::PropertyPath;

  auto const test = [&](std::span<PropertyPath const> props,
                        std::span<memgraph::storage::PropertyValueRange const> ranges, size_t expected_num_vertices,
                        auto &&props_validator) {
    EXPECT_EQ(CheckVertexProperties(this->storage->Access(), this->label1, props, ranges, props_validator),
              expected_num_vertices);
  };

  // Check 1 <= n.a <= 3 AND n.b IS NOT NULL
  test(std::array{PropertyPath{prop_a}, PropertyPath{prop_b}},
       std::array{bounded(1, 3), PropertyValueRange::IsNotNull()}, 21, [](std::span<PropertyValue const> values) {
         EXPECT_EQ(values[0].type(), Int);
         EXPECT_TRUE(values[0].ValueInt() >= 1 && values[0].ValueInt() <= 3);
         EXPECT_NE(values[1].type(), Null);
       });

  test(std::array{PropertyPath{prop_a}, PropertyPath{prop_b}}, std::array{bounded(1, 3), bounded("alfa", "bravo")}, 6,
       [](std::span<PropertyValue const> values) {
         EXPECT_EQ(values[0].type(), Int);
         EXPECT_TRUE(values[0].ValueInt() >= 1 && values[0].ValueInt() <= 3);
         EXPECT_EQ(values[1].type(), String);
         EXPECT_TRUE(values[1].ValueString() == "alfa" || values[1].ValueString() == "bravo");
       });

  // Check 1 <= n.a <= 3 AND 1 <= n.b <= 6
  test(std::array{PropertyPath{prop_a}, PropertyPath{prop_b}}, std::array{bounded(1, 3), bounded(1, 6)}, 12,
       [](std::span<PropertyValue const> values) {
         EXPECT_EQ(values[0].type(), Int);
         EXPECT_TRUE(values[0].ValueInt() >= 1 && values[0].ValueInt() <= 3);
         EXPECT_TRUE(values[1].type() == Int || values[1].type() == Double);
         EXPECT_TRUE(values[1].type() == Double || values[1].ValueInt() == 2 || values[1].ValueInt() == 4);
         EXPECT_TRUE(values[1].type() == Int || values[1].ValueDouble() == 3.0 || values[1].ValueDouble() == 6.0);
       });

  // Check 1 <= n.a <= 3
  test(std::array{PropertyPath{prop_a}, PropertyPath{prop_b}}, std::array{bounded(1, 3)}, 24,
       [](std::span<PropertyValue const> values) {
         EXPECT_EQ(values[0].type(), Int);
         EXPECT_TRUE(values[0].ValueInt() >= 1 && values[0].ValueInt() <= 3);
       });

  // Check n.a IS NOT NULL AND 1 <= n.b <= 3
  test(std::array{PropertyPath{prop_a}, PropertyPath{prop_b}},
       std::array{PropertyValueRange::IsNotNull(), bounded(1, 3)}, 10, [](std::span<PropertyValue const> values) {
         EXPECT_NE(values[0].type(), Null);

         auto b_type = values[1].type();
         EXPECT_TRUE(b_type == Int || b_type == Double);
         if (b_type == Int) {
           EXPECT_TRUE(values[1].ValueInt() >= 1 && values[1].ValueInt() <= 3);
         } else {
           EXPECT_TRUE(values[1].ValueDouble() >= 1 && values[1].ValueDouble() <= 3);
         }
       });
}

TYPED_TEST(IndexTest, LabelPropertyIndexDeletedVertex) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();

    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop_val, PropertyValue(0)));

    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop_val, PropertyValue(1)));

    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());

    auto acc2 = this->storage->Access();
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = acc2->DeleteVertex(&*vertex_to_delete);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase());

    auto acc3 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                            std::array{pvr::IsNotNull()}, View::NEW),
                             View::NEW),
                UnorderedElementsAre(1));
  }
}

/// TODO: empty lines, so it is easier to read what is actually going on here
TYPED_TEST(IndexTest, LabelPropertyIndexRemoveIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();

    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop_val, PropertyValue(0)));

    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop_val, PropertyValue(1)));

    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());

    auto acc2 = this->storage->Access();
    auto vertex_to_delete = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto res = vertex_to_delete->RemoveLabel(this->label1);
    ASSERT_FALSE(res.HasError());
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase());

    auto acc3 = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc3->Vertices(this->label1, std::array{PropertyPath{this->prop_val}},
                                            std::array{pvr::IsNotNull()}, View::NEW),
                             View::NEW),
                UnorderedElementsAre(1));
  }
}

TYPED_TEST(IndexTest, LabelPropertyIndexRemoveAndAddIndexedLabel) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();

    auto vertex1 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop_val, PropertyValue(0)));

    auto vertex2 = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop_val, PropertyValue(1)));

    EXPECT_THAT(this->GetIds(acc1->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());

    auto acc2 = this->storage->Access();
    auto target_vertex = acc2->FindVertex(vertex1.Gid(), memgraph::storage::View::NEW);
    auto remove_res = target_vertex->RemoveLabel(this->label1);
    ASSERT_FALSE(remove_res.HasError());
    auto add_res = target_vertex->AddLabel(this->label1);
    ASSERT_FALSE(add_res.HasError());
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase());
  }
}

TYPED_TEST(IndexTest, LabelPropertyIndexClearOldDataFromDisk) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    auto *disk_label_property_index =
        static_cast<memgraph::storage::DiskLabelPropertyIndex *>(this->storage->indices_.label_property_index_.get());

    {
      auto acc = this->CreateIndexAccessor();
      EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    auto acc1 = this->storage->Access();
    auto vertex = this->CreateVertex(acc1.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, PropertyValue(10)));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase());

    auto *tx_db = disk_label_property_index->GetRocksDBStorage()->db_;
    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc2 = this->storage->Access();
    auto vertex2 = acc2->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex2.SetProperty(this->prop_val, memgraph::storage::PropertyValue(10)).HasValue());
    ASSERT_FALSE(acc2->PrepareForCommitPhase().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc3 = this->storage->Access();
    auto vertex3 = acc3->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex3.SetProperty(this->prop_val, memgraph::storage::PropertyValue(15)).HasValue());
    ASSERT_FALSE(acc3->PrepareForCommitPhase().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexCreate) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto acc = this->storage->Access();
      EXPECT_FALSE(acc->EdgeTypeIndexReady(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().edge_type.size(), 0);
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 0);
    }

    {
      auto acc = this->storage->Access();
      for (int i = 0; i < 10; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }

    {
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_EQ(read_only_acc->ApproximateEdgeCount(this->edge_type_id1), 0);
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }

    {
      auto acc = this->storage->Access();
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 5);
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
    }

    {
      auto acc = this->storage->Access();
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

      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 10);
      acc->Abort();
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 5);
    }

    {
      auto acc = this->storage->Access();
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

      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 10);
    }

    {
      auto acc = this->storage->Access();
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

      acc->AdvanceCommand();

      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }

    // Check that GC doesn't remove useful elements
    {
      // Double call to free memory: first run unlinks and marks for cleanup; second run deletes
      this->storage->FreeMemory({}, false);
      this->storage->FreeMemory({}, false);
      auto acc = this->storage->Access();
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    }
    {
      {
        auto acc = this->storage->Access();
        for (auto vertex : acc->Vertices(View::OLD)) {
          auto edges = vertex.OutEdges(View::OLD)->edges;
          for (auto &edge : edges) {
            int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
            if (id % 3 == 0) {
              ASSERT_NO_ERROR(acc->DeleteEdge(&edge));
            }
          }
        }
        ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
      }
      this->storage->FreeMemory({}, false);
      this->storage->FreeMemory({}, false);
      auto acc = this->storage->Access();
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 5, 7, 23, 25, 29));
    }
    {
      {
        auto acc = this->storage->Access();
        for (auto vertex : acc->Vertices(View::OLD)) {
          auto edges = vertex.OutEdges(View::OLD)->edges;
          for (auto &edge : edges) {
            int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
            if (id % 5 == 0) {
              ASSERT_NO_ERROR(acc->DetachDelete({&vertex}, {}, true));
              break;
            }
          }
        }
        ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
      }
      this->storage->FreeMemory({}, false);
      this->storage->FreeMemory({}, false);
      auto acc = this->storage->Access();
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 7, 23, 29));
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypeIndexDrop) {
  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    {
      auto acc = this->storage->Access();
      EXPECT_FALSE(acc->EdgeTypeIndexReady(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().edge_type.size(), 0);
    }

    {
      auto acc = this->storage->Access();
      for (int i = 0; i < 10; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }

    {
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }

    {
      auto acc = this->storage->Access();
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::OLD), View::OLD),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                  UnorderedElementsAre(1, 3, 5, 7, 9));
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 5);
    }

    {
      auto drop_acc = this->DropIndexAccessor();
      EXPECT_FALSE(drop_acc->DropIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(drop_acc->PrepareForCommitPhase());
    }
    {
      auto acc = this->storage->Access();
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 0);
      EXPECT_FALSE(acc->EdgeTypeIndexReady(this->edge_type_id1));
      EXPECT_EQ(acc->ListAllIndices().edge_type.size(), 0);
      EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1), 0);
    }

    {
      auto acc = this->storage->Access();
      for (int i = 10; i < 20; ++i) {
        auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
        auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
        this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }

    {
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }
    {
      auto acc = this->storage->Access();
      EXPECT_TRUE(acc->EdgeTypeIndexReady(this->edge_type_id1));
      EXPECT_THAT(acc->ListAllIndices().edge_type, UnorderedElementsAre(this->edge_type_id1));
    }

    {
      auto acc = this->storage->Access();

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
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }
    {
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id2).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }

    auto acc = this->storage->Access();
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
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }
    {
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id2).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }

    auto acc_before = this->storage->Access();
    auto acc = this->storage->Access();
    auto acc_after = this->storage->Access();

    for (int i = 0; i < 5; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());

    EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_type_id1, View::NEW), View::NEW), IsEmpty());

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    auto acc_after_commit = this->storage->Access();

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
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }
    {
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id2).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }

    auto acc = this->storage->Access();
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
      auto read_only_acc = this->storage->ReadOnlyAccess();
      EXPECT_FALSE(read_only_acc->CreateIndex(this->edge_type_id1).HasError());
      ASSERT_NO_ERROR(read_only_acc->PrepareForCommitPhase());
    }

    auto acc = this->storage->Access();
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

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypePropertyIndexCreate) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgeTypePropertyIndexReady(this->edge_type_id1, this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_type_property.size(), 0);
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 0);
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 5);
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 10);
    acc->Abort();
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 5);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 10);
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Check that GC doesn't remove useful elements
  {
    // Double call to free memory: first run unlinks and marks for cleanup; second run deletes
    this->storage->FreeMemory({}, false);
    this->storage->FreeMemory({}, false);
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
  }
  {
    {
      auto acc = this->storage->Access();
      for (auto vertex : acc->Vertices(View::OLD)) {
        auto edges = vertex.OutEdges(View::OLD)->edges;
        for (auto &edge : edges) {
          int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
          if (id % 3 == 0) {
            ASSERT_NO_ERROR(acc->DeleteEdge(&edge));
          }
        }
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    this->storage->FreeMemory({}, false);
    this->storage->FreeMemory({}, false);
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 5, 7, 23, 25, 29));
  }
  {
    {
      auto acc = this->storage->Access();
      for (auto vertex : acc->Vertices(View::OLD)) {
        auto edges = vertex.OutEdges(View::OLD)->edges;
        for (auto &edge : edges) {
          int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
          if (id % 5 == 0) {
            ASSERT_NO_ERROR(acc->DetachDelete({&vertex}, {}, true));
            break;
          }
        }
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    this->storage->FreeMemory({}, false);
    this->storage->FreeMemory({}, false);
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 7, 23, 29));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypePropertyIndexDrop) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgeTypePropertyIndexReady(this->edge_type_id1, this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_type_property.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgeTypePropertyIndexReady(this->edge_type_id1, this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_type_property.size(), 0);
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_TRUE(acc->DropIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgeTypePropertyIndexReady(this->edge_type_id1, this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_type_property.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->EdgeTypePropertyIndexReady(this->edge_type_id1, this->edge_prop_id1));
    EXPECT_THAT(acc->ListAllIndices().edge_type_property,
                UnorderedElementsAre(std::make_pair(this->edge_type_id1, this->edge_prop_id1)));
  }

  {
    auto acc = this->storage->Access();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypePropertyIndexBasic) {
  // The following steps are performed and index correctness is validated after
  // each step:
  // 1. Create 10 edges numbered from 0 to 9.
  // 2. Add EdgeType1 to odd numbered, and EdgeType2 to even numbered edges.
  // 3. Delete even numbered edges.
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id2, this->edge_prop_id2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  EXPECT_THAT(acc->ListAllIndices().edge_type_property,
              UnorderedElementsAre(std::make_pair(this->edge_type_id1, this->edge_prop_id1),
                                   std::make_pair(this->edge_type_id2, this->edge_prop_id2)));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::NEW), View::NEW), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
    if (i % 2) {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    } else {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id2, memgraph::storage::PropertyValue(i)));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  acc->AdvanceCommand();
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::NEW), View::NEW),
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

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::NEW), View::NEW), IsEmpty());

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id2, this->edge_prop_id2, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypePropertyIndexTransactionalIsolation) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  // Check that transactions only see entries they are supposed to see.
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id2, this->edge_prop_id2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc_before = this->storage->Access();
  auto acc = this->storage->Access();
  auto acc_after = this->storage->Access();

  for (int i = 0; i < 5; ++i) {
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
    auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              IsEmpty());

  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  auto acc_after_commit = this->storage->Access();

  EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after_commit->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypePropertyIndexCountEstimate) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id2, this->edge_prop_id2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  for (int i = 0; i < 20; ++i) {
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
    if (i % 3) {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    } else {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id2, memgraph::storage::PropertyValue(i)));
    }
  }

  EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 13);
  EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id2, this->edge_prop_id2), 7);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgeTypePropertyIndexRepeatingEdgeTypesBetweenSameVertices) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->edge_type_id1, this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
  auto vertex_to = this->CreateVertexWithoutProperties(acc.get());

  for (int i = 0; i < 5; ++i) {
    auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
  }

  EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_type_id1, this->edge_prop_id1), 5);

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));

  auto edges = acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW);
  for (auto edge : edges) {
    auto prop_val = edge.GetProperty(this->prop_id, View::NEW)->ValueInt();
    ASSERT_NO_ERROR(edge.SetProperty(this->prop_id, memgraph::storage::PropertyValue(prop_val + 1)));
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_type_id1, this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 2, 3, 4, 5));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgePropertyIndexCreate) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgePropertyIndexReady(this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_type_property.size(), 0);
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 0);
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 10);
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));

    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 20);
    acc->Abort();
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 10);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 20);
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Check that GC doesn't remove useful elements
  {
    // Double call to free memory: first run unlinks and marks for cleanup; second run deletes
    this->storage->FreeMemory({}, false);
    this->storage->FreeMemory({}, false);
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
  }
  {
    {
      auto acc = this->storage->Access();
      for (auto vertex : acc->Vertices(View::OLD)) {
        auto edges = vertex.OutEdges(View::OLD)->edges;
        for (auto &edge : edges) {
          int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
          if (id % 3 == 0) {
            ASSERT_NO_ERROR(acc->DeleteEdge(&edge));
          }
        }
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    this->storage->FreeMemory({}, false);
    this->storage->FreeMemory({}, false);
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 2, 4, 5, 7, 8, 20, 22, 23, 25, 26, 28, 29));
  }
  {
    {
      auto acc = this->storage->Access();
      for (auto vertex : acc->Vertices(View::OLD)) {
        auto edges = vertex.OutEdges(View::OLD)->edges;
        for (auto &edge : edges) {
          int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
          if (id % 5 == 0) {
            ASSERT_NO_ERROR(acc->DetachDelete({&vertex}, {}, true));
            break;
          }
        }
      }
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }
    this->storage->FreeMemory({}, false);
    this->storage->FreeMemory({}, false);
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 2, 4, 7, 8, 22, 23, 26, 28, 29));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgePropertyIndexDrop) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgePropertyIndexReady(this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_property.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_FALSE(acc->DropGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgePropertyIndexReady(this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_property.size(), 0);
  }

  {
    auto acc = this->DropIndexAccessor();
    EXPECT_TRUE(acc->DropGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->EdgePropertyIndexReady(this->edge_prop_id1));
    EXPECT_EQ(acc->ListAllIndices().edge_property.size(), 0);
  }

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
      auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
      auto edge_acc =
          this->CreateEdge(&vertex_from, &vertex_to, i % 2 ? this->edge_type_id1 : this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->EdgePropertyIndexReady(this->edge_prop_id1));
    EXPECT_THAT(acc->ListAllIndices().edge_property, UnorderedElementsAre(this->edge_prop_id1));
  }

  {
    auto acc = this->storage->Access();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));

    acc->AdvanceCommand();

    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
    EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgePropertyIndexBasic) {
  // The following steps are performed and index correctness is validated after
  // each step:
  // 1. Create 10 edges numbered from 0 to 9.
  // 2. Add EdgeType1 to odd numbered, and EdgeType2 to even numbered edges.
  // 3. Delete even numbered edges.
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  EXPECT_THAT(acc->ListAllIndices().edge_property, UnorderedElementsAre(this->edge_prop_id1, this->edge_prop_id2));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::NEW), View::NEW), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
    if (i % 2) {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    } else {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id2, memgraph::storage::PropertyValue(i)));
    }
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  acc->AdvanceCommand();
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc->Vertices(View::OLD)) {
    auto edges = vertex.OutEdges(View::OLD)->edges;
    for (auto &edge : edges) {
      int64_t id = edge.GetProperty(this->prop_id, View::OLD)->ValueInt();
      if (id % 2 == 0) {
        ASSERT_NO_ERROR(acc->DetachDelete({}, {&edge}, false));
      }
    }
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::NEW), View::NEW), IsEmpty());

  acc->AdvanceCommand();

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id2, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgePropertyIndexTransactionalIsolation) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  // Check that transactions only see entries they are supposed to see.
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc_before = this->storage->Access();
  auto acc = this->storage->Access();
  auto acc_after = this->storage->Access();

  for (int i = 0; i < 5; ++i) {
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
    auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_prop_id1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_prop_id1, View::NEW), View::NEW), IsEmpty());

  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  auto acc_after_commit = this->storage->Access();

  EXPECT_THAT(this->GetIds(acc_before->Edges(this->edge_prop_id1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after->Edges(this->edge_prop_id1, View::NEW), View::NEW), IsEmpty());

  EXPECT_THAT(this->GetIds(acc_after_commit->Edges(this->edge_prop_id1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgePropertyIndexCountEstimate) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id2).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  for (int i = 0; i < 20; ++i) {
    auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
    auto vertex_to = this->CreateVertexWithoutProperties(acc.get());
    if (i % 3) {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
    } else {
      auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id2, acc.get());
      ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id2, memgraph::storage::PropertyValue(i)));
    }
  }

  EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 13);
  EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id2), 7);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, EdgePropertyIndexRepeatingEdgeTypesBetweenSameVertices) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    return;
  }
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateGlobalEdgeIndex(this->edge_prop_id1).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  auto vertex_from = this->CreateVertexWithoutProperties(acc.get());
  auto vertex_to = this->CreateVertexWithoutProperties(acc.get());

  for (int i = 0; i < 5; ++i) {
    auto edge_acc = this->CreateEdge(&vertex_from, &vertex_to, this->edge_type_id1, acc.get());
    ASSERT_NO_ERROR(edge_acc.SetProperty(this->edge_prop_id1, memgraph::storage::PropertyValue(i)));
  }

  EXPECT_EQ(acc->ApproximateEdgeCount(this->edge_prop_id1), 5);

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

  auto edges = acc->Edges(this->edge_prop_id1, View::NEW);
  for (auto edge : edges) {
    auto prop_val = edge.GetProperty(this->prop_id, View::NEW)->ValueInt();
    ASSERT_NO_ERROR(edge.SetProperty(this->prop_id, memgraph::storage::PropertyValue(prop_val + 1)));
  }

  EXPECT_THAT(this->GetIds(acc->Edges(this->edge_prop_id1, View::NEW), View::NEW), UnorderedElementsAre(1, 2, 3, 4, 5));
}

TYPED_TEST(IndexTest, CanIterateNestedLabelPropertyIndex) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Nested indices currently not supported on disk";
  }

  auto const make_map = [](PropertyId key, PropertyValue value) {
    return PropertyValue{PropertyValue::map_t{{key, std::move(value)}}};
  };

  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_a, this->prop_b, this->prop_c}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 3 < 2 ? this->label1 : this->label2));

      // INDEX: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
      // LABEL: 1 1 2 1 1 2 1 1 2 1  1  2  1  1  2  1  1  2  1  1
      // c=int: 0 0 0 1 0 0 0 0 1 0  0  0  0  1  0  0  0  0  1  0

      if (i % 5 == 0) {
        // no `a` property
      } else if (i % 5 == 1) {
        // has an `a` property, but it is not a map, but a string
        ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, PropertyValue(std::to_string(i))));
      } else if (i % 5 == 2) {
        // has `a.b`, where b is not a map, but a string
        ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, make_map(this->prop_b, PropertyValue(std::to_string(i)))));
      } else if (i % 5 == 3) {
        // has `a.b.c`, where c is an integer
        ASSERT_NO_ERROR(
            vertex.SetProperty(this->prop_a, make_map(this->prop_b, make_map(this->prop_c, PropertyValue(i)))));
      } else if (i % 5 == 4) {
        // has `a.b.c`, where c is a boolean `true`
        ASSERT_NO_ERROR(
            vertex.SetProperty(this->prop_a, make_map(this->prop_b, make_map(this->prop_c, PropertyValue(true)))));
      }
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b, this->prop_c}},
                                 std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(0)),
                                                       memgraph::utils::MakeBoundInclusive(PropertyValue(20)))},
                                 View::NEW),
                   View::NEW),
      UnorderedElementsAre(3, 13, 18));

  // TODO(colinbarry): Temporarily remove this portion of the test from ASAN
  // tests because it is (correctly) flagged as causing a leak. This is because
  // of an issue where if a Delta contains any map-type `PropertyValue`, the
  // storage for the inner part of the map is not deallocated.
#if __has_feature(address_sanitizer)
  GTEST_SKIP() << "Skipping portion of index test due to delta leak bug";
#endif

  // Remove a.b.c from vertex 13 and set a.b.c to 15 on vertex 15
  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id == 13) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, PropertyValue{}));
    } else if (id == 15) {
      ASSERT_NO_ERROR(
          vertex.SetProperty(this->prop_a, make_map(this->prop_b, make_map(this->prop_c, PropertyValue(15)))));
    }
  }

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b, this->prop_c}},
                                 std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(0)),
                                                       memgraph::utils::MakeBoundInclusive(PropertyValue(20)))},
                                 View::OLD),
                   View::OLD),
      UnorderedElementsAre(3, 13, 18));

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b, this->prop_c}},
                                 std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(0)),
                                                       memgraph::utils::MakeBoundInclusive(PropertyValue(20)))},
                                 View::NEW),
                   View::NEW),
      UnorderedElementsAre(3, 15, 18));

  acc->AdvanceCommand();

  EXPECT_THAT(
      this->GetIds(acc->Vertices(this->label1, std::array{PropertyPath{this->prop_a, this->prop_b, this->prop_c}},
                                 std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(0)),
                                                       memgraph::utils::MakeBoundInclusive(PropertyValue(20)))},
                                 View::OLD),
                   View::OLD),
      UnorderedElementsAre(3, 15, 18));
}

TYPED_TEST(IndexTest, CompositeIndicesReadOutOfOrderProperties) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Composite indices currently not supported on disk";
  }

  // TODO(colinbarry): Temporarily remove this portion of the test from ASAN
  // tests because it is (correctly) flagged as causing a leak. This is because
  // of an issue where if a Delta contains any map-type `PropertyValue`, the
  // storage for the inner part of the map is not deallocated.
#if __has_feature(address_sanitizer)
  GTEST_SKIP() << "Skipping portion of index test due to delta leak bug";
#endif

  {
    auto acc = this->CreateIndexAccessor();
    // Note the index properties are not based on monotonic `PropertyId`. They
    // will need to be permuted when writing and reading from the property store.
    EXPECT_FALSE(acc->CreateIndex(this->label1,
                                  {PropertyPath{this->prop_b}, PropertyPath{this->prop_a}, PropertyPath{this->prop_c}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  {
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, PropertyValue(i)));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, PropertyValue(i + 10)));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, PropertyValue(i + 20)));
    }
  }

  auto const get_ids = [&](View view) {
    return this->GetIds(
        acc->Vertices(this->label1,
                      std::array{PropertyPath{this->prop_b}, PropertyPath{this->prop_a}, PropertyPath{this->prop_c}},
                      std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(10)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(20))),
                                 pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(0)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(10))),
                                 pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(20)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(30)))},
                      view),
        view);
  };

  acc->AdvanceCommand();
  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, PropertyValue{}));
    }
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));

  acc->AdvanceCommand();
  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, PropertyValue{id + 10}));
    }

    EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  }
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

  acc->AdvanceCommand();
  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 3 < 2) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, PropertyValue{"a-string"}));
    }
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(2, 5, 8));

  acc->AdvanceCommand();
  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 3 < 2) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, PropertyValue{id + 20}));
    }
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(2, 5, 8));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

  acc->AdvanceCommand();
  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 5 < 3) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, PropertyValue{}));
    }
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(3, 4, 8, 9));
}

TYPED_TEST(IndexTest, NestedIndicesReadOutOfOrderProperties) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Composite indices currently not supported on disk";
  }

  {
    auto acc = this->CreateIndexAccessor();
    // Note the index properties are not based on monotonic `PropertyId`. They
    // will need to be permuted when writing and reading from the property store.
    EXPECT_FALSE(acc->CreateIndex(this->label1,
                                  {PropertyPath{this->prop_b, this->prop_d}, PropertyPath{this->prop_a, this->prop_d},
                                   PropertyPath{this->prop_c, this->prop_d}})
                     .HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();
  {
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, MakeMap(KVPair{this->prop_d, PropertyValue(i)})));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, MakeMap(KVPair{this->prop_d, PropertyValue(i + 10)})));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, MakeMap(KVPair{this->prop_d, PropertyValue(i + 20)})));
    }
  }

  auto const get_ids = [&](View view) {
    return this->GetIds(
        acc->Vertices(this->label1,
                      std::array{PropertyPath{this->prop_b, this->prop_d}, PropertyPath{this->prop_a, this->prop_d},
                                 PropertyPath{this->prop_c, this->prop_d}},
                      std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(10)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(20))),
                                 pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(0)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(10))),
                                 pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue(20)),
                                            memgraph::utils::MakeBoundInclusive(PropertyValue(30)))},
                      view),
        view);
  };

  acc->AdvanceCommand();
  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, PropertyValue{}));
    }
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));

  acc->AdvanceCommand();
  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));

  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    if (id % 3 == 0) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, PropertyValue{}));
    }
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(1, 5, 7));

  acc->AdvanceCommand();
  for (auto vertex : acc->Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(this->prop_id, View::OLD)->ValueInt();
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_a, MakeMap(KVPair{this->prop_d, PropertyValue(id)})));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_b, MakeMap(KVPair{this->prop_d, PropertyValue(id + 10)})));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_c, MakeMap(KVPair{this->prop_d, PropertyValue(id + 20)})));
  }

  EXPECT_THAT(get_ids(View::OLD), UnorderedElementsAre(1, 5, 7));
  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
}

TYPED_TEST(IndexTest, DeltaDoesNotLeak) {
  if constexpr (!(std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>)) {
    GTEST_SKIP() << "Testing in-memory only";
  }

  auto const make_map = [](PropertyId key, PropertyValue value) {
    return PropertyValue{PropertyValue::map_t{{key, std::move(value)}}};
  };
  {
    auto acc = this->storage->Access();
    auto vertex = this->CreateVertex(acc.get());
    vertex.SetProperty(this->prop_a, make_map(this->prop_b, make_map(this->prop_c, PropertyValue("hello"))));
    // vvv This create a `Delta` containing the above `PropertyValue` map,
    // which ASAN reports as a leak.
    vertex.SetProperty(this->prop_a, PropertyValue("goodbye"));
  }
  this->storage->FreeMemory();
}

TYPED_TEST(IndexTest, LabelPropertiesIndicesScansOnlyStringsForRegexes) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Regex index search currently not supported on disk";
  }
  auto const values = std::vector{
      PropertyValue{false},    PropertyValue{true},    PropertyValue{2},
      PropertyValue{3},        PropertyValue{"apple"}, PropertyValue{"banana"},
      PropertyValue{"cherry"}, PropertyValue{"date"},  PropertyValue{"eggplant"},
  };
  {
    auto acc = this->CreateIndexAccessor();
    EXPECT_FALSE(acc->CreateIndex(this->label1, {PropertyPath{this->prop_val}}).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  auto acc = this->storage->Access();

  for (auto &&value : values) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, value));
  }

  auto const get_ids = [&](View view) {
    return this->GetIds(
        acc->Vertices(
            this->label1, std::array{PropertyPath{this->prop_val}},
            std::array{pvr::Range(memgraph::utils::MakeBoundInclusive(PropertyValue("")),
                                  memgraph::storage::UpperBoundForType(memgraph::storage::PropertyValueType::String))},
            view),
        view);
  };

  EXPECT_THAT(get_ids(View::NEW), UnorderedElementsAre(4, 5, 6, 7, 8));
}
