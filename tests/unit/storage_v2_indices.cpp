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

#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

using testing::IsEmpty;
using testing::Types;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

template <typename StorageType>
class IndexTest : public testing::Test {
 protected:
  void SetUp() override {
    this->storage = std::make_unique<StorageType>(config_);
    auto acc = this->storage->Access();
    this->prop_id = acc->NameToProperty("id");
    this->prop_val = acc->NameToProperty("val");
    this->label1 = acc->NameToLabel("label1");
    this->label2 = acc->NameToLabel("label2");
    vertex_id = 0;
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      /// TODO: extract this into some method
      std::filesystem::remove_all(config_.disk.main_storage_directory);
      std::filesystem::remove_all(config_.disk.label_index_directory);
      std::filesystem::remove_all(config_.disk.label_property_index_directory);
      std::filesystem::remove_all(config_.disk.unique_constraints_directory);
    }
    this->storage.reset(nullptr);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  PropertyId prop_id;
  PropertyId prop_val;
  LabelId label1;
  LabelId label2;

  VertexAccessor CreateVertex(Storage::Accessor *accessor) {
    VertexAccessor vertex = accessor->CreateVertex();
    MG_ASSERT(!vertex.SetProperty(this->prop_id, PropertyValue(vertex_id++)).HasError());
    return vertex;
  }

  template <class TIterable>
  std::vector<int64_t> GetIds(TIterable iterable, View view = View::OLD) {
    std::vector<int64_t> ret;
    for (auto vertex : iterable) {
      ret.push_back(vertex.GetProperty(this->prop_id, view)->ValueInt());
    }
    return ret;
  }

 private:
  int vertex_id;
};

// using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
using StorageTypes = ::testing::Types<memgraph::storage::DiskStorage>;

TYPED_TEST_CASE(IndexTest, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexCreate) {
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
  }
  EXPECT_EQ(this->storage->ListAllIndices().label.size(), 0);

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());

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

    ASSERT_NO_ERROR(acc->Commit());
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

    ASSERT_NO_ERROR(acc->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexDrop) {
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
  }
  EXPECT_EQ(this->storage->ListAllIndices().label.size(), 0);

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());

  {
    auto acc = this->storage->Access();
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  EXPECT_FALSE(this->storage->DropIndex(this->label1).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
  }
  EXPECT_EQ(this->storage->ListAllIndices().label.size(), 0);

  EXPECT_TRUE(this->storage->DropIndex(this->label1).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelIndexExists(this->label1));
  }
  EXPECT_EQ(this->storage->ListAllIndices().label.size(), 0);

  {
    auto acc = this->storage->Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? this->label1 : this->label2));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelIndexExists(this->label1));
  }
  EXPECT_THAT(this->storage->ListAllIndices().label, UnorderedElementsAre(this->label1));

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
  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());
  EXPECT_FALSE(this->storage->CreateIndex(this->label2).HasError());

  auto acc = this->storage->Access();
  EXPECT_THAT(this->storage->ListAllIndices().label, UnorderedElementsAre(this->label1, this->label2));
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
  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());
  EXPECT_FALSE(this->storage->CreateIndex(this->label2).HasError());

  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 5; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    }

    EXPECT_THAT(this->GetIds(acc->Vertices(this->label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc->Commit());
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
TYPED_TEST(IndexTest, LabelIndexTransactionalIsolation) {
  // Check that transactions only see entries they are supposed to see.
  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());
  EXPECT_FALSE(this->storage->CreateIndex(this->label2).HasError());

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

  ASSERT_NO_ERROR(acc->Commit());

  auto acc_after_commit = this->storage->Access();

  EXPECT_THAT(this->GetIds(acc_before->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc_after->Vertices(this->label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(this->GetIds(acc_after_commit->Vertices(this->label1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelIndexCountEstimate) {
  EXPECT_FALSE(this->storage->CreateIndex(this->label1).HasError());
  EXPECT_FALSE(this->storage->CreateIndex(this->label2).HasError());

  auto acc = this->storage->Access();
  for (int i = 0; i < 20; ++i) {
    auto vertex = this->CreateVertex(acc.get());
    ASSERT_NO_ERROR(vertex.AddLabel(i % 3 ? this->label1 : this->label2));
  }

  EXPECT_EQ(acc->ApproximateVertexCount(this->label1), 13);
  EXPECT_EQ(acc->ApproximateVertexCount(this->label2), 7);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexCreateAndDrop) {
  EXPECT_EQ(this->storage->ListAllIndices().label_property.size(), 0);
  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_id).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelPropertyIndexExists(this->label1, this->prop_id));
  }
  EXPECT_THAT(this->storage->ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(this->label1, this->prop_id)));
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexExists(this->label2, this->prop_id));
  }
  EXPECT_TRUE(this->storage->CreateIndex(this->label1, this->prop_id).HasError());
  EXPECT_THAT(this->storage->ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(this->label1, this->prop_id)));

  EXPECT_FALSE(this->storage->CreateIndex(this->label2, this->prop_id).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_TRUE(acc->LabelPropertyIndexExists(this->label2, this->prop_id));
  }
  EXPECT_THAT(
      this->storage->ListAllIndices().label_property,
      UnorderedElementsAre(std::make_pair(this->label1, this->prop_id), std::make_pair(this->label2, this->prop_id)));

  EXPECT_FALSE(this->storage->DropIndex(this->label1, this->prop_id).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexExists(this->label1, this->prop_id));
  }
  EXPECT_THAT(this->storage->ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(this->label2, this->prop_id)));
  EXPECT_TRUE(this->storage->DropIndex(this->label1, this->prop_id).HasError());

  EXPECT_FALSE(this->storage->DropIndex(this->label2, this->prop_id).HasError());
  {
    auto acc = this->storage->Access();
    EXPECT_FALSE(acc->LabelPropertyIndexExists(this->label2, this->prop_id));
  }
  EXPECT_EQ(this->storage->ListAllIndices().label_property.size(), 0);
}

// The following three tests are almost an exact copy-paste of the corresponding
// label index tests. We request all vertices with given label and property from
// the index, without range filtering. Range filtering is tested in a separate
// test.

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(IndexTest, LabelPropertyIndexBasic) {
  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_val).HasError());
  EXPECT_FALSE(this->storage->CreateIndex(this->label2, this->prop_val).HasError());

  auto acc = this->storage->Access();
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
  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_val).HasError());
  {
    auto acc = this->storage->Access();
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
    auto acc = this->storage->Access();
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
  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_val).HasError());

  auto acc_before = this->storage->Access();
  auto acc = this->storage->Access();
  auto acc_after = this->storage->Access();

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

  auto acc_after_commit = this->storage->Access();

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

  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_val).HasError());

  {
    auto acc = this->storage->Access();

    for (int i = 0; i < 10; ++i) {
      auto vertex = this->CreateVertex(acc.get());
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop_val, i % 2 ? PropertyValue(i / 2) : PropertyValue(i / 2.0)));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }
  {
    auto acc = this->storage->Access();
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
  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_val).HasError());

  auto acc = this->storage->Access();
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

TYPED_TEST(IndexTest, LabelPropertyIndexMixedIteration) {
  EXPECT_FALSE(this->storage->CreateIndex(this->label1, this->prop_val).HasError());

  const std::array temporals{TemporalData{TemporalType::Date, 23}, TemporalData{TemporalType::Date, 28},
                             TemporalData{TemporalType::LocalDateTime, 20}};

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
  };

  // Create vertices, each with one of the values above.
  {
    auto acc = this->storage->Access();
    for (const auto &value : values) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(this->label1).HasValue());
      ASSERT_TRUE(v.SetProperty(this->prop_val, value).HasValue());
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Verify that all nodes are in the index.
  {
    auto acc = this->storage->Access();
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
    auto acc = this->storage->Access();
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
