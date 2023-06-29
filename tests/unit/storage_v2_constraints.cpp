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
#include <filesystem>
#include <variant>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include "disk_test_utils.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

using testing::Types;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

template <typename StorageType>
class ConstraintsTest : public testing::Test {
 public:
  const std::string testSuite = "storage_v2_constraints";

  ConstraintsTest() {
    /// TODO: andi How to make this better? Because currentlly for every test changed you need to create a configuration
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    storage = std::make_unique<StorageType>(config_);
    prop1 = storage->NameToProperty("prop1");
    prop2 = storage->NameToProperty("prop2");
    label1 = storage->NameToLabel("label1");
    label2 = storage->NameToLabel("label2");
  }

  void TearDown() override {
    storage.reset(nullptr);

    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  std::unique_ptr<Storage> storage;
  memgraph::storage::Config config_;
  PropertyId prop1;
  PropertyId prop2;
  LabelId label1;
  LabelId label2;
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(ConstraintsTest, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsCreateAndDrop) {
  EXPECT_EQ(this->storage->ListAllConstraints().existence.size(), 0);
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    EXPECT_FALSE(res.HasError());
  }
  EXPECT_THAT(this->storage->ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(this->label1, this->prop1)));
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    EXPECT_TRUE(res.HasError());
  }
  EXPECT_THAT(this->storage->ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(this->label1, this->prop1)));
  {
    auto res = this->storage->CreateExistenceConstraint(this->label2, this->prop1, {});
    EXPECT_FALSE(res.HasError());
  }
  EXPECT_THAT(
      this->storage->ListAllConstraints().existence,
      UnorderedElementsAre(std::make_pair(this->label1, this->prop1), std::make_pair(this->label2, this->prop1)));
  EXPECT_FALSE(this->storage->DropExistenceConstraint(this->label1, this->prop1, {}).HasError());
  EXPECT_TRUE(this->storage->DropExistenceConstraint(this->label1, this->prop1, {}).HasError());
  EXPECT_THAT(this->storage->ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(this->label2, this->prop1)));
  EXPECT_FALSE(this->storage->DropExistenceConstraint(this->label2, this->prop1, {}).HasError());
  EXPECT_TRUE(this->storage->DropExistenceConstraint(this->label2, this->prop2, {}).HasError());
  EXPECT_EQ(this->storage->ListAllConstraints().existence.size(), 0);
  {
    auto res = this->storage->CreateExistenceConstraint(this->label2, this->prop1, {});
    EXPECT_FALSE(res.HasError());
  }
  EXPECT_THAT(this->storage->ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(this->label2, this->prop1)));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsCreateFailure1) {
  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc->Commit());
  }
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
  {
    auto acc = this->storage->Access();
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    EXPECT_FALSE(res.HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsCreateFailure2) {
  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc->Commit());
  }
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
  {
    auto acc = this->storage->Access();
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    EXPECT_FALSE(res.HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsViolationOnCommit) {
  {
    auto res = this->storage->CreateExistenceConstraint(this->label1, this->prop1, {});
    EXPECT_FALSE(res.HasError());
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));

    auto res = acc->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue()));
    }

    auto res = acc->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
  }

  {
    auto acc = this->storage->Access();
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue()));
    }
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }

    ASSERT_NO_ERROR(acc->Commit());
  }

  ASSERT_FALSE(this->storage->DropExistenceConstraint(this->label1, this->prop1, {}).HasError());

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsCreateAndDropAndList) {
  EXPECT_EQ(this->storage->ListAllConstraints().unique.size(), 0);
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    EXPECT_TRUE(res.HasValue());
    EXPECT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
  EXPECT_THAT(this->storage->ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1})));
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    EXPECT_TRUE(res.HasValue());
    EXPECT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::ALREADY_EXISTS);
  }
  EXPECT_THAT(this->storage->ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1})));
  {
    auto res = this->storage->CreateUniqueConstraint(this->label2, {this->prop1}, {});
    EXPECT_TRUE(res.HasValue() && res.GetValue() == UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
  EXPECT_THAT(this->storage->ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1}),
                                   std::make_pair(this->label2, std::set<PropertyId>{this->prop1})));
  EXPECT_EQ(this->storage->DropUniqueConstraint(this->label1, {this->prop1}, {}).GetValue(),
            UniqueConstraints::DeletionStatus::SUCCESS);
  EXPECT_EQ(this->storage->DropUniqueConstraint(this->label1, {this->prop1}, {}).GetValue(),
            UniqueConstraints::DeletionStatus::NOT_FOUND);
  EXPECT_THAT(this->storage->ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(this->label2, std::set<PropertyId>{this->prop1})));
  EXPECT_EQ(this->storage->DropUniqueConstraint(this->label2, {this->prop1}, {}).GetValue(),
            UniqueConstraints::DeletionStatus::SUCCESS);
  EXPECT_EQ(this->storage->DropUniqueConstraint(this->label2, {this->prop2}, {}).GetValue(),
            UniqueConstraints::DeletionStatus::NOT_FOUND);
  EXPECT_EQ(this->storage->ListAllConstraints().unique.size(), 0);
  {
    auto res = this->storage->CreateUniqueConstraint(this->label2, {this->prop1}, {});
    EXPECT_TRUE(res.HasValue());
    EXPECT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
  EXPECT_THAT(this->storage->ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(this->label2, std::set<PropertyId>{this->prop1})));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsCreateFailure1) {
  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex1 = acc->CreateVertex();
      ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }

  {
    auto acc = this->storage->Access();
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsCreateFailure2) {
  {
    auto acc = this->storage->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }

  {
    auto acc = this->storage->Access();
    int value = 0;
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(value)));
      ++value;
    }
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation1) {
  Gid gid1;
  Gid gid2;
  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop2, PropertyValue(3)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation2) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---SP(v1, 2)---OK--
    // tx2: -B---SP(v2, 2)---SP(v2, 1)---OK-

    auto acc1 = this->storage->Access();
    auto acc2 = this->storage->Access();
    auto vertex1 = acc1->CreateVertex();
    auto vertex2 = acc2->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1->Commit());
    ASSERT_NO_ERROR(acc2->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation3) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---OK----------------------
    // tx2: --------------------B---SP(v1, 2)---OK--
    // tx3: ---------------------B---SP(v2, 1)---OK-

    auto acc1 = this->storage->Access();
    auto vertex1 = acc1->CreateVertex();
    auto gid = vertex1.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access();
    auto acc3 = this->storage->Access();
    auto vertex2 = acc2->FindVertex(gid, View::NEW);  // vertex1 == vertex2
    auto vertex3 = acc3->CreateVertex();

    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex3.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex3.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc2->Commit());
    ASSERT_NO_ERROR(acc3->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation4) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---OK-----------------------
    // tx2: --------------------B---SP(v2, 1)-----OK-
    // tx3: ---------------------B---SP(v1, 2)---OK--

    auto acc1 = this->storage->Access();
    auto vertex1 = acc1->CreateVertex();
    auto gid = vertex1.Gid();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access();
    auto acc3 = this->storage->Access();
    auto vertex2 = acc2->CreateVertex();
    auto vertex3 = acc3->FindVertex(gid, View::NEW);

    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc3->Commit());
    ASSERT_NO_ERROR(acc2->Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsViolationOnCommit1) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));
    auto res = acc->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
/// TODO: andi consistency problems
TYPED_TEST(ConstraintsTest, UniqueConstraintsViolationOnCommit2) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---SP(v2, 2)---OK-----------------------
    // tx2: -------------------------------B---SP(v1, 3)---OK----
    // tx3: --------------------------------B---SP(v2, 3)---FAIL-

    auto acc1 = this->storage->Access();
    auto vertex1 = acc1->CreateVertex();
    auto vertex2 = acc1->CreateVertex();
    auto gid1 = vertex1.Gid();
    auto gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access();
    auto acc3 = this->storage->Access();
    auto vertex3 = acc2->FindVertex(gid1, View::NEW);  // vertex3 == vertex1
    auto vertex4 = acc3->FindVertex(gid2, View::NEW);  // vertex4 == vertex2

    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop1, PropertyValue(3)));
    ASSERT_NO_ERROR(vertex4->SetProperty(this->prop1, PropertyValue(3)));

    ASSERT_NO_ERROR(acc2->Commit());
    auto res = acc3->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
/// TODO: andi consistency problems
TYPED_TEST(ConstraintsTest, UniqueConstraintsViolationOnCommit3) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---SP(v2, 2)---OK-----------------------
    // tx2: -------------------------------B---SP(v1, 2)---FAIL--
    // tx3: --------------------------------B---SP(v2, 1)---FAIL-

    auto acc1 = this->storage->Access();
    auto vertex1 = acc1->CreateVertex();
    auto vertex2 = acc1->CreateVertex();
    auto gid1 = vertex1.Gid();
    auto gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc1->Commit());

    auto acc2 = this->storage->Access();
    auto acc3 = this->storage->Access();
    auto vertex3 = acc2->FindVertex(gid1, View::OLD);  // vertex3 == vertex1
    auto vertex4 = acc3->FindVertex(gid2, View::OLD);  // vertex4 == vertex2

    // Setting `this->prop2` shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop2, PropertyValue(3)));
    ASSERT_NO_ERROR(vertex4->SetProperty(this->prop2, PropertyValue(3)));

    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex4->SetProperty(this->prop1, PropertyValue(1)));

    auto res = acc2->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
    res = acc3->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsLabelAlteration) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid1;
  Gid gid2;
  {
    // B---AL(v2)---SP(v1, 1)---SP(v2, 1)---OK

    auto acc = this->storage->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    spdlog::debug("Vertex1 gid: {} Vertex2 gid: {}\n", memgraph::utils::SerializeIdType(gid1),
                  memgraph::utils::SerializeIdType(gid2));

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label2));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    // tx1: B---AL(v1)-----OK-
    // tx2: -B---RL(v2)---OK--

    auto acc1 = this->storage->Access();
    auto acc2 = this->storage->Access();
    auto vertex1 = acc1->FindVertex(gid1, View::OLD);
    auto vertex2 = acc2->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(this->label1));

    // Reapplying labels shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label2));

    // Commit the second transaction.
    ASSERT_NO_ERROR(acc2->Commit());

    // Reapplying labels after first commit shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));

    // Commit the first transaction.
    ASSERT_NO_ERROR(acc1->Commit());
  }

  {
    // B---AL(v2)---FAIL

    auto acc = this->storage->Access();
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));

    auto res = acc->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(std::get<ConstraintViolation>(res.GetError()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1}}));
  }

  {
    // B---RL(v1)---OK

    auto acc = this->storage->Access();
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    // tx1: B---AL(v1)-----FAIL
    // tx2: -B---AL(v2)---OK---

    auto acc1 = this->storage->Access();
    auto acc2 = this->storage->Access();
    auto vertex1 = acc1->FindVertex(gid1, View::OLD);
    auto vertex2 = acc2->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));

    // Reapply everything.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));

    ASSERT_NO_ERROR(acc2->Commit());

    auto res = acc1->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(std::get<ConstraintViolation>(res.GetError()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsPropertySetSize) {
  {
    // This should fail since unique constraint cannot be created for an empty
    // property set.
    auto res = this->storage->CreateUniqueConstraint(this->label1, {}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::EMPTY_PROPERTIES);
  }

  // Removing a constraint with empty property set should also fail.
  ASSERT_EQ(this->storage->DropUniqueConstraint(this->label1, {}, {}).GetValue(),
            UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES);

  // Create a set of 33 properties.
  std::set<PropertyId> properties;
  for (int i = 1; i <= 33; ++i) {
    properties.insert(this->storage->NameToProperty("prop" + std::to_string(i)));
  }

  {
    // This should fail since list of properties exceeds the maximum number of
    // properties, which is 32.
    auto res = this->storage->CreateUniqueConstraint(this->label1, properties, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED);
  }

  // An attempt to delete constraint with too large property set should fail.
  ASSERT_EQ(this->storage->DropUniqueConstraint(this->label1, properties, {}).GetValue(),
            UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED);

  // Remove one property from the set.
  properties.erase(properties.begin());

  {
    // Creating a constraint for 32 properties should succeed.
    auto res = this->storage->CreateUniqueConstraint(this->label1, properties, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  EXPECT_THAT(this->storage->ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(this->label1, properties)));

  // Removing a constraint with 32 properties should succeed.
  ASSERT_EQ(this->storage->DropUniqueConstraint(this->label1, properties, {}).GetValue(),
            UniqueConstraints::DeletionStatus::SUCCESS);
  ASSERT_TRUE(this->storage->ListAllConstraints().unique.empty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
/// TODO: andi consistency problems
TYPED_TEST(ConstraintsTest, UniqueConstraintsMultipleProperties) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // An attempt to create an existing unique constraint.
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop2, this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::ALREADY_EXISTS);
  }

  Gid gid1;
  Gid gid2;
  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop2, PropertyValue(2)));

    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop2, PropertyValue(3)));

    ASSERT_NO_ERROR(acc->Commit());
  }

  // Try to change property of the second vertex so it becomes the same as the
  // first vertex-> It should fail.
  {
    auto acc = this->storage->Access();
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop2, PropertyValue(2)));
    auto res = acc->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(std::get<ConstraintViolation>(res.GetError()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1,
                                   std::set<PropertyId>{this->prop1, this->prop2}}));
  }

  // Then change the second property of both vertex to null. Property values of
  // both vertices should now be equal. However, this operation should succeed
  // since null value is treated as non-existing property.
  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop2, PropertyValue()));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop2, PropertyValue()));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

/// TODO: andi Test passes when ran alone but fails when all tests are run
TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertAbortInsert) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    acc->Abort();
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertRemoveInsert) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid;
  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_NO_ERROR(acc->DeleteVertex(&*vertex));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertRemoveAbortInsert) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid;
  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_NO_ERROR(acc->DeleteVertex(&*vertex));
    acc->Abort();
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));

    auto res = acc->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.GetError()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1, this->prop2}}));
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsDeleteVertexSetProperty) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid1;
  Gid gid2;
  {
    auto acc = this->storage->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc1 = this->storage->Access();
    auto acc2 = this->storage->Access();
    auto vertex1 = acc1->FindVertex(gid1, View::OLD);
    auto vertex2 = acc2->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(acc2->DeleteVertex(&*vertex2));
    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop1, PropertyValue(2)));

    auto res = acc1->Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(std::get<ConstraintViolation>(res.GetError()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1}}));

    ASSERT_NO_ERROR(acc2->Commit());
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertDropInsert) {
  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  ASSERT_EQ(this->storage->DropUniqueConstraint(this->label1, {this->prop2, this->prop1}, {}).GetValue(),
            UniqueConstraints::DeletionStatus::SUCCESS);

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsComparePropertyValues) {
  // Purpose of this test is to make sure that extracted property values
  // are correctly compared.

  {
    auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2}, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->Commit());
  }

  {
    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(0)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(3)));
    ASSERT_NO_ERROR(acc->Commit());
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsClearOldData) {
  // Purpose of this test is to make sure that extracted property values
  // are correctly compared.

  if constexpr ((std::is_same_v<TypeParam, memgraph::storage::DiskStorage>)) {
    auto *disk_constraints =
        static_cast<memgraph::storage::DiskUniqueConstraints *>(this->storage->constraints_.unique_constraints_.get());
    auto *tx_db = disk_constraints->GetRocksDBStorage()->db_;

    {
      auto res = this->storage->CreateUniqueConstraint(this->label1, {this->prop1}, {});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
    }

    auto acc = this->storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->Commit());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc2 = this->storage->Access(std::nullopt);
    auto vertex2 = acc2->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex2.SetProperty(this->prop1, memgraph::storage::PropertyValue(2)).HasValue());
    ASSERT_FALSE(acc2->Commit().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc3 = this->storage->Access(std::nullopt);
    auto vertex3 = acc3->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex3.SetProperty(this->prop1, memgraph::storage::PropertyValue(10)).HasValue());
    ASSERT_FALSE(acc3->Commit().HasError());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
  }
}
