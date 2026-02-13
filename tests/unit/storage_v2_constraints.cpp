// Copyright 2026 Memgraph Ltd.
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
#include <type_traits>
#include <variant>

#include "dbms/database.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

#include "disk_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

using testing::Types;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())

template <typename StorageType>
class ConstraintsTest : public testing::Test {
 public:
  const std::string testSuite = "storage_v2_constraints";

  ConstraintsTest() {
    /// TODO: andi How to make this better? Because currentlly for every test changed you need to create a configuration
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    config_.force_on_disk = std::is_same_v<StorageType, memgraph::storage::DiskStorage>;
    db_gk_.emplace(config_);
    auto db_acc_opt = db_gk_->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    db_acc_ = *db_acc_opt;
    storage = db_acc_->get()->storage();
    prop1 = storage->NameToProperty("prop1");
    prop2 = storage->NameToProperty("prop2");
    label1 = storage->NameToLabel("label1");
    label2 = storage->NameToLabel("label2");
  }

  void TearDown() override {
    storage = nullptr;
    db_acc_.reset();
    db_gk_.reset();

    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  auto CreateConstraintAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      return this->db_acc_->get()->ReadOnlyAccess();
    } else {
      return this->db_acc_->get()->UniqueAccess();
    }
  }

  auto DropConstraintAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      return this->db_acc_->get()->ReadOnlyAccess();
    } else {
      return this->db_acc_->get()->UniqueAccess();
    }
  }

  Storage *storage;
  memgraph::storage::Config config_;
  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk_;
  PropertyId prop1;
  PropertyId prop2;
  LabelId label1;
  LabelId label2;
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(ConstraintsTest, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsCreateAndDrop) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().existence.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_FALSE(!res.has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(this->label1, this->prop1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_TRUE(!res.has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(this->label1, this->prop1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label2, this->prop1);
    EXPECT_FALSE(!res.has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(
        acc->ListAllConstraints().existence,
        UnorderedElementsAre(std::make_pair(this->label1, this->prop1), std::make_pair(this->label2, this->prop1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_FALSE(!constraint_acc->DropExistenceConstraint(this->label1, this->prop1).has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_TRUE(!constraint_acc->DropExistenceConstraint(this->label1, this->prop1).has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(this->label2, this->prop1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_FALSE(!constraint_acc->DropExistenceConstraint(this->label2, this->prop1).has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_TRUE(!constraint_acc->DropExistenceConstraint(this->label2, this->prop2).has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().existence.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label2, this->prop1);
    EXPECT_FALSE(!res.has_value());
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(this->label2, this->prop1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsCreateFailure1) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs())
                    .has_value());  // TODO: Check if we are committing here?
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_FALSE(!res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsCreateFailure2) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs())
                    .has_value());  // TODO: Check if we are committing here?
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_FALSE(!res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, ExistenceConstraintsViolationOnCommit) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_FALSE(!res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));

    auto res = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue()));
    }

    auto res = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, this->label1, std::set<PropertyId>{this->prop1}}));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue()));
    }
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    ASSERT_TRUE(constraint_acc->DropExistenceConstraint(this->label1, this->prop1).has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsCreateAndDropAndList) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().unique.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::ALREADY_EXISTS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label2, {this->prop1});
    ASSERT_EQ(res, UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1}),
                                     std::make_pair(this->label2, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_EQ(constraint_acc->DropUniqueConstraint(this->label1, {this->prop1}),
              UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_EQ(constraint_acc->DropUniqueConstraint(this->label1, {this->prop1}),
              UniqueConstraints::DeletionStatus::NOT_FOUND);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label2, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_EQ(constraint_acc->DropUniqueConstraint(this->label2, {this->prop1}),
              UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_EQ(constraint_acc->DropUniqueConstraint(this->label2, {this->prop2}),
              UniqueConstraints::DeletionStatus::NOT_FOUND);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().unique.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label2, {this->prop1});
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label2, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsCreateFailure1) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (int i = 0; i < 2; ++i) {
      auto vertex1 = acc->CreateVertex();
      ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs())
                    .has_value());  // TODO: Check if we are committing here?
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc->DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsCreateFailure2) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
    ASSERT_TRUE(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs())
                    .has_value());  // TODO: Check if we are committing here?
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    int value = 0;
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(value)));
      ++value;
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation1) {
  Gid gid1;
  Gid gid2;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop2, PropertyValue(3)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation2) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---SP(v1, 1)---SP(v1, 2)---OK--
    // tx2: -B---SP(v2, 2)---SP(v2, 1)---OK-

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();
    auto vertex2 = acc2->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation3) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---SP(v1, 1)---OK----------------------
    // tx2: --------------------B---SP(v1, 2)---OK--
    // tx3: ---------------------B---SP(v2, 1)---OK-

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();
    auto gid = vertex1.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto acc3 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex2 = acc2->FindVertex(gid, View::NEW);  // vertex1 == vertex2
    auto vertex3 = acc3->CreateVertex();

    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex3.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex3.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    ASSERT_NO_ERROR(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsNoViolation4) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---SP(v1, 1)---OK-----------------------
    // tx2: --------------------B---SP(v2, 1)-----OK-
    // tx3: ---------------------B---SP(v1, 2)---OK--

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();
    auto gid = vertex1.Gid();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto acc3 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex2 = acc2->CreateVertex();
    auto vertex3 = acc3->FindVertex(gid, View::NEW);

    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsViolationOnCommit1) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));
    auto res = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
/// TODO: andi consistency problems
TYPED_TEST(ConstraintsTest, UniqueConstraintsViolationOnCommit2) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---SP(v1, 1)---SP(v2, 2)---OK-----------------------
    // tx2: -------------------------------B---SP(v1, 3)---OK----
    // tx3: --------------------------------B---SP(v2, 3)---FAIL-

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();
    auto vertex2 = acc1->CreateVertex();
    auto gid1 = vertex1.Gid();
    auto gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto acc3 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex3 = acc2->FindVertex(gid1, View::NEW);  // vertex3 == vertex1
    auto vertex4 = acc3->FindVertex(gid2, View::NEW);  // vertex4 == vertex2

    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop1, PropertyValue(3)));
    ASSERT_NO_ERROR(vertex4->SetProperty(this->prop1, PropertyValue(3)));

    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    auto res = acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
/// TODO: andi consistency problems
TYPED_TEST(ConstraintsTest, UniqueConstraintsViolationOnCommit3) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---SP(v1, 1)---SP(v2, 2)---OK-----------------------
    // tx2: -------------------------------B---SP(v1, 2)---FAIL--
    // tx3: --------------------------------B---SP(v2, 1)---FAIL-

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();
    auto vertex2 = acc1->CreateVertex();
    auto gid1 = vertex1.Gid();
    auto gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto acc3 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex3 = acc2->FindVertex(gid1, View::OLD);  // vertex3 == vertex1
    auto vertex4 = acc3->FindVertex(gid2, View::OLD);  // vertex4 == vertex2

    // Setting `this->prop2` shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop2, PropertyValue(3)));
    ASSERT_NO_ERROR(vertex4->SetProperty(this->prop2, PropertyValue(3)));

    ASSERT_NO_ERROR(vertex3->SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex4->SetProperty(this->prop1, PropertyValue(1)));

    auto res = acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
    res = acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsLabelAlteration) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  Gid gid1;
  Gid gid2;
  {
    // B---AL(v2)---SP(v1, 1)---SP(v2, 1)---OK

    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    spdlog::debug("Vertex1 gid: {} Vertex2 gid: {}\n", gid1.ToString(), gid2.ToString());

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label2));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---AL(v1)-----OK-
    // tx2: -B---RL(v2)---OK--

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
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
    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Reapplying labels after first commit shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));

    // Commit the first transaction.
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // B---AL(v2)---FAIL

    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));

    auto res = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1}}));
  }

  {
    // B---RL(v1)---OK

    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // tx1: B---AL(v1)-----FAIL
    // tx2: -B---AL(v2)---OK---

    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->FindVertex(gid1, View::OLD);
    auto vertex2 = acc2->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));

    // Reapply everything.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(this->label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(this->label1));

    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    auto res = acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(ConstraintsTest, UniqueConstraintsPropertySetSize) {
  {
    // This should fail since unique constraint cannot be created for an empty
    // property set.
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::EMPTY_PROPERTIES);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {  // Removing a constraint with empty property set should also fail.
    auto constraint_acc = this->DropConstraintAccessor();
    ASSERT_EQ(constraint_acc->DropUniqueConstraint(this->label1, {}),
              UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Create a set of 33 properties.
  std::set<PropertyId> properties;
  for (int i = 1; i <= 33; ++i) {
    properties.insert(this->storage->NameToProperty("prop" + std::to_string(i)));
  }

  {
    // This should fail since list of properties exceeds the maximum number of
    // properties, which is 32.
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, properties);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {  // An attempt to delete constraint with too large property set should fail.
    auto constraint_acc = this->DropConstraintAccessor();
    ASSERT_EQ(constraint_acc->DropUniqueConstraint(this->label1, properties),
              UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Remove one property from the set.
  properties.erase(properties.begin());

  {
    // Creating a constraint for 32 properties should succeed.
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, properties);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique, UnorderedElementsAre(std::make_pair(this->label1, properties)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {  // Removing a constraint with 32 properties should succeed.
    auto constraint_acc = this->DropConstraintAccessor();
    ASSERT_EQ(constraint_acc->DropUniqueConstraint(this->label1, properties),
              UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    ASSERT_TRUE(acc->ListAllConstraints().unique.empty());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
/// TODO: andi consistency problems
TYPED_TEST(ConstraintsTest, UniqueConstraintsMultipleProperties) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    // An attempt to create an existing unique constraint.
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop2, this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::ALREADY_EXISTS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  Gid gid1;
  Gid gid2;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
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

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Try to change property of the second vertex so it becomes the same as the
  // first vertex-> It should fail.
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop2, PropertyValue(2)));
    auto res = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()),
              (ConstraintViolation{
                  ConstraintViolation::Type::UNIQUE, this->label1, std::set<PropertyId>{this->prop1, this->prop2}}));
  }

  // Then change the second property of both vertex to null. Property values of
  // both vertices should now be equal. However, this operation should succeed
  // since null value is treated as non-existing property.
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->FindVertex(gid1, View::OLD);
    auto vertex2 = acc->FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop2, PropertyValue()));
    ASSERT_NO_ERROR(vertex2->SetProperty(this->prop2, PropertyValue()));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

/// TODO: andi Test passes when ran alone but fails when all tests are run
TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertAbortInsert) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    acc->Abort();
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertRemoveInsert) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  Gid gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_NO_ERROR(acc->DeleteVertex(&*vertex));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertRemoveAbortInsert) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  Gid gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_NO_ERROR(acc->DeleteVertex(&*vertex));
    acc->Abort();
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));

    auto res = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
        std::get<ConstraintViolation>(res.error()),
        (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1, this->prop2}}));
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsDeleteVertexSetProperty) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  Gid gid1;
  Gid gid2;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex2.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.SetProperty(this->prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->FindVertex(gid1, View::OLD);
    auto vertex2 = acc2->FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(acc2->DeleteVertex(&*vertex2));
    ASSERT_NO_ERROR(vertex1->SetProperty(this->prop1, PropertyValue(2)));

    auto res = acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, this->label1, std::set{this->prop1}}));

    ASSERT_NO_ERROR(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsInsertDropInsert) {
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->DropConstraintAccessor();
    ASSERT_EQ(constraint_acc->DropUniqueConstraint(this->label1, {this->prop2, this->prop1}),
              UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TYPED_TEST(ConstraintsTest, UniqueConstraintsComparePropertyValues) {
  // Purpose of this test is to make sure that extracted property values
  // are correctly compared.

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1, this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop2, PropertyValue(0)));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(3)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
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
      auto constraint_acc = this->CreateConstraintAccessor();
      auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
      ASSERT_TRUE(res.has_value());
      ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
      ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    }

    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc2 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex2 = acc2->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex2.SetProperty(this->prop1, memgraph::storage::PropertyValue(2)).has_value());
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);

    auto acc3 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex3 = acc3->FindVertex(vertex.Gid(), memgraph::storage::View::NEW).value();
    ASSERT_TRUE(vertex3.SetProperty(this->prop1, memgraph::storage::PropertyValue(10)).has_value());
    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    ASSERT_EQ(disk_test_utils::GetRealNumberOfEntriesInRocksDB(tx_db), 1);
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraints) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_THROW((void)vertex1.SetProperty(this->prop1, PropertyValue("problem")), memgraph::query::QueryException);
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsInitProperties) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> invalid_props{
        {this->prop1, PropertyValue("problem")}};
    ASSERT_THROW((void)vertex1.InitProperties(invalid_props), memgraph::query::QueryException);
    std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> valid_props{
        {this->prop1, PropertyValue(1)}};
    ASSERT_NO_ERROR(vertex1.InitProperties(valid_props));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsUpdateProperties) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    auto properties1 = std::map<PropertyId, PropertyValue>{{this->prop1, PropertyValue("problem")}};
    ASSERT_THROW((void)vertex1.UpdateProperties(properties1), memgraph::query::QueryException);
    auto properties2 = std::map<PropertyId, PropertyValue>{{this->prop1, PropertyValue(1)}};
    ASSERT_NO_ERROR(vertex1.UpdateProperties(properties2));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsMultiplePropertiesSameLabel) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop2, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_THROW((void)vertex1.SetProperty(this->prop1, PropertyValue("problem")), memgraph::query::QueryException);
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_THROW((void)vertex1.SetProperty(this->prop2, PropertyValue("problem")), memgraph::query::QueryException);
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop2, PropertyValue(1)));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsDuplicate) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_FALSE(res.has_value());
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsAddLabelLast) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue("problem")));
    ASSERT_THROW((void)vertex1.AddLabel(this->label1), memgraph::query::QueryException);
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsAddConstraintLastWithViolation) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue("problem")));
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_FALSE(res.has_value());
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsAddConstraintLastWithoutViolation) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_NO_ERROR(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsWhenItDoesNotApply) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label2));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue("problem")));
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(1)));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsSubtypeCheckForTemporalData) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::DATE);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
    ASSERT_THROW((void)vertex1.SetProperty(this->prop1, PropertyValue("problem")), memgraph::query::QueryException);
    ASSERT_THROW((void)vertex1.SetProperty(this->prop1, PropertyValue(TemporalData{TemporalType::LocalDateTime, 0})),
                 memgraph::query::QueryException);
    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(TemporalData{TemporalType::Date, 0})));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsSubtypeCheckForTemporalDataAddLabelLast) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::DATE);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(TemporalData{TemporalType::LocalDateTime, 0})));
    ASSERT_THROW((void)vertex1.AddLabel(this->label1), memgraph::query::QueryException);
  }

  {
    auto acc1 = this->storage->Access(memgraph::storage::WRITE);
    auto vertex1 = acc1->CreateVertex();

    ASSERT_NO_ERROR(vertex1.SetProperty(this->prop1, PropertyValue(TemporalData{TemporalType::Date, 0})));
    ASSERT_NO_ERROR(vertex1.AddLabel(this->label1));
  }
}

TYPED_TEST(ConstraintsTest, TypeConstraintsDrop) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_FALSE(res.has_value());
  }

  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res1 = constraint_acc->DropTypeConstraint(this->label1, this->prop1, TypeConstraintKind::FLOAT);
    ASSERT_FALSE(res1.has_value());
    auto res2 = constraint_acc->DropTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res2);
  }
}

// Test that setting a property to NULL (removing it) is allowed even when a type constraint exists.
// Type constraints only enforce the type when the property has a value, not its existence.
TYPED_TEST(ConstraintsTest, TypeConstraintsSetPropertyToNull) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  // Create a type constraint requiring INTEGER
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Create a vertex with the label and set the property to an integer (valid)
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(42)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Now try to set the property to NULL (remove it) - this should be allowed
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    // Setting to NULL should NOT throw - type constraints don't require property existence
    ASSERT_NO_ERROR(vertex->SetProperty(this->prop1, PropertyValue()));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify the property is actually removed
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto vertex = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    auto prop_value = vertex->GetProperty(this->prop1, memgraph::storage::View::OLD);
    ASSERT_NO_ERROR(prop_value);
    EXPECT_TRUE(prop_value->IsNull()) << "Property should be NULL (removed)";
    acc->Abort();
  }
}

// Test that UpdateProperties handles NULL values correctly with type constraints
TYPED_TEST(ConstraintsTest, TypeConstraintsUpdatePropertiesToNull) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for on-disk";
  }

  // Create a type constraint requiring INTEGER
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_NO_ERROR(res);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Create a vertex with the label and set the property to an integer (valid)
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(42)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Now try to use UpdateProperties to set the property to NULL
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    auto properties = std::map<PropertyId, PropertyValue>{{this->prop1, PropertyValue()}};
    // UpdateProperties with NULL should NOT throw
    ASSERT_NO_ERROR(vertex->UpdateProperties(properties));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// MVCC visibility tests - verify constraints respect snapshot isolation
// A transaction should only see constraints that were committed before its start_timestamp

TYPED_TEST(ConstraintsTest, ExistenceConstraintMvccSnapshotIsolation) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "MVCC visibility test only applies to InMemoryStorage";
  }

  // Scenario: T1 starts READ -> T2 creates constraint -> T2 commits -> T1 should NOT see it -> T3 CAN see it

  // T1 starts first (gets an earlier start_timestamp)
  auto t1_acc = this->storage->Access(memgraph::storage::READ);

  // T2 creates and commits the constraint
  {
    auto t2_acc = this->CreateConstraintAccessor();
    auto res = t2_acc->CreateExistenceConstraint(this->label1, this->prop1);
    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(t2_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // T1 should NOT see the constraint (its snapshot is from before T2 committed)
  {
    auto constraints = t1_acc->ListAllConstraints();
    EXPECT_EQ(constraints.existence.size(), 0) << "T1 (started before constraint commit) should not see the constraint";
  }

  // T3 starts after T2 committed - it SHOULD see the constraint
  {
    auto t3_acc = this->storage->Access(memgraph::storage::READ);
    auto constraints = t3_acc->ListAllConstraints();
    EXPECT_EQ(constraints.existence.size(), 1) << "T3 (started after commit) should see the constraint";
    EXPECT_THAT(constraints.existence, UnorderedElementsAre(std::make_pair(this->label1, this->prop1)));
    t3_acc->Abort();
  }

  t1_acc->Abort();
}

TYPED_TEST(ConstraintsTest, ExistenceConstraintMvccCreatorCommitsAfterReader) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "MVCC visibility test only applies to InMemoryStorage";
  }

  // Scenario: T1 creates constraint -> T2 starts READ -> T1 commits -> T2 should NOT see it -> T3 CAN see it

  // T1 creates constraint (not yet committed)
  auto t1_acc = this->CreateConstraintAccessor();
  auto res = t1_acc->CreateExistenceConstraint(this->label1, this->prop1);
  ASSERT_TRUE(res.has_value());

  // T2 starts (gets snapshot before T1 commits)
  auto t2_acc = this->storage->Access(memgraph::storage::READ);

  // T1 commits
  ASSERT_TRUE(t1_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  // T2 should NOT see the constraint (its snapshot is from before T1 committed)
  {
    auto constraints = t2_acc->ListAllConstraints();
    EXPECT_EQ(constraints.existence.size(), 0) << "T2 (started before T1 committed) should not see the constraint";
  }

  // T3 starts after T1 committed - it SHOULD see the constraint
  {
    auto t3_acc = this->storage->Access(memgraph::storage::READ);
    auto constraints = t3_acc->ListAllConstraints();
    EXPECT_EQ(constraints.existence.size(), 1) << "T3 (started after commit) should see the constraint";
    t3_acc->Abort();
  }

  t2_acc->Abort();
}

TYPED_TEST(ConstraintsTest, UniqueConstraintMvccSnapshotIsolation) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "MVCC visibility test only applies to InMemoryStorage";
  }

  // T1 starts first
  auto t1_acc = this->storage->Access(memgraph::storage::READ);

  // T2 creates and commits unique constraint
  {
    auto t2_acc = this->CreateConstraintAccessor();
    auto res = t2_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_TRUE(t2_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // T1 should NOT see the constraint
  {
    auto constraints = t1_acc->ListAllConstraints();
    EXPECT_EQ(constraints.unique.size(), 0)
        << "T1 (started before constraint commit) should not see the unique constraint";
  }

  // T3 starts after commit - SHOULD see the constraint
  {
    auto t3_acc = this->storage->Access(memgraph::storage::READ);
    auto constraints = t3_acc->ListAllConstraints();
    EXPECT_EQ(constraints.unique.size(), 1) << "T3 (started after commit) should see the unique constraint";
    t3_acc->Abort();
  }

  t1_acc->Abort();
}

TYPED_TEST(ConstraintsTest, TypeConstraintMvccSnapshotIsolation) {
  if (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "MVCC visibility test only applies to InMemoryStorage";
  }

  // T1 starts first
  auto t1_acc = this->storage->Access(memgraph::storage::READ);

  // T2 creates and commits type constraint
  {
    auto t2_acc = this->CreateConstraintAccessor();
    auto res = t2_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(t2_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // T1 should NOT see the constraint
  {
    auto constraints = t1_acc->ListAllConstraints();
    EXPECT_EQ(constraints.type.size(), 0) << "T1 (started before constraint commit) should not see the type constraint";
  }

  // T3 starts after commit - SHOULD see the constraint
  {
    auto t3_acc = this->storage->Access(memgraph::storage::READ);
    auto constraints = t3_acc->ListAllConstraints();
    EXPECT_EQ(constraints.type.size(), 1) << "T3 (started after commit) should see the type constraint";
    t3_acc->Abort();
  }

  t1_acc->Abort();
}

// Test that re-creating a dropped constraint works (ActiveConstraints pattern)
TYPED_TEST(ConstraintsTest, UniqueConstraintCreateDropCreate) {
  // Create initial constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Drop the constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    EXPECT_EQ(constraint_acc->DropUniqueConstraint(this->label1, {this->prop1}),
              UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint is gone
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().unique.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Re-create the same constraint - this should succeed (was failing before ActiveConstraints fix)
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS)
        << "Re-creating a dropped constraint should succeed";
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists again
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THAT(acc->ListAllConstraints().unique,
                UnorderedElementsAre(std::make_pair(this->label1, std::set<PropertyId>{this->prop1})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// Test that re-creating a dropped existence constraint works (ActiveConstraints pattern)
TYPED_TEST(ConstraintsTest, ExistenceConstraintCreateDropCreate) {
  // Create initial existence constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().existence.size(), 1);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Drop the constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropExistenceConstraint(this->label1, this->prop1);
    EXPECT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint is gone
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().existence.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Re-create the same constraint - this should succeed (was failing before ActiveConstraints fix)
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_TRUE(res.has_value()) << "Re-creating a dropped existence constraint should succeed";
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists again
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().existence.size(), 1);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// Test that re-creating a dropped type constraint works (ActiveConstraints pattern)
TYPED_TEST(ConstraintsTest, TypeConstraintCreateDropCreate) {
  // Skip for DiskStorage - type constraints not implemented
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for DiskStorage";
  }

  // Create initial type constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    EXPECT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().type.size(), 1);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Drop the constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    EXPECT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint is gone
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().type.size(), 0);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Re-create the same constraint - this should succeed (was failing before ActiveConstraints fix)
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    EXPECT_TRUE(res.has_value()) << "Re-creating a dropped type constraint should succeed";
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists again
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().type.size(), 1);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// Test that a constraint validation failure (transaction aborted/rolled back)
// properly cleans up via MVCC status, allowing T2 to create the same constraint.
TYPED_TEST(ConstraintsTest, ExistenceConstraintValidationFailsThenT2Creates) {
  // Create a vertex without the required property - this will cause validation to fail
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(this->label1));
    // Deliberately NOT setting prop1
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // T1: Try to create constraint - should fail validation
  {
    auto t1_acc = this->CreateConstraintAccessor();
    auto res = t1_acc->CreateExistenceConstraint(this->label1, this->prop1);
    ASSERT_FALSE(res.has_value()) << "Constraint creation should fail due to existing vertex without property";
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()).type, ConstraintViolation::Type::EXISTENCE);
    // T1 accessor goes out of scope and aborts (implicit abort on destructor)
  }

  // Fix the vertex by adding the missing property
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(this->prop1, PropertyValue(42)));
    }
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // T2: Should be able to create the same constraint after T1's validation failure
  {
    auto t2_acc = this->CreateConstraintAccessor();
    auto res = t2_acc->CreateExistenceConstraint(this->label1, this->prop1);
    EXPECT_TRUE(res.has_value()) << "T2 should be able to create constraint after T1's validation failure";
    ASSERT_NO_ERROR(t2_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify constraint exists
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllConstraints().existence.size(), 1);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// Tests for constraint metrics
TYPED_TEST(ConstraintsTest, ExistenceConstraintMetrics) {
  auto initial_count = memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveExistenceConstraints);

  // Create first existence constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop1);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveExistenceConstraints), initial_count + 1);

  // Create second existence constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateExistenceConstraint(this->label1, this->prop2);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveExistenceConstraints), initial_count + 2);

  // Drop first constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropExistenceConstraint(this->label1, this->prop1);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveExistenceConstraints), initial_count + 1);

  // Drop second constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropExistenceConstraint(this->label1, this->prop2);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveExistenceConstraints), initial_count);
}

TYPED_TEST(ConstraintsTest, UniqueConstraintMetrics) {
  auto initial_count = memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveUniqueConstraints);

  // Create first unique constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop1});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveUniqueConstraints), initial_count + 1);

  // Create second unique constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateUniqueConstraint(this->label1, {this->prop2});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveUniqueConstraints), initial_count + 2);

  // Drop first constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropUniqueConstraint(this->label1, {this->prop1});
    ASSERT_EQ(res, UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveUniqueConstraints), initial_count + 1);

  // Drop second constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropUniqueConstraint(this->label1, {this->prop2});
    ASSERT_EQ(res, UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveUniqueConstraints), initial_count);
}

TYPED_TEST(ConstraintsTest, TypeConstraintMetrics) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Type constraints not implemented for DiskStorage";
  }

  auto initial_count = memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveTypeConstraints);

  // Create first type constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveTypeConstraints), initial_count + 1);

  // Create second type constraint
  {
    auto constraint_acc = this->CreateConstraintAccessor();
    auto res = constraint_acc->CreateTypeConstraint(this->label1, this->prop2, TypeConstraintKind::STRING);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveTypeConstraints), initial_count + 2);

  // Drop first constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropTypeConstraint(this->label1, this->prop1, TypeConstraintKind::INTEGER);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveTypeConstraints), initial_count + 1);

  // Drop second constraint
  {
    auto constraint_acc = this->DropConstraintAccessor();
    auto res = constraint_acc->DropTypeConstraint(this->label1, this->prop2, TypeConstraintKind::STRING);
    ASSERT_TRUE(res.has_value());
    ASSERT_NO_ERROR(constraint_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_EQ(memgraph::metrics::GetCounterValue(memgraph::metrics::ActiveTypeConstraints), initial_count);
}
