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

#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

class ConstraintsTest : public testing::Test {
 protected:
  ConstraintsTest()
      : prop1(storage.NameToProperty("prop1")),
        prop2(storage.NameToProperty("prop2")),
        label1(storage.NameToLabel("label1")),
        label2(storage.NameToLabel("label2")) {}

  Storage storage;
  PropertyId prop1;
  PropertyId prop2;
  LabelId label1;
  LabelId label2;
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, ExistenceConstraintsCreateAndDrop) {
  EXPECT_EQ(storage.ListAllConstraints().existence.size(), 0);
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(label1, prop1)));
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && !res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(label1, prop1)));
  {
    auto res = storage.CreateExistenceConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(label1, prop1), std::make_pair(label2, prop1)));
  EXPECT_TRUE(storage.DropExistenceConstraint(label1, prop1));
  EXPECT_FALSE(storage.DropExistenceConstraint(label1, prop1));
  EXPECT_THAT(storage.ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(label2, prop1)));
  EXPECT_TRUE(storage.DropExistenceConstraint(label2, prop1));
  EXPECT_FALSE(storage.DropExistenceConstraint(label2, prop2));
  EXPECT_EQ(storage.ListAllConstraints().existence.size(), 0);
  {
    auto res = storage.CreateExistenceConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence, UnorderedElementsAre(std::make_pair(label2, prop1)));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, ExistenceConstraintsCreateFailure1) {
  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label1, std::set<PropertyId>{prop1}}));
  }
  {
    auto acc = storage.Access();
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, ExistenceConstraintsCreateFailure2) {
  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label1, std::set<PropertyId>{prop1}}));
  }
  {
    auto acc = storage.Access();
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, ExistenceConstraintsViolationOnCommit) {
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));

    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::EXISTENCE,
                                                                                label1, std::set<PropertyId>{prop1}}}));
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue()));
    }

    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::EXISTENCE,
                                                                                label1, std::set<PropertyId>{prop1}}}));
  }

  {
    auto acc = storage.Access();
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue()));
    }
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }

    ASSERT_NO_ERROR(acc.Commit());
  }

  ASSERT_TRUE(storage.DropExistenceConstraint(label1, prop1));

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(acc.Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsCreateAndDropAndList) {
  EXPECT_EQ(storage.ListAllConstraints().unique.size(), 0);
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    EXPECT_TRUE(res.HasValue());
    EXPECT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label1, std::set<PropertyId>{prop1})));
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    EXPECT_TRUE(res.HasValue());
    EXPECT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::ALREADY_EXISTS);
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label1, std::set<PropertyId>{prop1})));
  {
    auto res = storage.CreateUniqueConstraint(label2, {prop1});
    EXPECT_TRUE(res.HasValue() && res.GetValue() == UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label1, std::set<PropertyId>{prop1}),
                                   std::make_pair(label2, std::set<PropertyId>{prop1})));
  EXPECT_EQ(storage.DropUniqueConstraint(label1, {prop1}), UniqueConstraints::DeletionStatus::SUCCESS);
  EXPECT_EQ(storage.DropUniqueConstraint(label1, {prop1}), UniqueConstraints::DeletionStatus::NOT_FOUND);
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label2, std::set<PropertyId>{prop1})));
  EXPECT_EQ(storage.DropUniqueConstraint(label2, {prop1}), UniqueConstraints::DeletionStatus::SUCCESS);
  EXPECT_EQ(storage.DropUniqueConstraint(label2, {prop2}), UniqueConstraints::DeletionStatus::NOT_FOUND);
  EXPECT_EQ(storage.ListAllConstraints().unique.size(), 0);
  {
    auto res = storage.CreateUniqueConstraint(label2, {prop1});
    EXPECT_TRUE(res.HasValue());
    EXPECT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label2, std::set<PropertyId>{prop1})));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsCreateFailure1) {
  {
    auto acc = storage.Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex1 = acc.CreateVertex();
      ASSERT_NO_ERROR(vertex1.AddLabel(label1));
      ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1, std::set<PropertyId>{prop1}}));
  }

  {
    auto acc = storage.Access();
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsCreateFailure2) {
  {
    auto acc = storage.Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc.CreateVertex();
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
      ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1, std::set<PropertyId>{prop1}}));
  }

  {
    auto acc = storage.Access();
    int value = 0;
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(value)));
      ++value;
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsNoViolation1) {
  Gid gid1;
  Gid gid2;
  {
    auto acc = storage.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = storage.Access();
    auto vertex1 = acc.FindVertex(gid1, View::OLD);
    auto vertex2 = acc.FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2->AddLabel(label1));
    ASSERT_NO_ERROR(vertex2->SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2->SetProperty(prop2, PropertyValue(3)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    auto vertex1 = acc.FindVertex(gid1, View::OLD);
    auto vertex2 = acc.FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex1->SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2->SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsNoViolation2) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---SP(v1, 2)---OK--
    // tx2: -B---SP(v2, 2)---SP(v2, 1)---OK-

    auto acc1 = storage.Access();
    auto acc2 = storage.Access();
    auto vertex1 = acc1.CreateVertex();
    auto vertex2 = acc2.CreateVertex();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1.Commit());
    ASSERT_NO_ERROR(acc2.Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsNoViolation3) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---OK----------------------
    // tx2: --------------------B---SP(v1, 2)---OK--
    // tx3: ---------------------B---SP(v2, 1)---OK-

    auto acc1 = storage.Access();
    auto vertex1 = acc1.CreateVertex();
    auto gid = vertex1.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1.Commit());

    auto acc2 = storage.Access();
    auto acc3 = storage.Access();
    auto vertex2 = acc2.FindVertex(gid, View::NEW);  // vertex1 == vertex2
    auto vertex3 = acc3.CreateVertex();

    ASSERT_NO_ERROR(vertex2->SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex3.AddLabel(label1));
    ASSERT_NO_ERROR(vertex3.SetProperty(prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc2.Commit());
    ASSERT_NO_ERROR(acc3.Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsNoViolation4) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---OK-----------------------
    // tx2: --------------------B---SP(v2, 1)-----OK-
    // tx3: ---------------------B---SP(v1, 2)---OK--

    auto acc1 = storage.Access();
    auto vertex1 = acc1.CreateVertex();
    auto gid = vertex1.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc1.Commit());

    auto acc2 = storage.Access();
    auto acc3 = storage.Access();
    auto vertex2 = acc2.CreateVertex();
    auto vertex3 = acc3.FindVertex(gid, View::NEW);

    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex3->SetProperty(prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc3.Commit());
    ASSERT_NO_ERROR(acc2.Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsViolationOnCommit1) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = storage.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(1)));
    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::UNIQUE,
                                                                                label1, std::set<PropertyId>{prop1}}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsViolationOnCommit2) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---SP(v2, 2)---OK-----------------------
    // tx2: -------------------------------B---SP(v1, 3)---OK----
    // tx3: --------------------------------B---SP(v2, 3)---FAIL-

    auto acc1 = storage.Access();
    auto vertex1 = acc1.CreateVertex();
    auto vertex2 = acc1.CreateVertex();
    auto gid1 = vertex1.Gid();
    auto gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc1.Commit());

    auto acc2 = storage.Access();
    auto acc3 = storage.Access();
    auto vertex3 = acc2.FindVertex(gid1, View::NEW);  // vertex3 == vertex1
    auto vertex4 = acc3.FindVertex(gid2, View::NEW);  // vertex4 == vertex2

    ASSERT_NO_ERROR(vertex3->SetProperty(prop1, PropertyValue(3)));
    ASSERT_NO_ERROR(vertex4->SetProperty(prop1, PropertyValue(3)));

    ASSERT_NO_ERROR(acc2.Commit());
    auto res = acc3.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::UNIQUE,
                                                                                label1, std::set<PropertyId>{prop1}}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsViolationOnCommit3) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // tx1: B---SP(v1, 1)---SP(v2, 2)---OK-----------------------
    // tx2: -------------------------------B---SP(v1, 2)---FAIL--
    // tx3: --------------------------------B---SP(v2, 1)---FAIL-

    auto acc1 = storage.Access();
    auto vertex1 = acc1.CreateVertex();
    auto vertex2 = acc1.CreateVertex();
    auto gid1 = vertex1.Gid();
    auto gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc1.Commit());

    auto acc2 = storage.Access();
    auto acc3 = storage.Access();
    auto vertex3 = acc2.FindVertex(gid1, View::OLD);  // vertex3 == vertex1
    auto vertex4 = acc3.FindVertex(gid2, View::OLD);  // vertex4 == vertex2

    // Setting `prop2` shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex3->SetProperty(prop2, PropertyValue(3)));
    ASSERT_NO_ERROR(vertex4->SetProperty(prop2, PropertyValue(3)));

    ASSERT_NO_ERROR(vertex3->SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex4->SetProperty(prop1, PropertyValue(1)));

    auto res = acc2.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::UNIQUE,
                                                                                label1, std::set<PropertyId>{prop1}}}));
    res = acc3.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::UNIQUE,
                                                                                label1, std::set<PropertyId>{prop1}}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsLabelAlteration) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid1;
  Gid gid2;
  {
    // B---AL(v2)---SP(v1, 1)---SP(v2, 1)---OK

    auto acc = storage.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label2));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(1)));

    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    // tx1: B---AL(v1)-----OK-
    // tx2: -B---RL(v2)---OK--

    auto acc1 = storage.Access();
    auto acc2 = storage.Access();
    auto vertex1 = acc1.FindVertex(gid1, View::OLD);
    auto vertex2 = acc2.FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->AddLabel(label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(label1));

    // Reapplying labels shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(label1));
    ASSERT_NO_ERROR(vertex1->RemoveLabel(label2));

    // Commit the second transaction.
    ASSERT_NO_ERROR(acc2.Commit());

    // Reapplying labels after first commit shouldn't affect the remaining code.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(label1));

    // Commit the first transaction.
    ASSERT_NO_ERROR(acc1.Commit());
  }

  {
    // B---AL(v2)---FAIL

    auto acc = storage.Access();
    auto vertex2 = acc.FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->AddLabel(label1));

    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{
                                  ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1, std::set{prop1}}}));
  }

  {
    // B---RL(v1)---OK

    auto acc = storage.Access();
    auto vertex1 = acc.FindVertex(gid1, View::OLD);
    ASSERT_NO_ERROR(vertex1->RemoveLabel(label1));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    // tx1: B---AL(v1)-----FAIL
    // tx2: -B---AL(v2)---OK---

    auto acc1 = storage.Access();
    auto acc2 = storage.Access();
    auto vertex1 = acc1.FindVertex(gid1, View::OLD);
    auto vertex2 = acc2.FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(vertex1->AddLabel(label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(label1));

    // Reapply everything.
    ASSERT_NO_ERROR(vertex1->RemoveLabel(label1));
    ASSERT_NO_ERROR(vertex2->RemoveLabel(label1));
    ASSERT_NO_ERROR(vertex1->AddLabel(label1));
    ASSERT_NO_ERROR(vertex2->AddLabel(label1));

    ASSERT_NO_ERROR(acc2.Commit());

    auto res = acc1.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{
                                  ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1, std::set{prop1}}}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsPropertySetSize) {
  {
    // This should fail since unique constraint cannot be created for an empty
    // property set.
    auto res = storage.CreateUniqueConstraint(label1, {});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::EMPTY_PROPERTIES);
  }

  // Removing a constraint with empty property set should also fail.
  ASSERT_EQ(storage.DropUniqueConstraint(label1, {}), UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES);

  // Create a set of 33 properties.
  std::set<PropertyId> properties;
  for (int i = 1; i <= 33; ++i) {
    properties.insert(storage.NameToProperty("prop" + std::to_string(i)));
  }

  {
    // This should fail since list of properties exceeds the maximum number of
    // properties, which is 32.
    auto res = storage.CreateUniqueConstraint(label1, properties);
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED);
  }

  // An attempt to delete constraint with too large property set should fail.
  ASSERT_EQ(storage.DropUniqueConstraint(label1, properties),
            UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED);

  // Remove one property from the set.
  properties.erase(properties.begin());

  {
    // Creating a constraint for 32 properties should succeed.
    auto res = storage.CreateUniqueConstraint(label1, properties);
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  EXPECT_THAT(storage.ListAllConstraints().unique, UnorderedElementsAre(std::make_pair(label1, properties)));

  // Removing a constraint with 32 properties should succeed.
  ASSERT_EQ(storage.DropUniqueConstraint(label1, properties), UniqueConstraints::DeletionStatus::SUCCESS);
  ASSERT_TRUE(storage.ListAllConstraints().unique.empty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsMultipleProperties) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    // An attempt to create an existing unique constraint.
    auto res = storage.CreateUniqueConstraint(label1, {prop2, prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::ALREADY_EXISTS);
  }

  Gid gid1;
  Gid gid2;
  {
    auto acc = storage.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop2, PropertyValue(2)));

    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop2, PropertyValue(3)));

    ASSERT_NO_ERROR(acc.Commit());
  }

  // Try to change property of the second vertex so it becomes the same as the
  // first vertex. It should fail.
  {
    auto acc = storage.Access();
    auto vertex2 = acc.FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->SetProperty(prop2, PropertyValue(2)));
    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{
                                  ConstraintViolation::Type::UNIQUE, label1, std::set<PropertyId>{prop1, prop2}}}));
  }

  // Then change the second property of both vertex to null. Property values of
  // both vertices should now be equal. However, this operation should succeed
  // since null value is treated as non-existing property.
  {
    auto acc = storage.Access();
    auto vertex1 = acc.FindVertex(gid1, View::OLD);
    auto vertex2 = acc.FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex1->SetProperty(prop2, PropertyValue()));
    ASSERT_NO_ERROR(vertex2->SetProperty(prop2, PropertyValue()));
    ASSERT_NO_ERROR(acc.Commit());
  }
}

TEST_F(ConstraintsTest, UniqueConstraintsInsertAbortInsert) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    acc.Abort();
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }
}

TEST_F(ConstraintsTest, UniqueConstraintsInsertRemoveInsert) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid;
  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_NO_ERROR(acc.DeleteVertex(&*vertex));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc.Commit());
  }
}

TEST_F(ConstraintsTest, UniqueConstraintsInsertRemoveAbortInsert) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid;
  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.FindVertex(gid, View::OLD);
    ASSERT_NO_ERROR(acc.DeleteVertex(&*vertex));
    acc.Abort();
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(2)));

    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{ConstraintViolation{ConstraintViolation::Type::UNIQUE,
                                                                                label1, std::set{prop1, prop2}}}));
  }
}

TEST_F(ConstraintsTest, UniqueConstraintsDeleteVertexSetProperty) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  Gid gid1;
  Gid gid2;
  {
    auto acc = storage.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    gid1 = vertex1.Gid();
    gid2 = vertex2.Gid();

    ASSERT_NO_ERROR(vertex1.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex1.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(2)));

    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc1 = storage.Access();
    auto acc2 = storage.Access();
    auto vertex1 = acc1.FindVertex(gid1, View::OLD);
    auto vertex2 = acc2.FindVertex(gid2, View::OLD);

    ASSERT_NO_ERROR(acc2.DeleteVertex(&*vertex2));
    ASSERT_NO_ERROR(vertex1->SetProperty(prop1, PropertyValue(2)));

    auto res = acc1.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(), (StorageDataManipulationError{
                                  ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1, std::set{prop1}}}));

    ASSERT_NO_ERROR(acc2.Commit());
  }
}

TEST_F(ConstraintsTest, UniqueConstraintsInsertDropInsert) {
  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  ASSERT_EQ(storage.DropUniqueConstraint(label1, {prop2, prop1}), UniqueConstraints::DeletionStatus::SUCCESS);

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }
}

TEST_F(ConstraintsTest, UniqueConstraintsComparePropertyValues) {
  // Purpose of this test is to make sure that extracted property values
  // are correctly compared.

  {
    auto res = storage.CreateUniqueConstraint(label1, {prop1, prop2});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(1)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(1)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(2)));
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop2, PropertyValue(0)));
    ASSERT_NO_ERROR(vertex.SetProperty(prop1, PropertyValue(3)));
    ASSERT_NO_ERROR(acc.Commit());
  }
}
