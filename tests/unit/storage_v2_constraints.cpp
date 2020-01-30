#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace storage;

using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

bool operator==(const ConstraintViolation &lhs,
                const ConstraintViolation &rhs) {
  return lhs.type == rhs.type && lhs.label == rhs.label &&
         lhs.property == rhs.property;
}

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
  EXPECT_THAT(storage.ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(label1, prop1)));
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && !res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(label1, prop1)));
  {
    auto res = storage.CreateExistenceConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(label1, prop1),
                                   std::make_pair(label2, prop1)));
  EXPECT_TRUE(storage.DropExistenceConstraint(label1, prop1));
  EXPECT_FALSE(storage.DropExistenceConstraint(label1, prop1));
  EXPECT_THAT(storage.ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(label2, prop1)));
  EXPECT_TRUE(storage.DropExistenceConstraint(label2, prop1));
  EXPECT_FALSE(storage.DropExistenceConstraint(label2, prop2));
  EXPECT_EQ(storage.ListAllConstraints().existence.size(), 0);
  {
    auto res = storage.CreateExistenceConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().existence,
              UnorderedElementsAre(std::make_pair(label2, prop1)));
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
              (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label1,
                                   prop1}));
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
              (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label1,
                                   prop1}));
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
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label1,
                                   prop1}));
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
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label1,
                                   prop1}));
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
TEST_F(ConstraintsTest, UniqueConstraintsCreateAndDrop) {
  EXPECT_EQ(storage.ListAllConstraints().unique.size(), 0);
  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label1, prop1)));
  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && !res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label1, prop1)));
  {
    auto res = storage.CreateUniqueConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label1, prop1),
                                   std::make_pair(label2, prop1)));
  EXPECT_TRUE(storage.DropUniqueConstraint(label1, prop1));
  EXPECT_FALSE(storage.DropUniqueConstraint(label1, prop1));
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label2, prop1)));
  EXPECT_TRUE(storage.DropUniqueConstraint(label2, prop1));
  EXPECT_FALSE(storage.DropUniqueConstraint(label2, prop2));
  EXPECT_EQ(storage.ListAllConstraints().unique.size(), 0);
  {
    auto res = storage.CreateUniqueConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_THAT(storage.ListAllConstraints().unique,
              UnorderedElementsAre(std::make_pair(label2, prop1)));
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
  }

  {
    auto acc = storage.Access();
    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    EXPECT_TRUE(!res.HasError() && res.GetValue());
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    EXPECT_TRUE(!res.HasError() && res.GetValue());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsNoViolation1) {
  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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
    ASSERT_NO_ERROR(vertex2.AddLabel(label1));
    ASSERT_NO_ERROR(vertex2.SetProperty(prop1, PropertyValue(2)));
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsViolationOnCommit2) {
  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsViolationOnCommit3) {
  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() & res.GetValue());
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

    ASSERT_NO_ERROR(vertex3->SetProperty(prop1, PropertyValue(2)));
    ASSERT_NO_ERROR(vertex4->SetProperty(prop1, PropertyValue(1)));

    auto res = acc2.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
    res = acc3.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, UniqueConstraintsLabelAlteration) {
  {
    auto res = storage.CreateUniqueConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
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

    ASSERT_NO_ERROR(acc2.Commit());
    ASSERT_NO_ERROR(acc1.Commit());
  }

  {
    // B---AL(v2)---FAIL

    auto acc = storage.Access();
    auto vertex2 = acc.FindVertex(gid2, View::OLD);
    ASSERT_NO_ERROR(vertex2->AddLabel(label1));

    auto res = acc.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
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

    ASSERT_NO_ERROR(acc2.Commit());

    auto res = acc1.Commit();
    ASSERT_TRUE(res.HasError());
    EXPECT_EQ(res.GetError(),
              (ConstraintViolation{ConstraintViolation::Type::UNIQUE, label1,
                                   prop1}));
  }
}
