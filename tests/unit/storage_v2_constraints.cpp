#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

bool operator==(const ExistenceConstraintViolation &lhs,
                const ExistenceConstraintViolation &rhs) {
  return lhs.label == rhs.label && lhs.property == rhs.property;
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
TEST_F(ConstraintsTest, CreateAndDrop) {
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(res.HasValue() && !res.GetValue());
  }
  {
    auto res = storage.CreateExistenceConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
  EXPECT_TRUE(storage.DropExistenceConstraint(label1, prop1));
  EXPECT_FALSE(storage.DropExistenceConstraint(label1, prop1));
  EXPECT_TRUE(storage.DropExistenceConstraint(label2, prop1));
  EXPECT_FALSE(storage.DropExistenceConstraint(label2, prop2));
  {
    auto res = storage.CreateExistenceConstraint(label2, prop1);
    EXPECT_TRUE(res.HasValue() && res.GetValue());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(ConstraintsTest, CreateFailure1) {
  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(
        res.HasError() &&
        (res.GetError() == ExistenceConstraintViolation{label1, prop1}));
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
TEST_F(ConstraintsTest, CreateFailure2) {
  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    EXPECT_TRUE(
        res.HasError() &&
        (res.GetError() == ExistenceConstraintViolation{label1, prop1}));
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
TEST_F(ConstraintsTest, ViolationOnCommit) {
  {
    auto res = storage.CreateExistenceConstraint(label1, prop1);
    ASSERT_TRUE(res.HasValue() && res.GetValue());
  }

  {
    auto acc = storage.Access();
    auto vertex = acc.CreateVertex();
    ASSERT_NO_ERROR(vertex.AddLabel(label1));

    auto res = acc.Commit();
    EXPECT_TRUE(
        res.HasError() &&
        (res.GetError() == ExistenceConstraintViolation{label1, prop1}));
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
    EXPECT_TRUE(
        res.HasError() &&
        (res.GetError() == ExistenceConstraintViolation{label1, prop1}));
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
