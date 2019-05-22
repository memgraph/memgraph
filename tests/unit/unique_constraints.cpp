#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/common/constraints/unique_constraints.hpp"

using storage::constraints::ConstraintEntry;
using storage::constraints::UniqueConstraints;

class UniqueConstraintsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto dba = db_.Access();
    label_ = dba.Label("label");
    property1_ = dba.Property("property1");
    property2_ = dba.Property("property2");
    property3_ = dba.Property("property3");
    dba.BuildUniqueConstraint(label_, {property1_, property2_, property3_});
    dba.Commit();
  }

  database::GraphDb db_;
  storage::Label label_;
  storage::Property property1_;
  storage::Property property2_;
  storage::Property property3_;
  PropertyValue value1_{"value1"};
  PropertyValue value2_{"value2"};
  PropertyValue value3_{"value3"};
  UniqueConstraints constraints_;
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, BuildDrop) {
  auto constraint =
      ConstraintEntry{label_, {property1_, property2_, property3_}};
  constraints_.AddConstraint(constraint);

  EXPECT_TRUE(
      constraints_.Exists(label_, {property2_, property1_, property3_}));
  EXPECT_TRUE(
      constraints_.Exists(label_, {property1_, property2_, property3_}));
  EXPECT_FALSE(constraints_.Exists(label_, {property2_, property3_}));

  constraints_.RemoveConstraint(constraint);

  EXPECT_FALSE(
      constraints_.Exists(label_, {property2_, property1_, property3_}));
  EXPECT_FALSE(
      constraints_.Exists(label_, {property1_, property2_, property3_}));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, BuildWithViolation) {
  auto dba = db_.Access();
  dba.DeleteUniqueConstraint(label_, {property1_, property2_, property3_});
  dba.Commit();

  auto dba1 = db_.Access();
  auto v1 = dba1.InsertVertex();
  v1.add_label(label_);
  v1.PropsSet(property1_, value1_);
  v1.PropsSet(property2_, value2_);
  v1.PropsSet(property3_, value3_);

  auto v2 = dba1.InsertVertex();
  v2.add_label(label_);
  v2.PropsSet(property1_, value1_);
  v2.PropsSet(property3_, value3_);

  auto v3 = dba1.InsertVertex();
  v3.add_label(label_);
  v3.PropsSet(property3_, value3_);
  v3.PropsSet(property1_, value1_);
  v3.PropsSet(property2_, value2_);
  dba1.Commit();

  auto dba2 = db_.Access();
  EXPECT_THROW(
      dba2.BuildUniqueConstraint(label_, {property1_, property2_, property3_}),
      database::ConstraintViolationException);
  dba2.Commit();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property2_, value2_);
    v.PropsSet(property1_, value1_);
    v.PropsSet(property3_, value3_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property3_, value3_);
    v.PropsSet(property2_, value2_);
    EXPECT_THROW(v.PropsSet(property1_, value1_),
                 database::ConstraintViolationException);
    dba.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertInsertDiffValues) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property2_, value2_);
    v.PropsSet(property1_, value1_);
    v.PropsSet(property3_, value3_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    PropertyValue other3("Some other value 3");
    v.PropsSet(property3_, other3);
    v.add_label(label_);
    PropertyValue other2("Some other value 2");
    v.PropsSet(property2_, other2);
    PropertyValue other1("Some other value 1");
    v.PropsSet(property1_, other1);
    dba.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertAbortInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    v.add_label(label_);
    v.PropsSet(property2_, value2_);
    v.PropsSet(property3_, value3_);
    dba.Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    v.PropsSet(property1_, value1_);
    v.PropsSet(property3_, value3_);
    v.add_label(label_);
    dba.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertRemoveAbortInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    v.PropsSet(property1_, value1_);
    v.PropsSet(property3_, value3_);
    v.add_label(label_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.FindVertex(gid, false);
    v.PropsErase(property2_);
    dba.Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    v.add_label(label_);
    v.PropsSet(property2_, value2_);
    EXPECT_THROW(v.PropsSet(property3_, value3_),
                 database::ConstraintViolationException);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertInsertSameTransaction) {
  {
    auto dba = db_.Access();
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    v1.add_label(label_);
    v2.add_label(label_);

    v1.PropsSet(property1_, value1_);
    v1.PropsSet(property2_, value2_);

    v2.PropsSet(property2_, value2_);
    v2.PropsSet(property3_, value3_);
    v2.PropsSet(property1_, value1_);

    EXPECT_THROW(v1.PropsSet(property3_, value3_),
                 database::ConstraintViolationException);
    dba.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertInsertReversed) {
  auto dba1 = db_.Access();
  auto dba2 = db_.Access();
  auto v1 = dba1.InsertVertex();
  auto v2 = dba2.InsertVertex();
  v1.add_label(label_);
  v2.add_label(label_);

  v1.PropsSet(property1_, value1_);
  v1.PropsSet(property2_, value2_);

  v2.PropsSet(property2_, value2_);
  v2.PropsSet(property3_, value3_);
  v2.PropsSet(property1_, value1_);

  EXPECT_THROW(v1.PropsSet(property3_, value3_), mvcc::SerializationError);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertRemoveInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    v.PropsSet(property1_, value1_);
    v.PropsSet(property3_, value3_);
    v.add_label(label_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.FindVertex(gid, false);
    v.PropsErase(property2_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    v.add_label(label_);
    v.PropsSet(property2_, value2_);
    v.PropsSet(property3_, value3_);
    dba.Commit();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertRemoveInsertSameTransaction) {
  auto dba = db_.Access();
  auto v = dba.InsertVertex();
  v.PropsSet(property2_, value2_);
  v.PropsSet(property1_, value1_);
  v.PropsSet(property3_, value3_);
  v.add_label(label_);
  v.PropsErase(property2_);
  v.PropsSet(property2_, value2_);
  dba.Commit();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(UniqueConstraintsTest, InsertDropInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    v.add_label(label_);
    v.PropsSet(property2_, value2_);
    v.PropsSet(property3_, value3_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    dba.DeleteUniqueConstraint(label_, {property2_, property3_, property1_});
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    v.PropsSet(property1_, value1_);
    v.add_label(label_);
    v.PropsSet(property3_, value3_);
    dba.Commit();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
