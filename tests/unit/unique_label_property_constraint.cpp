#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"

class UniqueLabelPropertyTest : public ::testing::Test {
 public:
  void SetUp() override {
    auto dba = db_.Access();
    label_ = dba.Label("label");
    property_ = dba.Property("property");
    dba.BuildUniqueConstraint(label_, property_);
    dba.Commit();
  }

  database::GraphDb db_;
  storage::Label label_;
  storage::Property property_;
  PropertyValue value_{"value"};
};

TEST_F(UniqueLabelPropertyTest, BuildDrop) {
  {
    auto dba = db_.Access();
    EXPECT_TRUE(dba.UniqueConstraintExists(label_, property_));
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    dba.DeleteUniqueConstraint(label_, property_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    EXPECT_FALSE(dba.UniqueConstraintExists(label_, property_));
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, BuildWithViolation) {
  auto dba1 = db_.Access();
  auto l1 = dba1.Label("l1");
  auto p1 = dba1.Property("p1");

  auto v1 = dba1.InsertVertex();
  v1.add_label(l1);
  v1.PropsSet(p1, value_);

  auto v2 = dba1.InsertVertex();
  v2.add_label(l1);
  v2.PropsSet(p1, value_);
  dba1.Commit();

  auto dba2 = db_.Access();
  EXPECT_THROW(dba2.BuildUniqueConstraint(l1, p1),
               database::IndexConstraintViolationException);
}

TEST_F(UniqueLabelPropertyTest, InsertInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    EXPECT_THROW(v.PropsSet(property_, value_),
        database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertyTest, InsertInsertDiffValues) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    PropertyValue other_value{"Some other value"};
    v.add_label(label_);
    v.PropsSet(property_, other_value);
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, InsertAbortInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    dba.Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, InsertRemoveAbortInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    gid = v.gid();
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.FindVertex(gid, false);
    v.PropsErase(property_);
    dba.Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    EXPECT_THROW(v.PropsSet(property_, value_),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertyTest, InsertInsertSameTransaction) {
  {
    auto dba = db_.Access();
    auto v1 = dba.InsertVertex();
    v1.add_label(label_);
    v1.PropsSet(property_, value_);

    auto v2 = dba.InsertVertex();
    v2.add_label(label_);
    EXPECT_THROW(v2.PropsSet(property_, value_),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertyTest, InsertInsertReversed) {
  auto dba1 = db_.Access();
  auto dba2 = db_.Access();

  auto v2 = dba2.InsertVertex();
  v2.add_label(label_);
  v2.PropsSet(property_, value_);
  dba2.Commit();

  auto v1 = dba1.InsertVertex();
  v1.add_label(label_);
  EXPECT_THROW(v1.PropsSet(property_, value_),
               mvcc::SerializationError);
}

TEST_F(UniqueLabelPropertyTest, InsertRemoveInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    gid = v.gid();
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.FindVertex(gid, false);
    v.PropsErase(property_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
  }
}

TEST_F(UniqueLabelPropertyTest, InsertRemoveInsertSameTransaction) {
  auto dba = db_.Access();
  auto v = dba.InsertVertex();
  v.add_label(label_);
  v.PropsSet(property_, value_);
  v.PropsErase(property_);
  v.PropsSet(property_, value_);
  dba.Commit();
}

TEST_F(UniqueLabelPropertyTest, InsertDropInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    dba.DeleteUniqueConstraint(label_, property_);
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    v.PropsSet(property_, value_);
    dba.Commit();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
