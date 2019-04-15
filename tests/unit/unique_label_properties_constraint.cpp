
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/single_node/constraints/unique_label_properties_constraint.hpp"

class UniqueLabelPropertiesTest : public ::testing::Test {
 public:
  void SetUp() override {
    auto dba = db_.AccessBlocking();
    label_ = dba.Label("label");
    property1_ = dba.Property("property1");
    property2_ = dba.Property("property2");
    property3_ = dba.Property("property3");
    constraint_.AddConstraint(label_, {property1_, property2_, property3_},
                              dba.transaction());
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
  storage::constraints::UniqueLabelPropertiesConstraint constraint_;
};

TEST_F(UniqueLabelPropertiesTest, BuildDrop) {
  {
    auto dba = db_.Access();
    EXPECT_TRUE(
        constraint_.Exists(label_, {property2_, property1_, property3_}));
    EXPECT_TRUE(
        constraint_.Exists(label_, {property1_, property2_, property3_}));
    EXPECT_FALSE(
        constraint_.Exists(label_, {property1_, property2_}));
    dba.Commit();
  }
  {
    auto dba = db_.AccessBlocking();
    constraint_.RemoveConstraint(label_, {property2_, property1_, property3_});
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    EXPECT_FALSE(
        constraint_.Exists(label_, {property2_, property1_, property3_}));
    EXPECT_FALSE(
        constraint_.Exists(label_, {property1_, property2_, property3_}));
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertiesTest, BuildWithViolation) {
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
  auto v4 = dba2.FindVertex(v1.gid(), false);
  auto v5 = dba2.FindVertex(v2.gid(), false);
  auto v6 = dba2.FindVertex(v3.gid(), false);
  constraint_.UpdateOnAddLabel(label_, v4, dba2.transaction());
  constraint_.UpdateOnAddLabel(label_, v5, dba2.transaction());
  EXPECT_THROW(constraint_.UpdateOnAddLabel(label_, v6, dba2.transaction()),
               database::IndexConstraintViolationException);
}

TEST_F(UniqueLabelPropertiesTest, InsertInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property1_, value1_, v,
                                                 dba.transaction()),
                 database::IndexConstraintViolationException);
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertiesTest, InsertInsertDiffValues) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    PropertyValue other3("Some other value 3");
    v.PropsSet(property3_, other3);
    constraint_.UpdateOnAddProperty(property3_, other3, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    PropertyValue other2("Some other value 2");
    v.PropsSet(property2_, other2);
    constraint_.UpdateOnAddProperty(property2_, other2, v, dba.transaction());
    PropertyValue other1("Some other value 1");
    v.PropsSet(property1_, other1);
    constraint_.UpdateOnAddProperty(property1_, other1, v, dba.transaction());
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertiesTest, InsertAbortInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    dba.Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    dba.Commit();
  }
}

TEST_F(UniqueLabelPropertiesTest, InsertRemoveAbortInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.FindVertex(gid, false);
    v.PropsErase(property2_);
    constraint_.UpdateOnRemoveProperty(property2_, value2_, v,
                                       dba.transaction());
    dba.Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property3_, value3_, v,
                                                 dba.transaction()),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertiesTest, InsertInsertSameTransaction) {
  {
    auto dba = db_.Access();
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    v2.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v2, dba.transaction());
    v1.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v1, dba.transaction());
    v1.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v1,
                                    dba.transaction());
    v1.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v1,
                                    dba.transaction());
    v2.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v2,
                                    dba.transaction());
    v2.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v2,
                                    dba.transaction());
    v2.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v2,
                                    dba.transaction());

    v1.PropsSet(property3_, value3_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property3_, value3_, v1,
                                                 dba.transaction()),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertiesTest, InsertInsertReversed) {
    auto dba1 = db_.Access();
    auto dba2 = db_.Access();
    auto v1 = dba1.InsertVertex();
    auto v2 = dba2.InsertVertex();
    v2.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v2, dba2.transaction());
    v1.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v1, dba1.transaction());
    v1.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v1,
                                    dba1.transaction());
    v1.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v1,
                                    dba1.transaction());
    v2.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v2,
                                    dba2.transaction());
    v2.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v2,
                                    dba2.transaction());
    v2.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v2,
                                    dba2.transaction());

    v1.PropsSet(property3_, value3_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property3_, value3_, v1,
                                                 dba1.transaction()),
                 mvcc::SerializationError);
}

TEST_F(UniqueLabelPropertiesTest, InsertRemoveInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.FindVertex(gid, false);
    v.PropsErase(property2_);
    constraint_.UpdateOnRemoveProperty(property2_, value2_, v,
                                       dba.transaction());
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
  }
}

TEST_F(UniqueLabelPropertiesTest, InsertRemoveInsertSameTransaction) {
  auto dba = db_.Access();
  auto v = dba.InsertVertex();
  v.PropsSet(property2_, value2_);
  constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
  v.PropsSet(property1_, value1_);
  constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
  v.PropsSet(property3_, value3_);
  constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
  v.add_label(label_);
  constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
  v.PropsErase(property2_);
  constraint_.UpdateOnRemoveProperty(property2_, value2_, v,
                                     dba.transaction());
  v.PropsSet(property2_, value2_);
  constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
}

TEST_F(UniqueLabelPropertiesTest, InsertDropInsert) {
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
  }
  {
    auto dba = db_.AccessBlocking();
    constraint_.RemoveConstraint(label_, {property2_, property3_, property1_});
    dba.Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba.InsertVertex();
    v.PropsSet(property2_, value2_);
    constraint_.UpdateOnAddProperty(property2_, value2_, v, dba.transaction());
    v.PropsSet(property1_, value1_);
    constraint_.UpdateOnAddProperty(property1_, value1_, v, dba.transaction());
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba.transaction());
    v.PropsSet(property3_, value3_);
    constraint_.UpdateOnAddProperty(property3_, value3_, v, dba.transaction());
    dba.Commit();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
