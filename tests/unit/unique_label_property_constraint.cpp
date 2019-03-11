#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/single_node/constraints/unique_label_property_constraint.hpp"

class UniqueLabelPropertyTest : public ::testing::Test {
 public:
  void SetUp() override {
    auto dba = db_.AccessBlocking();
    label_ = dba->Label("label");
    property_ = dba->Property("property");
    constraint_.AddConstraint(label_, property_, dba->transaction());
    dba->Commit();
  }

  database::GraphDb db_;
  storage::Label label_;
  storage::Property property_;
  PropertyValue value_{"value"};
  storage::constraints::UniqueLabelPropertyConstraint constraint_;
};

TEST_F(UniqueLabelPropertyTest, BuildDrop) {
  {
    auto dba = db_.Access();
    EXPECT_TRUE(constraint_.Exists(label_, property_));
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    constraint_.RemoveConstraint(label_, property_);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    EXPECT_FALSE(constraint_.Exists(label_, property_));
    dba->Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, BuildWithViolation) {
  auto dba1 = db_.Access();
  auto v1 = dba1->InsertVertex();
  v1.add_label(label_);
  v1.PropsSet(property_, value_);

  auto v2 = dba1->InsertVertex();
  v2.add_label(label_);
  v2.PropsSet(property_, value_);
  dba1->Commit();

  auto dba2 = db_.Access();
  auto v3 = dba2->FindVertex(v1.gid(), false);
  auto v4 = dba2->FindVertex(v2.gid(), false);
  constraint_.UpdateOnAddLabel(label_, v3, dba2->transaction());
  EXPECT_THROW(constraint_.UpdateOnAddLabel(label_, v4, dba2->transaction()),
               database::IndexConstraintViolationException);
  EXPECT_THROW(constraint_.UpdateOnAddProperty(property_, value_, v4,
                                               dba2->transaction()),
               database::IndexConstraintViolationException);
}

TEST_F(UniqueLabelPropertyTest, InsertInsert) {
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property_, value_, v,
                                                 dba->transaction()),
                 database::IndexConstraintViolationException);
    EXPECT_THROW(constraint_.UpdateOnAddLabel(label_, v, dba->transaction()),
                 database::IndexConstraintViolationException);
    dba->Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, InsertInsertDiffValues) {
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    PropertyValue other_value{"Some other value"};
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, other_value);
    constraint_.UpdateOnAddProperty(property_, other_value, v,
                                    dba->transaction());
    dba->Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, InsertAbortInsert) {
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    dba->Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    dba->Commit();
  }
}

TEST_F(UniqueLabelPropertyTest, InsertRemoveAbortInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    gid = v.gid();
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->FindVertex(gid, false);
    v.PropsErase(property_);
    constraint_.UpdateOnRemoveProperty(property_, value_, v,
                                       dba->transaction());
    dba->Abort();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property_, value_, v,
                                                 dba->transaction()),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertyTest, InsertInsertSameTransaction) {
  {
    auto dba = db_.Access();
    auto v1 = dba->InsertVertex();
    v1.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v1, dba->transaction());
    v1.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v1, dba->transaction());

    auto v2 = dba->InsertVertex();
    v2.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v2, dba->transaction());
    v2.PropsSet(property_, value_);
    EXPECT_THROW(constraint_.UpdateOnAddProperty(property_, value_, v2,
                                                 dba->transaction()),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(UniqueLabelPropertyTest, InsertInsertReversed) {
  auto dba1 = db_.Access();
  auto dba2 = db_.Access();

  auto v2 = dba2->InsertVertex();
  v2.add_label(label_);
  constraint_.UpdateOnAddLabel(label_, v2, dba2->transaction());
  v2.PropsSet(property_, value_);
  constraint_.UpdateOnAddProperty(property_, value_, v2, dba2->transaction());
  dba2->Commit();

  auto v1 = dba1->InsertVertex();
  v1.add_label(label_);
  constraint_.UpdateOnAddLabel(label_, v1, dba1->transaction());
  v1.PropsSet(property_, value_);
  EXPECT_THROW(constraint_.UpdateOnAddProperty(property_, value_, v1,
                                               dba1->transaction()),
               mvcc::SerializationError);
}

TEST_F(UniqueLabelPropertyTest, InsertRemoveInsert) {
  gid::Gid gid = 0;
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    gid = v.gid();
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->FindVertex(gid, false);
    v.PropsErase(property_);
    constraint_.UpdateOnRemoveProperty(property_, value_, v,
                                       dba->transaction());
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
  }
}

TEST_F(UniqueLabelPropertyTest, InsertRemoveInsertSameTransaction) {
  auto dba = db_.Access();
  auto v = dba->InsertVertex();
  v.add_label(label_);
  constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
  v.PropsSet(property_, value_);
  constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
  v.PropsErase(property_);
  constraint_.UpdateOnRemoveProperty(property_, value_, v, dba->transaction());
  v.PropsSet(property_, value_);
  constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
  dba->Commit();
}

TEST_F(UniqueLabelPropertyTest, InsertDropInsert) {
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    dba->Commit();
  }
  {
    auto dba = db_.AccessBlocking();
    constraint_.RemoveConstraint(label_, property_);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    constraint_.UpdateOnAddLabel(label_, v, dba->transaction());
    v.PropsSet(property_, value_);
    constraint_.UpdateOnAddProperty(property_, value_, v, dba->transaction());
    dba->Commit();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
