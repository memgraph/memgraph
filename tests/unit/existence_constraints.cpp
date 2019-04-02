#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"

class ExistenceConstraintsTest : public ::testing::Test {
 public:
  void SetUp() override {
    auto dba = db_.Access();
    label_ = dba->Label("label");
    property_ = dba->Property("property");
    properties_ = {property_};
    rule_ = {label_, properties_};
    dba->Commit();
  }

  storage::constraints::ExistenceConstraints constraints_;
  database::GraphDb db_;
  storage::Label label_;
  storage::Property property_;
  std::vector<storage::Property> properties_;
  storage::constraints::ExistenceRule rule_;
};

TEST_F(ExistenceConstraintsTest, BuildDrop) {
  {
    auto dba = db_.Access();
    EXPECT_FALSE(dba->ExistenceConstraintExists(label_, properties_));
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    dba->BuildExistenceConstraint(label_, properties_);
    EXPECT_TRUE(dba->ExistenceConstraintExists(label_, properties_));
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    dba->DeleteExistenceConstraint(label_, properties_);
    EXPECT_FALSE(dba->ExistenceConstraintExists(label_, properties_));
    dba->Commit();
  }
}

TEST_F(ExistenceConstraintsTest, BuildWithViolation) {
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label_);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    EXPECT_THROW(dba->BuildExistenceConstraint(label_, properties_),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(ExistenceConstraintsTest, InsertFail) {
  {
    auto dba = db_.Access();
    dba->BuildExistenceConstraint(label_, properties_);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    EXPECT_THROW(v.add_label(label_),
                 database::IndexConstraintViolationException);
  }
}

TEST_F(ExistenceConstraintsTest, InsertPass) {
  {
    auto dba = db_.Access();
    dba->BuildExistenceConstraint(label_, properties_);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.PropsSet(property_, PropertyValue("Something"));
    v.add_label(label_);
    dba->Commit();
  }
}

TEST_F(ExistenceConstraintsTest, RemoveFail) {
  {
    auto dba = db_.Access();
    dba->BuildExistenceConstraint(label_, properties_);
    dba->Commit();
  }
  gid::Gid gid;
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.PropsSet(property_, PropertyValue("Something"));
    v.add_label(label_);
    gid = v.gid();
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->FindVertex(gid, false);
    EXPECT_THROW(v.PropsErase(property_),
        database::IndexConstraintViolationException);
  }
}

TEST_F(ExistenceConstraintsTest, RemovePass) {
  {
    auto dba = db_.Access();
    dba->BuildExistenceConstraint(label_, properties_);
    dba->Commit();
  }
  gid::Gid gid;
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.PropsSet(property_, PropertyValue("Something"));
    v.add_label(label_);
    gid = v.gid();
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->FindVertex(gid, false);
    v.remove_label(label_);
    v.PropsErase(property_);
    dba->Commit();
  }
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
