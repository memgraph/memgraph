#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/single_node/constraints/existence_constraints.hpp"

class ExistenceConstraintsTest : public ::testing::Test {
 public:
  void SetUp() override {}
  database::ExistenceConstraints constraints_;
  database::GraphDb db_;
};

TEST_F(ExistenceConstraintsTest, MultiBuildDrop) {
  auto d = db_.Access();
  auto label = d->Label("label");
  auto prop = d->Property("property");
  database::ExistenceRule rule{label, std::vector<storage::Property>{prop}};
  d->Commit();

  {
    auto dba = db_.AccessBlocking();
    constraints_.AddConstraint(rule);
    EXPECT_TRUE(constraints_.Exists(rule));
    dba->Commit();
  }
  {
    auto dba = db_.AccessBlocking();
    constraints_.RemoveConstraint(rule);
    EXPECT_FALSE(constraints_.Exists(rule));
    dba->Commit();
  }
}

TEST_F(ExistenceConstraintsTest, InsertTest) {
  auto d = db_.Access();
  auto label = d->Label("label");
  auto prop = d->Property("property");
  database::ExistenceRule rule{label, std::vector<storage::Property>{prop}};
  d->Commit();

  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label);
    EXPECT_TRUE(constraints_.CheckIfSatisfies(v.GetNew()));
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    bool can_add_constraint = true;
    for (auto v : dba->Vertices(false)) {
      if (!database::CheckIfSatisfiesExistenceRule(v.GetOld(), rule)) {
        can_add_constraint = false;
      }
    }

    EXPECT_FALSE(can_add_constraint);
    dba->Commit();
  }
  {
    auto dba = db_.AccessBlocking();
    constraints_.AddConstraint(rule);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v1 = dba->InsertVertex();
    v1.add_label(label);
    EXPECT_FALSE(
        constraints_.CheckIfSatisfies(v1.GetNew()));
    auto v2 = dba->InsertVertex();
    v2.PropsSet(prop, PropertyValue(false));
    v2.add_label(label);
    EXPECT_TRUE(constraints_.CheckIfSatisfies(v2.GetNew()));
    dba->Commit();
  }
  {
    auto dba = db_.AccessBlocking();
    constraints_.RemoveConstraint(rule);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    auto v = dba->InsertVertex();
    v.add_label(label);
    EXPECT_TRUE(constraints_.CheckIfSatisfies(v.GetNew()));
    dba->Commit();
  }
}

TEST_F(ExistenceConstraintsTest, GraphDbAccessor) {
  auto d = db_.Access();
  auto label = d->Label("label");
  auto prop = d->Property("property");
  auto properties = std::vector<storage::Property>{prop};
  d->Commit();

  {
    auto dba = db_.Access();
    dba->BuildExistenceConstraint(label, properties);
    // Constraint is not visible because transaction creates blocking
    // transaction with different id;
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    EXPECT_TRUE(dba->ExistenceConstraintExists(label, properties));
    auto v1 = dba->InsertVertex();
    EXPECT_THROW(v1.add_label(label),
                 database::IndexConstraintViolationException);
    auto v2 = dba->InsertVertex();
    v2.PropsSet(prop, PropertyValue(false));
    v2.add_label(label);
    EXPECT_THROW(v2.PropsErase(prop),
                 database::IndexConstraintViolationException);
    v2.remove_label(label);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    dba->DeleteExistenceConstraint(label, properties);
    dba->Commit();
  }
  {
    auto dba = db_.Access();
    EXPECT_FALSE(dba->ExistenceConstraintExists(label, properties));
    auto v1 = dba->InsertVertex();
    v1.add_label(label);
    dba->Commit();
  }
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
