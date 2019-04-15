#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node/indexes/label_property_index.hpp"
#include "transactions/single_node/engine.hpp"

#include "mvcc_gc_common.hpp"

using namespace database;

class LabelPropertyIndexComplexTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    auto accessor = db_.Access();

    label = accessor.Label("label");
    property = accessor.Property("property");
    label2 = accessor.Label("label2");
    property2 = accessor.Property("property2");

    key = new LabelPropertyIndex::Key(label, property);
    EXPECT_EQ(index.CreateIndex(*key), true);

    t = engine.Begin();
    vlist = new mvcc::VersionList<Vertex>(*t, 0);
    engine.Advance(t->id_);

    vertex = vlist->find(*t);
    ASSERT_NE(vertex, nullptr);
    vertex->labels_.push_back(label);
    vertex->properties_.set(property, PropertyValue(0));

    EXPECT_EQ(index.Count(*key), 0);
  }

  virtual void TearDown() {
    delete key;
    delete vlist;
  }

 public:
  GraphDb db_;
  LabelPropertyIndex index;
  LabelPropertyIndex::Key *key;

  tx::Engine engine;
  tx::Transaction *t{nullptr};

  mvcc::VersionList<Vertex> *vlist;
  Vertex *vertex;

  storage::Label label;
  storage::Property property;
  storage::Label label2;
  storage::Property property2;
};

TEST(LabelPropertyIndex, CreateIndex) {
  GraphDb db;
  auto accessor = db.Access();
  LabelPropertyIndex::Key key(accessor.Label("test"),
                              accessor.Property("test2"));
  LabelPropertyIndex index;
  EXPECT_EQ(index.CreateIndex(key), true);
  EXPECT_EQ(index.CreateIndex(key), false);
}

TEST(LabelPropertyIndex, DeleteIndex) {
  GraphDb db;
  auto accessor = db.Access();
  LabelPropertyIndex::Key key(accessor.Label("test"),
                              accessor.Property("test2"));
  LabelPropertyIndex index;
  EXPECT_EQ(index.CreateIndex(key), true);
  EXPECT_EQ(index.CreateIndex(key), false);
  index.DeleteIndex(key);
  EXPECT_EQ(index.CreateIndex(key), true);
}

TEST(LabelPropertyIndex, IndexExistance) {
  GraphDb db;
  auto accessor = db.Access();
  LabelPropertyIndex::Key key(accessor.Label("test"),
                              accessor.Property("test2"));
  LabelPropertyIndex index;
  EXPECT_EQ(index.CreateIndex(key), true);
  // Index doesn't exist - and can't be used untill it's been notified as built.
  EXPECT_EQ(index.IndexExists(key), true);
}

TEST(LabelPropertyIndex, Count) {
  GraphDb db;
  auto accessor = db.Access();
  auto label = accessor.Label("label");
  auto property = accessor.Property("property");
  LabelPropertyIndex::Key key(label, property);
  LabelPropertyIndex index;
  EXPECT_EQ(index.CreateIndex(key), true);
  EXPECT_EQ(index.Count(key), 0);
}

// Add on label+property to index.
TEST_F(LabelPropertyIndexComplexTest, UpdateOnLabelPropertyTrue) {
  index.UpdateOnLabelProperty(vlist, vertex);
  EXPECT_EQ(index.Count(*key), 1);
}

// Try adding on label+property but fail because labels are clear.
TEST_F(LabelPropertyIndexComplexTest, UpdateOnLabelPropertyFalse) {
  vertex->labels_.clear();
  index.UpdateOnLabelProperty(vlist, vertex);
  EXPECT_EQ(index.Count(*key), 0);
}

// Add on label to index.
TEST_F(LabelPropertyIndexComplexTest, UpdateOnLabelTrue) {
  index.UpdateOnLabel(label, vlist, vertex);
  EXPECT_EQ(index.Count(*key), 1);
}

// Try adding on label but fail because label is wrong.
TEST_F(LabelPropertyIndexComplexTest, UpdateOnLabelFalse) {
  index.UpdateOnLabel(label2, vlist, vertex);
  EXPECT_EQ(index.Count(*key), 0);
}

// Add on property to index.
TEST_F(LabelPropertyIndexComplexTest, UpdateOnPropertyTrue) {
  index.UpdateOnProperty(property, vlist, vertex);
  EXPECT_EQ(index.Count(*key), 1);
}

// Try adding on property but fail because property is wrong.
TEST_F(LabelPropertyIndexComplexTest, UpdateOnPropertyFalse) {
  index.UpdateOnProperty(property2, vlist, vertex);
  EXPECT_EQ(index.Count(*key), 0);
}

// Test index does it insert everything uniquely
TEST_F(LabelPropertyIndexComplexTest, UniqueInsert) {
  index.UpdateOnLabelProperty(vlist, vertex);
  index.UpdateOnLabelProperty(vlist, vertex);
  EXPECT_EQ(index.Count(*key), 1);
}

// Check if index filters duplicates.
TEST_F(LabelPropertyIndexComplexTest, UniqueFilter) {
  index.UpdateOnLabelProperty(vlist, vertex);
  engine.Commit(*t);

  auto t2 = engine.Begin();
  auto vertex2 = vlist->update(*t2);
  engine.Commit(*t2);

  index.UpdateOnLabelProperty(vlist, vertex2);
  EXPECT_EQ(index.Count(*key), 2);

  auto t3 = engine.Begin();
  auto iter = index.GetVlists(*key, *t3, false);
  EXPECT_EQ(std::distance(iter.begin(), iter.end()), 1);
  engine.Commit(*t3);
}

// Remove label and check if index vertex is not returned now.
TEST_F(LabelPropertyIndexComplexTest, RemoveLabel) {
  index.UpdateOnLabelProperty(vlist, vertex);

  auto iter1 = index.GetVlists(*key, *t, false);
  EXPECT_EQ(std::distance(iter1.begin(), iter1.end()), 1);

  vertex->labels_.clear();
  auto iter2 = index.GetVlists(*key, *t, false);
  EXPECT_EQ(std::distance(iter2.begin(), iter2.end()), 0);
}

// Remove property and check if vertex is not returned now.
TEST_F(LabelPropertyIndexComplexTest, RemoveProperty) {
  index.UpdateOnLabelProperty(vlist, vertex);

  auto iter1 = index.GetVlists(*key, *t, false);
  EXPECT_EQ(std::distance(iter1.begin(), iter1.end()), 1);

  vertex->properties_.clear();
  auto iter2 = index.GetVlists(*key, *t, false);
  EXPECT_EQ(std::distance(iter2.begin(), iter2.end()), 0);
}

// Refresh with a vertex that looses its labels and properties.
TEST_F(LabelPropertyIndexComplexTest, Refresh) {
  index.UpdateOnLabelProperty(vlist, vertex);
  engine.Commit(*t);
  EXPECT_EQ(index.Count(*key), 1);
  vertex->labels_.clear();
  vertex->properties_.clear();
  index.Refresh(GcSnapshot(engine, nullptr), engine);
  auto iter = index.GetVlists(*key, *t, false);
  EXPECT_EQ(std::distance(iter.begin(), iter.end()), 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
