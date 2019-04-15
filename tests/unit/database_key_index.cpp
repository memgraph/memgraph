#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node/vertex.hpp"
#include "transactions/single_node/engine.hpp"

#include "mvcc_gc_common.hpp"

using testing::UnorderedElementsAreArray;

// Test index does it insert everything uniquely
TEST(LabelsIndex, UniqueInsert) {
  database::KeyIndex<storage::Label, Vertex> index;
  database::GraphDb db;
  auto dba = db.Access();
  tx::Engine engine;

  auto t1 = engine.Begin();
  mvcc::VersionList<Vertex> vlist(*t1, 0);
  engine.Commit(*t1);
  auto t2 = engine.Begin();

  vlist.find(*t2)->labels_.push_back(dba.Label("1"));
  index.Update(dba.Label("1"), &vlist, vlist.find(*t2));
  // Try multiple inserts
  index.Update(dba.Label("1"), &vlist, vlist.find(*t2));

  vlist.find(*t2)->labels_.push_back(dba.Label("2"));
  index.Update(dba.Label("2"), &vlist, vlist.find(*t2));

  vlist.find(*t2)->labels_.push_back(dba.Label("3"));
  index.Update(dba.Label("3"), &vlist, vlist.find(*t2));
  engine.Commit(*t2);

  EXPECT_EQ(index.Count(dba.Label("1")), 1);
  EXPECT_EQ(index.Count(dba.Label("2")), 1);
  EXPECT_EQ(index.Count(dba.Label("3")), 1);
}

// Check if index filters duplicates.
TEST(LabelsIndex, UniqueFilter) {
  database::GraphDb db;
  database::KeyIndex<storage::Label, Vertex> index;
  auto dba = db.Access();
  tx::Engine engine;

  auto t1 = engine.Begin();
  mvcc::VersionList<Vertex> vlist1(*t1, 0);
  mvcc::VersionList<Vertex> vlist2(*t1, 1);
  engine.Advance(t1->id_);
  auto r1v1 = vlist1.find(*t1);
  auto r1v2 = vlist2.find(*t1);
  EXPECT_NE(vlist1.find(*t1), nullptr);

  auto label1 = dba.Label("1");
  vlist1.find(*t1)->labels_.push_back(label1);
  vlist2.find(*t1)->labels_.push_back(label1);
  index.Update(label1, &vlist1, r1v1);
  index.Update(label1, &vlist2, r1v2);
  engine.Commit(*t1);

  auto t2 = engine.Begin();
  auto r2v1 = vlist1.update(*t2);
  auto r2v2 = vlist2.update(*t2);
  index.Update(label1, &vlist1, r2v1);
  index.Update(label1, &vlist2, r2v2);
  engine.Commit(*t2);

  auto t3 = engine.Begin();
  std::vector<mvcc::VersionList<Vertex> *> expected = {&vlist1, &vlist2};
  sort(expected.begin(),
       expected.end());  // Entries will be sorted by vlist pointers.
  int cnt = 0;
  for (auto vlist : index.GetVlists(label1, *t3, false)) {
    EXPECT_LT(cnt, expected.size());
    EXPECT_EQ(vlist, expected[cnt++]);
  }
}

// Delete not anymore relevant recods from index.
TEST(LabelsIndex, Refresh) {
  database::KeyIndex<storage::Label, Vertex> index;
  database::GraphDb db;
  auto access = db.Access();
  tx::Engine engine;

  // add two vertices to  database
  auto t1 = engine.Begin();
  mvcc::VersionList<Vertex> vlist1(*t1, 0);
  mvcc::VersionList<Vertex> vlist2(*t1, 1);
  engine.Advance(t1->id_);

  auto v1r1 = vlist1.find(*t1);
  auto v2r1 = vlist2.find(*t1);
  EXPECT_NE(v1r1, nullptr);
  EXPECT_NE(v2r1, nullptr);

  auto label = access.Label("label");
  v1r1->labels_.push_back(label);
  v2r1->labels_.push_back(label);
  index.Update(label, &vlist1, v1r1);
  index.Update(label, &vlist2, v2r1);
  engine.Commit(*t1);

  auto t2 = engine.Begin();
  auto v1r2 = vlist1.update(*t2);
  auto v2r2 = vlist2.update(*t2);
  index.Update(label, &vlist1, v1r2);
  index.Update(label, &vlist2, v2r2);

  index.Refresh(GcSnapshot(engine, t2), engine);
  EXPECT_EQ(index.Count(label), 4);

  engine.Commit(*t2);
  EXPECT_EQ(index.Count(label), 4);
  index.Refresh(GcSnapshot(engine, nullptr), engine);
  EXPECT_EQ(index.Count(label), 2);
}

// Transaction hasn't ended and so the vertex is not visible.
TEST(LabelsIndexDb, AddGetZeroLabels) {
  database::GraphDb db;
  auto dba = db.Access();
  auto vertex = dba.InsertVertex();
  vertex.add_label(dba.Label("test"));
  auto collection = dba.Vertices(dba.Label("test"), false);
  std::vector<VertexAccessor> collection_vector(collection.begin(),
                                                collection.end());
  EXPECT_EQ(collection_vector.size(), (size_t)0);
}

// Test label index by adding and removing one vertex, and removing label from
// another, while the third one with an irrelevant label exists.
TEST(LabelsIndexDb, AddGetRemoveLabel) {
  database::GraphDb db;
  {
    auto dba = db.Access();

    auto vertex1 = dba.InsertVertex();
    vertex1.add_label(dba.Label("test"));

    auto vertex2 = dba.InsertVertex();
    vertex2.add_label(dba.Label("test2"));

    auto vertex3 = dba.InsertVertex();
    vertex3.add_label(dba.Label("test"));

    dba.Commit();
  }  // Finish transaction.
  {
    auto dba = db.Access();

    auto filtered = dba.Vertices(dba.Label("test"), false);
    std::vector<VertexAccessor> collection(filtered.begin(), filtered.end());
    auto vertices = dba.Vertices(false);

    std::vector<VertexAccessor> expected_collection;
    for (auto vertex : vertices) {
      if (vertex.has_label(dba.Label("test"))) {
        expected_collection.push_back(vertex);
      } else {
        EXPECT_TRUE(vertex.has_label(dba.Label("test2")));
      }
    }

    EXPECT_EQ(expected_collection.size(), collection.size());
    EXPECT_TRUE(collection[0].has_label(dba.Label("test")));
    EXPECT_TRUE(collection[1].has_label(dba.Label("test")));
    EXPECT_FALSE(collection[0].has_label(dba.Label("test2")));
    EXPECT_FALSE(collection[1].has_label(dba.Label("test2")));
    dba.RemoveVertex(collection[0]);  // Remove from database and test if
                                       // index won't return it.

    // Remove label from the vertex and add new label.
    collection[1].remove_label(dba.Label("test"));
    collection[1].add_label(dba.Label("test2"));
    dba.Commit();
  }
  {
    auto dba = db.Access();

    auto filtered = dba.Vertices(dba.Label("test"), false);
    std::vector<VertexAccessor> collection(filtered.begin(), filtered.end());
    auto vertices = dba.Vertices(false);

    std::vector<VertexAccessor> expected_collection;
    for (auto vertex : vertices) {
      if (vertex.has_label(dba.Label("test"))) {
        expected_collection.push_back(vertex);
      } else {
        EXPECT_TRUE(vertex.has_label(dba.Label("test2")));
      }
    }

    // It should be empty since everything with an old label is either deleted
    // or doesn't have that label anymore.
    EXPECT_EQ(expected_collection.size(), 0);
    EXPECT_EQ(collection.size(), 0);
  }
}

// TODO gleich - discuss with Flor the API changes and the tests

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
