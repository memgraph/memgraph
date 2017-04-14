#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "data_structures/ptr_int.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "dbms/dbms.hpp"
#include "storage/vertex.hpp"

using testing::UnorderedElementsAreArray;

// Test counter of indexed vertices with the given label.
TEST(LabelsIndex, Count) {
  Dbms dbms;
  auto accessor = dbms.active();
  const int ITERS = 50;
  size_t cnt = 0;
  for (int i = 0; i < ITERS; ++i) {
    auto vertex = accessor->insert_vertex();
    if (rand() & 1) {
      vertex.add_label(accessor->label("test"));
      ++cnt;
    } else {
      vertex.add_label(accessor->label("test2"));
    }
    // Greater or equal since we said that we always estimate at least the
    // real number.
    EXPECT_GE(accessor->vertices_count(accessor->label("test")), cnt);
  }
}

// Test index does it insert everything uniquely
TEST(LabelsIndex, UniqueInsert) {
  KeyIndex<GraphDbTypes::Label, Vertex> index;
  Dbms dbms;
  auto access = dbms.active();
  tx::Engine engine;
  auto t1 = engine.begin();
  mvcc::VersionList<Vertex> vlist(*t1);
  t1->commit();
  auto t2 = engine.begin();

  vlist.find(*t2)->labels_.push_back(access->label("1"));
  index.Update(access->label("1"), &vlist, vlist.find(*t2));
  // Try multiple inserts
  index.Update(access->label("1"), &vlist, vlist.find(*t2));

  vlist.find(*t2)->labels_.push_back(access->label("2"));
  index.Update(access->label("2"), &vlist, vlist.find(*t2));

  vlist.find(*t2)->labels_.push_back(access->label("3"));
  index.Update(access->label("3"), &vlist, vlist.find(*t2));
  t2->commit();

  EXPECT_EQ(index.Count(access->label("1")), 1);
  EXPECT_EQ(index.Count(access->label("2")), 1);
  EXPECT_EQ(index.Count(access->label("3")), 1);
}

// Check if index filters duplicates.
TEST(LabelsIndex, UniqueFilter) {
  KeyIndex<GraphDbTypes::Label, Vertex> index;
  Dbms dbms;
  auto access = dbms.active();
  tx::Engine engine;

  auto t1 = engine.begin();
  mvcc::VersionList<Vertex> vlist1(*t1);
  mvcc::VersionList<Vertex> vlist2(*t1);
  t1->engine.advance(
      t1->id);  // advance command so we can see our inserted version
  auto r1v1 = vlist1.find(*t1);
  auto r1v2 = vlist1.find(*t1);
  EXPECT_NE(vlist1.find(*t1), nullptr);

  auto label1 = access->label("1");
  vlist1.find(*t1)->labels_.push_back(label1);
  vlist2.find(*t1)->labels_.push_back(label1);
  index.Update(label1, &vlist1, r1v1);
  index.Update(label1, &vlist2, r1v2);
  t1->commit();

  auto t2 = engine.begin();
  auto r2v1 = vlist1.update(*t2);
  auto r2v2 = vlist2.update(*t2);
  index.Update(label1, &vlist1, r2v1);
  index.Update(label1, &vlist2, r2v2);
  t2->commit();

  auto t3 = engine.begin();
  std::vector<mvcc::VersionList<Vertex> *> expected = {&vlist1, &vlist2};
  sort(expected.begin(),
       expected.end());  // Entries will be sorted by vlist pointers.
  int cnt = 0;
  for (auto vlist : index.GetVlists(label1, *t3)) {
    EXPECT_LT(cnt, expected.size());
    EXPECT_EQ(vlist, expected[cnt++]);
  }
}

// Delete not anymore relevant recods from index.
TEST(LabelsIndex, Refresh) {
  KeyIndex<GraphDbTypes::Label, Vertex> index;
  Dbms dbms;
  auto access = dbms.active();
  tx::Engine engine;

  auto t1 = engine.begin();
  mvcc::VersionList<Vertex> vlist1(*t1);
  mvcc::VersionList<Vertex> vlist2(*t1);
  t1->engine.advance(
      t1->id);  // advance command so we can see our inserted version
  auto r1v1 = vlist1.find(*t1);
  auto r1v2 = vlist2.find(*t1);
  EXPECT_NE(vlist1.find(*t1), nullptr);

  auto label1 = access->label("1");
  vlist1.find(*t1)->labels_.push_back(label1);
  vlist2.find(*t1)->labels_.push_back(label1);
  index.Update(label1, &vlist1, r1v1);
  index.Update(label1, &vlist2, r1v2);
  t1->commit();

  auto t2 = engine.begin();
  auto r2v1 = vlist1.update(*t2);
  auto r2v2 = vlist2.update(*t2);
  index.Update(label1, &vlist1, r2v1);
  index.Update(label1, &vlist2, r2v2);
  int last_id = t2->id;
  t2->commit();
  EXPECT_EQ(index.Count(label1), 4);

  index.Refresh(last_id, engine);
  EXPECT_EQ(index.Count(label1), 4);

  index.Refresh(last_id + 1, engine);
  EXPECT_EQ(index.Count(label1), 2);
}

// Transaction hasn't ended and so the vertex is not visible.
TEST(LabelsIndexDb, AddGetZeroLabels) {
  Dbms dbms;
  auto accessor = dbms.active();
  auto vertex = accessor->insert_vertex();
  vertex.add_label(accessor->label("test"));
  auto collection = accessor->vertices(accessor->label("test"));
  std::vector<VertexAccessor> collection_vector(collection.begin(),
                                                collection.end());
  EXPECT_EQ(collection_vector.size(), (size_t)0);
}

// Test label index by adding and removing one vertex, and removing label from
// another, while the third one with an irrelevant label exists.
TEST(LabelsIndexDb, AddGetRemoveLabel) {
  Dbms dbms;
  {
    auto accessor = dbms.active();

    auto vertex1 = accessor->insert_vertex();
    vertex1.add_label(accessor->label("test"));

    auto vertex2 = accessor->insert_vertex();
    vertex2.add_label(accessor->label("test2"));

    auto vertex3 = accessor->insert_vertex();
    vertex3.add_label(accessor->label("test"));

    accessor->commit();
  }  // Finish transaction.
  {
    auto accessor = dbms.active();

    auto filtered = accessor->vertices(accessor->label("test"));
    std::vector<VertexAccessor> collection(filtered.begin(), filtered.end());
    auto vertices = accessor->vertices();

    std::vector<VertexAccessor> expected_collection;
    for (auto vertex : vertices) {
      if (vertex.has_label(accessor->label("test"))) {
        expected_collection.push_back(vertex);
      } else {
        EXPECT_TRUE(vertex.has_label(accessor->label("test2")));
      }
    }

    EXPECT_EQ(expected_collection.size(), collection.size());
    EXPECT_TRUE(collection[0].has_label(accessor->label("test")));
    EXPECT_TRUE(collection[1].has_label(accessor->label("test")));
    EXPECT_FALSE(collection[0].has_label(accessor->label("test2")));
    EXPECT_FALSE(collection[1].has_label(accessor->label("test2")));
    accessor->remove_vertex(collection[0]);  // Remove from database and test if
                                             // index won't return it.

    // Remove label from the vertex and add new label.
    collection[1].remove_label(accessor->label("test"));
    collection[1].add_label(accessor->label("test2"));
    accessor->commit();
  }
  {
    auto accessor = dbms.active();

    auto filtered = accessor->vertices(accessor->label("test"));
    std::vector<VertexAccessor> collection(filtered.begin(), filtered.end());
    auto vertices = accessor->vertices();

    std::vector<VertexAccessor> expected_collection;
    for (auto vertex : vertices) {
      if (vertex.has_label(accessor->label("test"))) {
        expected_collection.push_back(vertex);
      } else {
        EXPECT_TRUE(vertex.has_label(accessor->label("test2")));
      }
    }

    // It should be empty since everything with an old label is either deleted
    // or doesn't have that label anymore.
    EXPECT_EQ(expected_collection.size(), 0);
    EXPECT_EQ(collection.size(), 0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
