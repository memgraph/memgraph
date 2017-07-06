#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "dbms/dbms.hpp"
#include "storage/vertex.hpp"

#include "mvcc_gc_common.hpp"

using testing::UnorderedElementsAreArray;

// Test index does it insert everything uniquely
TEST(LabelsIndex, UniqueInsert) {
  KeyIndex<GraphDbTypes::Label, Vertex> index;
  Dbms dbms;
  auto dba = dbms.active();
  tx::Engine engine;
  auto t1 = engine.Begin();
  mvcc::VersionList<Vertex> vlist(*t1);
  t1->Commit();
  auto t2 = engine.Begin();

  vlist.find(*t2)->labels_.push_back(dba->label("1"));
  index.Update(dba->label("1"), &vlist, vlist.find(*t2));
  // Try multiple inserts
  index.Update(dba->label("1"), &vlist, vlist.find(*t2));

  vlist.find(*t2)->labels_.push_back(dba->label("2"));
  index.Update(dba->label("2"), &vlist, vlist.find(*t2));

  vlist.find(*t2)->labels_.push_back(dba->label("3"));
  index.Update(dba->label("3"), &vlist, vlist.find(*t2));
  t2->Commit();

  EXPECT_EQ(index.Count(dba->label("1")), 1);
  EXPECT_EQ(index.Count(dba->label("2")), 1);
  EXPECT_EQ(index.Count(dba->label("3")), 1);
}

// Check if index filters duplicates.
TEST(LabelsIndex, UniqueFilter) {
  Dbms dbms;
  KeyIndex<GraphDbTypes::Label, Vertex> index;
  auto dba = dbms.active();
  tx::Engine engine;

  auto t1 = engine.Begin();
  mvcc::VersionList<Vertex> vlist1(*t1);
  mvcc::VersionList<Vertex> vlist2(*t1);
  engine.Advance(t1->id_);
  auto r1v1 = vlist1.find(*t1);
  auto r1v2 = vlist2.find(*t1);
  EXPECT_NE(vlist1.find(*t1), nullptr);

  auto label1 = dba->label("1");
  vlist1.find(*t1)->labels_.push_back(label1);
  vlist2.find(*t1)->labels_.push_back(label1);
  index.Update(label1, &vlist1, r1v1);
  index.Update(label1, &vlist2, r1v2);
  t1->Commit();

  auto t2 = engine.Begin();
  auto r2v1 = vlist1.update(*t2);
  auto r2v2 = vlist2.update(*t2);
  index.Update(label1, &vlist1, r2v1);
  index.Update(label1, &vlist2, r2v2);
  t2->Commit();

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
  KeyIndex<GraphDbTypes::Label, Vertex> index;
  Dbms dbms;
  auto access = dbms.active();
  tx::Engine engine;

  // add two vertices to  database
  auto t1 = engine.Begin();
  mvcc::VersionList<Vertex> vlist1(*t1);
  mvcc::VersionList<Vertex> vlist2(*t1);
  engine.Advance(t1->id_);

  auto v1r1 = vlist1.find(*t1);
  auto v2r1 = vlist2.find(*t1);
  EXPECT_NE(v1r1, nullptr);
  EXPECT_NE(v2r1, nullptr);

  auto label = access->label("label");
  v1r1->labels_.push_back(label);
  v2r1->labels_.push_back(label);
  index.Update(label, &vlist1, v1r1);
  index.Update(label, &vlist2, v2r1);
  t1->Commit();

  auto t2 = engine.Begin();
  auto v1r2 = vlist1.update(*t2);
  auto v2r2 = vlist2.update(*t2);
  index.Update(label, &vlist1, v1r2);
  index.Update(label, &vlist2, v2r2);

  index.Refresh(GcSnapshot(engine, t2), engine);
  EXPECT_EQ(index.Count(label), 4);

  t2->Commit();
  EXPECT_EQ(index.Count(label), 4);
  index.Refresh(GcSnapshot(engine, nullptr), engine);
  EXPECT_EQ(index.Count(label), 2);
}

// Transaction hasn't ended and so the vertex is not visible.
TEST(LabelsIndexDb, AddGetZeroLabels) {
  Dbms dbms;
  auto dba = dbms.active();
  auto vertex = dba->insert_vertex();
  vertex.add_label(dba->label("test"));
  auto collection = dba->vertices(dba->label("test"), false);
  std::vector<VertexAccessor> collection_vector(collection.begin(),
                                                collection.end());
  EXPECT_EQ(collection_vector.size(), (size_t)0);
}

// Test label index by adding and removing one vertex, and removing label from
// another, while the third one with an irrelevant label exists.
TEST(LabelsIndexDb, AddGetRemoveLabel) {
  Dbms dbms;
  {
    auto dba = dbms.active();

    auto vertex1 = dba->insert_vertex();
    vertex1.add_label(dba->label("test"));

    auto vertex2 = dba->insert_vertex();
    vertex2.add_label(dba->label("test2"));

    auto vertex3 = dba->insert_vertex();
    vertex3.add_label(dba->label("test"));

    dba->commit();
  }  // Finish transaction.
  {
    auto dba = dbms.active();

    auto filtered = dba->vertices(dba->label("test"), false);
    std::vector<VertexAccessor> collection(filtered.begin(), filtered.end());
    auto vertices = dba->vertices(false);

    std::vector<VertexAccessor> expected_collection;
    for (auto vertex : vertices) {
      if (vertex.has_label(dba->label("test"))) {
        expected_collection.push_back(vertex);
      } else {
        EXPECT_TRUE(vertex.has_label(dba->label("test2")));
      }
    }

    EXPECT_EQ(expected_collection.size(), collection.size());
    EXPECT_TRUE(collection[0].has_label(dba->label("test")));
    EXPECT_TRUE(collection[1].has_label(dba->label("test")));
    EXPECT_FALSE(collection[0].has_label(dba->label("test2")));
    EXPECT_FALSE(collection[1].has_label(dba->label("test2")));
    dba->remove_vertex(collection[0]);  // Remove from database and test if
                                        // index won't return it.

    // Remove label from the vertex and add new label.
    collection[1].remove_label(dba->label("test"));
    collection[1].add_label(dba->label("test2"));
    dba->commit();
  }
  {
    auto dba = dbms.active();

    auto filtered = dba->vertices(dba->label("test"), false);
    std::vector<VertexAccessor> collection(filtered.begin(), filtered.end());
    auto vertices = dba->vertices(false);

    std::vector<VertexAccessor> expected_collection;
    for (auto vertex : vertices) {
      if (vertex.has_label(dba->label("test"))) {
        expected_collection.push_back(vertex);
      } else {
        EXPECT_TRUE(vertex.has_label(dba->label("test2")));
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
