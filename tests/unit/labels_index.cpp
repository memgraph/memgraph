#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "data_structures/ptr_int.hpp"
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"

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
    EXPECT_GE(accessor->vertices_by_label_count(accessor->label("test")), cnt);
  }
}

// Transaction hasn't ended and so the vertex is not visible.
TEST(LabelsIndex, AddGetZeroLabels) {
  Dbms dbms;
  auto accessor = dbms.active();
  auto vertex = accessor->insert_vertex();
  vertex.add_label(accessor->label("test"));
  accessor->commit();
  auto collection = accessor->vertices_by_label(accessor->label("test"));
  std::vector<VertexAccessor> collection_vector;
  EXPECT_EQ(collection_vector.size(), (size_t)0);
}

// Test label index by adding and removing one vertex, and removing label from
// another, while the third one with an irrelevant label exists.
TEST(LabelsIndex, AddGetRemoveLabel) {
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

    auto filtered = accessor->vertices_by_label(accessor->label("test"));
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

    auto filtered = accessor->vertices_by_label(accessor->label("test"));
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
