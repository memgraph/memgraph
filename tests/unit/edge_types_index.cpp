#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "data_structures/ptr_int.hpp"
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"

using testing::UnorderedElementsAreArray;

// Test counter of indexed edges with the given edge_type.
TEST(EdgeTypesIndex, Count) {
  Dbms dbms;
  auto accessor = dbms.active();
  const int ITERS = 50;
  size_t cnt = 0;
  for (int i = 0; i < ITERS; ++i) {
    auto vertex1 = accessor->insert_vertex();
    auto vertex2 = accessor->insert_vertex();
    if (rand() & 1) {
      accessor->insert_edge(vertex1, vertex2, accessor->edge_type("test"));
      ++cnt;
    } else {
      accessor->insert_edge(vertex1, vertex2, accessor->edge_type("test2"));
    }
    // Greater or equal since we said that we always estimate at least the
    // real number.
    EXPECT_GE(accessor->edges_count(accessor->edge_type("test")), cnt);
  }
}

// Transaction hasn't ended and so the edge is not visible.
TEST(EdgeTypesIndex, AddGetZeroEdgeTypes) {
  Dbms dbms;
  auto accessor = dbms.active();
  auto vertex1 = accessor->insert_vertex();
  auto vertex2 = accessor->insert_vertex();
  accessor->insert_edge(vertex1, vertex2, accessor->edge_type("test"));
  auto collection = accessor->edges(accessor->edge_type("test"));
  std::vector<EdgeAccessor> collection_vector(collection.begin(),
                                              collection.end());
  EXPECT_EQ(collection_vector.size(), (size_t)0);
}

// Test edge type index by adding and removing one edge, checking edge_type of
// another, while the third one with an irrelevant edge_type exists.
TEST(LabelsIndex, AddGetRemoveEdgeTypes) {
  Dbms dbms;
  {
    auto accessor = dbms.active();

    auto vertex11 = accessor->insert_vertex();
    auto vertex12 = accessor->insert_vertex();
    accessor->insert_edge(vertex11, vertex12, accessor->edge_type("test"));

    auto vertex21 = accessor->insert_vertex();
    auto vertex22 = accessor->insert_vertex();
    accessor->insert_edge(vertex21, vertex22, accessor->edge_type("test2"));

    auto vertex31 = accessor->insert_vertex();
    auto vertex32 = accessor->insert_vertex();
    accessor->insert_edge(vertex31, vertex32, accessor->edge_type("test"));

    accessor->commit();
  }  // Finish transaction.
  {
    auto accessor = dbms.active();

    auto filtered = accessor->edges(accessor->edge_type("test"));
    std::vector<EdgeAccessor> collection(filtered.begin(), filtered.end());
    auto edges = accessor->edges();

    std::vector<EdgeAccessor> expected_collection;
    for (auto edge : edges) {
      if (edge.edge_type() == accessor->edge_type("test")) {
        expected_collection.push_back(edge);
      } else {
        EXPECT_TRUE(edge.edge_type() == accessor->edge_type("test2"));
      }
    }

    EXPECT_EQ(expected_collection.size(), collection.size());
    EXPECT_TRUE(collection[0].edge_type() == accessor->edge_type("test"));
    EXPECT_TRUE(collection[1].edge_type() == accessor->edge_type("test"));
    EXPECT_FALSE(collection[0].edge_type() == accessor->edge_type("test2"));
    EXPECT_FALSE(collection[1].edge_type() == accessor->edge_type("test2"));
    accessor->remove_edge(collection[0]);  // Remove from database and test if
                                           // index won't return it.
    accessor->remove_edge(collection[1]);  // Remove from database and test if
                                           // index won't return it.

    accessor->commit();
  }
  {
    auto accessor = dbms.active();

    auto filtered = accessor->edges(accessor->edge_type("test"));
    std::vector<EdgeAccessor> collection(filtered.begin(), filtered.end());
    auto edges = accessor->edges();

    std::vector<EdgeAccessor> expected_collection;
    for (auto edge : edges) {
      if (edge.edge_type() == accessor->edge_type("test")) {
        expected_collection.push_back(edge);
      } else {
        EXPECT_TRUE(edge.edge_type() == accessor->edge_type("test2"));
      }
    }

    // It should be empty since everything with an old edge_type is either
    // deleted or doesn't have that edge_type anymore.
    EXPECT_EQ(expected_collection.size(), 0);
    EXPECT_EQ(collection.size(), 0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
