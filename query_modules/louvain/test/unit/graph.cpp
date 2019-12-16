#include <glog/logging.h>
#include <gtest/gtest.h>

#include "data_structures/graph.hpp"
#include "utils.hpp"

// Checks if commmunities of nodes in graph correspond to a given community
// vector.
bool CommunityCheck(const comdata::Graph &graph,
                    const std::vector<uint32_t> &c) {
  if (graph.Size() != c.size()) return false;
  for (uint32_t node_id = 0; node_id < graph.Size(); ++node_id)
    if (graph.Community(node_id) != c[node_id])
      return false;
  return true;
}

// Checks if degrees of nodes in graph correspond to a given degree vector.
bool DegreeCheck(const comdata::Graph &graph,
                 const std::vector<uint32_t> &deg) {
  if (graph.Size() != deg.size()) return false;
  for (uint32_t node_id = 0; node_id < graph.Size(); ++node_id)
    if (graph.Degree(node_id) != deg[node_id])
      return false;
  return true;
}

// Checks if incident weights of nodes in graph correspond to a given weight
// vector.
bool IncidentWeightCheck(const comdata::Graph &graph,
                         const std::vector<double> &inc_w) {
  if (graph.Size() != inc_w.size()) return false;
  for (uint32_t node_id = 0; node_id < graph.Size(); ++node_id)
    if (std::abs(graph.IncidentWeight(node_id) - inc_w[node_id]) > 1e-6)
      return false;
  return true;
}

// Sets communities of nodes in graph. Returns true on success.
bool SetCommunities(comdata::Graph *graph, const std::vector<uint32_t> &c) {
  if (graph->Size() != c.size()) return false;
  for (uint32_t node_id = 0; node_id < graph->Size(); ++node_id)
    graph->SetCommunity(node_id, c[node_id]);
  return true;
}

TEST(Graph, Constructor) {
  uint32_t nodes = 100;
  comdata::Graph graph(nodes);
  ASSERT_EQ(graph.Size(), nodes);
  for (uint32_t node_id = 0; node_id < nodes; ++node_id) {
    ASSERT_EQ(graph.IncidentWeight(node_id), 0);
    ASSERT_EQ(graph.Community(node_id), node_id);
  }
}

TEST(Graph, Size) {
  comdata::Graph graph1 = GenRandomUnweightedGraph(0, 0);
  comdata::Graph graph2 = GenRandomUnweightedGraph(42, 41);
  comdata::Graph graph3 = GenRandomUnweightedGraph(100, 250);
  ASSERT_EQ(graph1.Size(), 0);
  ASSERT_EQ(graph2.Size(), 42);
  ASSERT_EQ(graph3.Size(), 100);
}

TEST(Graph, Communities) {
  comdata::Graph graph = GenRandomUnweightedGraph(100, 250);

  for (int i = 0; i < 100; ++i) graph.SetCommunity(i, i % 5);
  for (int i = 0; i < 100; ++i) ASSERT_EQ(graph.Community(i), i % 5);

  // Try to set communities on non-existing nodes
  EXPECT_THROW({ graph.SetCommunity(100, 2); }, std::out_of_range);
  EXPECT_THROW({ graph.SetCommunity(150, 0); }, std::out_of_range);

  // Try to get a the community of a non-existing node
  EXPECT_THROW({ graph.Community(100); }, std::out_of_range);
  EXPECT_THROW({ graph.Community(150); }, std::out_of_range);
}

TEST(Graph, CommunityNormalization) {
  // Communities are already normalized.
  comdata::Graph graph = GenRandomUnweightedGraph(5, 10);
  std::vector<uint32_t> init_c = {0, 2, 1, 3, 4};
  std::vector<uint32_t> final_c = {0, 2, 1, 3, 4};
  ASSERT_TRUE(SetCommunities(&graph, init_c));
  graph.NormalizeCommunities();
  ASSERT_TRUE(CommunityCheck(graph, final_c));

  // Each node in its own community.
  graph = GenRandomUnweightedGraph(5, 10);
  init_c = {20, 30, 10, 40, 50};
  final_c = {1, 2, 0, 3, 4};
  ASSERT_TRUE(SetCommunities(&graph, init_c));
  graph.NormalizeCommunities();
  ASSERT_TRUE(CommunityCheck(graph, final_c));

  // Multiple nodes in the same community
  graph = GenRandomUnweightedGraph(7, 10);
  init_c = {13, 99, 13, 13, 1, 99, 1};
  final_c = {1, 2, 1, 1, 0, 2, 0};
  ASSERT_TRUE(SetCommunities(&graph, init_c));
  graph.NormalizeCommunities();
  ASSERT_TRUE(CommunityCheck(graph, final_c));
}

TEST(Graph, AddEdge) {
  comdata::Graph graph = GenRandomUnweightedGraph(5, 0);

  // Node out of bounds.
  EXPECT_THROW({ graph.AddEdge(1, 5, 7); }, std::out_of_range);

  // Repeated edge
  graph.AddEdge(1, 2, 1);
  EXPECT_THROW({ graph.AddEdge(1, 2, 7); }, std::invalid_argument);

  // Non-positive edge weight
  EXPECT_THROW({ graph.AddEdge(2, 3, -7); }, std::out_of_range);
  EXPECT_THROW({ graph.AddEdge(3, 4, 0); }, std::out_of_range);
}

TEST(Graph, Degrees) {
  // Graph without edges
  comdata::Graph graph = GenRandomUnweightedGraph(5, 0);
  std::vector<uint32_t> deg = {0, 0, 0, 0, 0};
  ASSERT_TRUE(DegreeCheck(graph, deg));

  // Chain
  // (0)--(1)--(2)--(3)--(4)
  graph = BuildGraph(5, {{0, 1, 1},
                     {1, 2, 1},
                     {2, 3, 1},
                     {3, 4, 1}});
  deg = {1, 2, 2, 2, 1};
  ASSERT_TRUE(DegreeCheck(graph, deg));

  // Tree
  //      (0)--(3)
  //     /   \
  //   (1)   (2)
  //    |   /   \
  //   (4) (5)  (6)
  graph = BuildGraph(7, {{0, 1, 1},
                     {0, 2, 1},
                     {0, 3, 1},
                     {1, 4, 1},
                     {2, 5, 1},
                     {2, 6, 1}});
  deg = {3, 2, 3, 1, 1, 1, 1};
  ASSERT_TRUE(DegreeCheck(graph, deg));

  // Graph without self-loops
  // (0)--(1)
  //  | \  | \
  //  |  \ |  \
  // (2)--(3)-(4)
  graph = BuildGraph(5, {{0, 1, 1},
                     {0, 2, 1},
                     {0, 3, 1},
                     {1, 3, 1},
                     {1, 4, 1},
                     {2, 3, 1},
                     {3, 4, 1}});
  deg = {3, 3, 2, 4, 2};
  ASSERT_TRUE(DegreeCheck(graph, deg));

  // Graph with self loop [*nodes have self loops]
  // (0)--(1*)
  //  | \  | \
  //  |  \ |  \
  // (2*)--(3)-(4*)
  graph = BuildGraph(5, {{0, 1, 1},
                     {0, 2, 1},
                     {0, 3, 1},
                     {1, 3, 1},
                     {1, 4, 1},
                     {2, 3, 1},
                     {3, 4, 1},
                     {1, 1, 1},
                     {2, 2, 2},
                     {4, 4, 4}});
  deg = {3, 4, 3, 4, 3};
  ASSERT_TRUE(DegreeCheck(graph, deg));

  // Try to get degree of non-existing nodes
  EXPECT_THROW({ graph.Degree(5); }, std::out_of_range);
  EXPECT_THROW({ graph.Degree(100); }, std::out_of_range);
}

TEST(Graph, Weights) {
  // Graph without edges
  comdata::Graph graph = GenRandomUnweightedGraph(5, 0);
  std::vector<double> inc_w = {0, 0, 0, 0, 0};
  ASSERT_TRUE(IncidentWeightCheck(graph, inc_w));
  ASSERT_EQ(graph.TotalWeight(), 0);

  // Chain
  // (0)--(1)--(2)--(3)--(4)
  graph = BuildGraph(5, {{0, 1, 0.1},
                     {1, 2, 0.5},
                     {2, 3, 2.3},
                     {3, 4, 4.2}});
  inc_w = {0.1, 0.6, 2.8, 6.5, 4.2};
  ASSERT_TRUE(IncidentWeightCheck(graph, inc_w));
  ASSERT_NEAR(graph.TotalWeight(), 7.1, 1e-6);

  // Tree
  //      (0)--(3)
  //     /   \
  //   (1)   (2)
  //    |   /   \
  //   (4) (5)  (6)
  graph = BuildGraph(7, {{0, 1, 1.3},
                     {0, 2, 0.2},
                     {0, 3, 1},
                     {1, 4, 3.2},
                     {2, 5, 4.2},
                     {2, 6, 0.7}});
  inc_w = {2.5, 4.5, 5.1, 1, 3.2, 4.2, 0.7};
  ASSERT_TRUE(IncidentWeightCheck(graph, inc_w));
  EXPECT_NEAR(graph.TotalWeight(), 10.6, 1e-6);

  // Graph without self-loops
  // (0)--(1)
  //  | \  | \
  //  |  \ |  \
  // (2)--(3)-(4)
  graph = BuildGraph(5, {{0, 1, 0.1},
                     {0, 2, 0.2},
                     {0, 3, 0.3},
                     {1, 3, 0.4},
                     {1, 4, 0.5},
                     {2, 3, 0.6},
                     {3, 4, 0.7}});
  inc_w = {0.6, 1, 0.8, 2, 1.2};
  ASSERT_TRUE(IncidentWeightCheck(graph, inc_w));
  EXPECT_NEAR(graph.TotalWeight(), 2.8, 1e-6);

  // Graph with self loop [*nodes have self loops]
  // (0)--(1*)
  //  | \  | \
  //  |  \ |  \
  // (2*)--(3)-(4*)
  graph = BuildGraph(5, {{0, 1, 0.1},
                     {0, 2, 0.2},
                     {0, 3, 0.3},
                     {1, 3, 0.4},
                     {1, 4, 0.5},
                     {2, 3, 0.6},
                     {3, 4, 0.7},
                     {1, 1, 0.8},
                     {2, 2, 0.9},
                     {4, 4, 1}});
  inc_w = {0.6, 1.8, 1.7, 2, 2.2};
  ASSERT_TRUE(IncidentWeightCheck(graph, inc_w));
  EXPECT_NEAR(graph.TotalWeight(), 5.5, 1e-6);

  // Try to get incident weight of non-existing node
  EXPECT_THROW({ graph.IncidentWeight(5); }, std::out_of_range);
  EXPECT_THROW({ graph.IncidentWeight(100); }, std::out_of_range);
}

TEST(Graph, Modularity) {
  // Graph without edges
  comdata::Graph graph = GenRandomUnweightedGraph(5, 0);
  ASSERT_EQ(graph.Modularity(), 0);

  // Chain
  // (0)--(1)--(2)--(3)--(4)
  graph = BuildGraph(5, {{0, 1, 0.1},
                     {1, 2, 0.5},
                     {2, 3, 2.3},
                     {3, 4, 4.2}});
  std::vector<uint32_t> c = {0, 1, 1, 2, 2};
  SetCommunities(&graph, c);
  EXPECT_NEAR(graph.Modularity(), 0.036798254314620096, 1e-6);

  // Tree
  //      (0)--(3)
  //     /   \
  //   (1)   (2)
  //    |   /   \
  //   (4) (5)  (6)
  graph = BuildGraph(7, {{0, 1, 1.3},
                     {0, 2, 0.2},
                     {0, 3, 1},
                     {1, 4, 3.2},
                     {2, 5, 4.2},
                     {2, 6, 0.7}});
  c = {0, 0, 1, 0, 0, 1, 2};
  SetCommunities(&graph, c);
  EXPECT_NEAR(graph.Modularity(), 0.4424617301530794, 1e-6);

  // Graph without self-loops
  // (0)--(1)
  //  | \  | \
  //  |  \ |  \
  // (2)--(3)-(4)
  graph = BuildGraph(5, {{0, 1, 0.1},
                     {0, 2, 0.2},
                     {0, 3, 0.3},
                     {1, 3, 0.4},
                     {1, 4, 0.5},
                     {2, 3, 0.6},
                     {3, 4, 0.7}});
  c = {0, 1, 1, 1, 1};
  SetCommunities(&graph, c);
  EXPECT_NEAR(graph.Modularity(), -0.022959183673469507, 1e-6);

  // Graph with self loop [*nodes have self loops]
  // (0)--(1*)
  //  | \  | \
  //  |  \ |  \
  // (2*)--(3)-(4*)
  graph = BuildGraph(5, {{0, 1, 0.1},
                     {0, 2, 0.2},
                     {0, 3, 0.3},
                     {1, 3, 0.4},
                     {1, 4, 0.5},
                     {2, 3, 0.6},
                     {3, 4, 0.7},
                     {1, 1, 0.8},
                     {2, 2, 0.9},
                     {4, 4, 1}});
  c = {0, 0, 0, 0, 1};
  SetCommunities(&graph, c);
  EXPECT_NEAR(graph.Modularity(), 0.188842975206611, 1e-6);

  // Neo4j example graph
  // (0)--(1)---(3)--(4)
  //   \  /       \ /
  //    (2)       (5)
  graph = BuildGraph(6, {{0, 1, 1},
                         {1, 2, 1},
                         {0, 2, 1},
                         {1, 3, 1},
                         {3, 5, 1},
                         {5, 4, 1},
                         {3, 4, 1}});
  c = {0, 0, 0, 1, 1, 1};
  SetCommunities(&graph, c);
  EXPECT_NEAR(graph.Modularity(), 0.3571428571428571, 1e-6);

  // Example graph from wikipedia
  // (0)--(1)--(3)--(4)--(5)
  //   \  /     |      \ /
  //    (2)    (7)     (6)
  //          /   \
  //         (8)--(9)
  graph = BuildGraph(10, {{0, 1, 1},
                          {1, 2, 1},
                          {0, 2, 1},
                          {1, 3, 1},
                          {3, 4, 1},
                          {4, 5, 1},
                          {5, 6, 1},
                          {6, 4, 1},
                          {3, 7, 1},
                          {7, 8, 1},
                          {7, 9, 1},
                          {8, 9, 1}});
  c = {0, 0, 0, 0, 1, 1, 1, 2, 2, 2};
  SetCommunities(&graph, c);
  EXPECT_NEAR(graph.Modularity(), 0.4896, 1e-4);
}
