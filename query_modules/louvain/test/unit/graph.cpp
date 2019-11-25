#include <glog/logging.h>
#include <gtest/gtest.h>

#include "data_structures/graph.hpp"
#include "utils.hpp"

// Checks if commmunities of nodes in G correspond to a given community vector.
bool community_check(const comdata::Graph &G, const std::vector<uint32_t> &c) {
  if (G.Size() != c.size()) return false;
  for (uint32_t node_id = 0; node_id < G.Size(); ++node_id)
    if (G.Community(node_id) != c[node_id])
      return false;
  return true;
}

// Checks if degrees of nodes in G correspond to a given degree vector.
bool degree_check(const comdata::Graph &G, const std::vector<uint32_t> &deg) {
  if (G.Size() != deg.size()) return false;
  for (uint32_t node_id = 0; node_id < G.Size(); ++node_id)
    if (G.Degree(node_id) != deg[node_id])
      return false;
  return true;
}

// Checks if incident weights of nodes in G correspond to a given weight vector.
bool inc_w_check(const comdata::Graph &G, const std::vector<double> &inc_w) {
  if (G.Size() != inc_w.size()) return false;
  for (uint32_t node_id = 0; node_id < G.Size(); ++node_id)
    if (std::abs(G.IncidentWeight(node_id) - inc_w[node_id]) > 1e-6)
      return false;
  return true;
}

// Sets communities of nodes in G. Returns true on success.
bool set_communities(comdata::Graph *G, const std::vector<uint32_t> &c) {
  if (G->Size() != c.size()) return false;
  for (uint32_t node_id = 0; node_id < G->Size(); ++node_id)
    G->SetCommunity(node_id, c[node_id]);
  return true;
}

TEST(Graph, Constructor) {
  uint32_t nodes = 100;
  comdata::Graph G(nodes);
  ASSERT_EQ(G.Size(), nodes);
  for (uint32_t node_id = 0; node_id < nodes; ++node_id) {
    ASSERT_EQ(G.IncidentWeight(node_id), 0);
    ASSERT_EQ(G.Community(node_id), node_id);
  }
}

TEST(Graph, Size) {
  comdata::Graph G1 = GenRandomUnweightedGraph(0, 0);
  comdata::Graph G2 = GenRandomUnweightedGraph(42, 41);
  comdata::Graph G3 = GenRandomUnweightedGraph(100, 250);
  ASSERT_EQ(G1.Size(), 0);
  ASSERT_EQ(G2.Size(), 42);
  ASSERT_EQ(G3.Size(), 100);
}

TEST(Graph, Communities) {
  comdata::Graph G = GenRandomUnweightedGraph(100, 250);

  for (int i = 0; i < 100; ++i) G.SetCommunity(i, i % 5);
  for (int i = 0; i < 100; ++i) ASSERT_EQ(G.Community(i), i % 5);

  // Try to set communities on non-existing nodes
  ASSERT_DEATH({ G.SetCommunity(100, 2); }, "");
  ASSERT_DEATH({ G.SetCommunity(150, 0); }, "");

  // Try to get a the community of a non-existing node
  ASSERT_DEATH({ G.Community(100); }, "");
  ASSERT_DEATH({ G.Community(150); }, "");
}

TEST(Graph, CommunityNormalization) {
  // Communities are already normalized.
  comdata::Graph G = GenRandomUnweightedGraph(5, 10);
  std::vector<uint32_t> init_c = {0, 2, 1, 3, 4};
  std::vector<uint32_t> final_c = {0, 2, 1, 3, 4};
  ASSERT_TRUE(set_communities(&G, init_c));
  G.NormalizeCommunities();
  ASSERT_TRUE(community_check(G, final_c));

  // Each node in its own community.
  G = GenRandomUnweightedGraph(5, 10);
  init_c = {20, 30, 10, 40, 50};
  final_c = {1, 2, 0, 3, 4};
  ASSERT_TRUE(set_communities(&G, init_c));
  G.NormalizeCommunities();
  ASSERT_TRUE(community_check(G, final_c));

  // Multiple nodes in the same community
  G = GenRandomUnweightedGraph(7, 10);
  init_c = {13, 99, 13, 13, 1, 99, 1};
  final_c = {1, 2, 1, 1, 0, 2, 0};
  ASSERT_TRUE(set_communities(&G, init_c));
  G.NormalizeCommunities();
  ASSERT_TRUE(community_check(G, final_c));
}

TEST(Graph, AddEdge) {
  comdata::Graph G = GenRandomUnweightedGraph(5, 0);

  // Node out of bounds.
  ASSERT_DEATH({ G.AddEdge(1, 5, 7); }, "");

  // Repeated edge
  G.AddEdge(1, 2, 1);
  ASSERT_DEATH({ G.AddEdge(1, 2, 7); }, "");

  // Non-positive edge weight
  ASSERT_DEATH({ G.AddEdge(2, 3, -7); }, "");
  ASSERT_DEATH({ G.AddEdge(3, 4, 0); }, "");
}

TEST(Graph, Degrees) {
  // Graph without edges
  comdata::Graph G = GenRandomUnweightedGraph(5, 0);
  std::vector<uint32_t> deg = {0, 0, 0, 0, 0};
  ASSERT_TRUE(degree_check(G, deg));

  // Chain
  // (0)--(1)--(2)--(3)--(4)
  G = BuildGraph(5, {{0, 1, 1},
                     {1, 2, 1},
                     {2, 3, 1},
                     {3, 4, 1}});
  deg = {1, 2, 2, 2, 1};
  ASSERT_TRUE(degree_check(G, deg));

  // Tree
  //      (0)--(3)
  //     /   \
  //   (1)   (2)
  //    |   /   \
  //   (4) (5)  (6)
  G = BuildGraph(7, {{0, 1, 1},
                     {0, 2, 1},
                     {0, 3, 1},
                     {1, 4, 1},
                     {2, 5, 1},
                     {2, 6, 1}});
  deg = {3, 2, 3, 1, 1, 1, 1};
  ASSERT_TRUE(degree_check(G, deg));

  // Graph without self-loops
  // (0)--(1)
  //  | \  | \
  //  |  \ |  \
  // (2)--(3)-(4)
  G = BuildGraph(5, {{0, 1, 1},
                     {0, 2, 1},
                     {0, 3, 1},
                     {1, 3, 1},
                     {1, 4, 1},
                     {2, 3, 1},
                     {3, 4, 1}});
  deg = {3, 3, 2, 4, 2};
  ASSERT_TRUE(degree_check(G, deg));

  // Graph with self loop [*nodes have self loops]
  // (0)--(1*)
  //  | \  | \
  //  |  \ |  \
  // (2*)--(3)-(4*)
  G = BuildGraph(5, {{0, 1, 1},
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
  ASSERT_TRUE(degree_check(G, deg));

  // Try to get degree of non-existing nodes
  ASSERT_DEATH({ G.Degree(5); }, "");
  ASSERT_DEATH({ G.Degree(100); }, "");
}

TEST(Graph, Weights) {
  // Graph without edges
  comdata::Graph G = GenRandomUnweightedGraph(5, 0);
  std::vector<double> inc_w = {0, 0, 0, 0, 0};
  ASSERT_TRUE(inc_w_check(G, inc_w));
  ASSERT_EQ(G.TotalWeight(), 0);

  // Chain
  // (0)--(1)--(2)--(3)--(4)
  G = BuildGraph(5, {{0, 1, 0.1},
                     {1, 2, 0.5},
                     {2, 3, 2.3},
                     {3, 4, 4.2}});
  inc_w = {0.1, 0.6, 2.8, 6.5, 4.2};
  ASSERT_TRUE(inc_w_check(G, inc_w));
  ASSERT_NEAR(G.TotalWeight(), 7.1, 1e-6);

  // Tree
  //      (0)--(3)
  //     /   \
  //   (1)   (2)
  //    |   /   \
  //   (4) (5)  (6)
  G = BuildGraph(7, {{0, 1, 1.3},
                     {0, 2, 0.2},
                     {0, 3, 1},
                     {1, 4, 3.2},
                     {2, 5, 4.2},
                     {2, 6, 0.7}});
  inc_w = {2.5, 4.5, 5.1, 1, 3.2, 4.2, 0.7};
  ASSERT_TRUE(inc_w_check(G, inc_w));
  EXPECT_NEAR(G.TotalWeight(), 10.6, 1e-6);

  // Graph without self-loops
  // (0)--(1)
  //  | \  | \
  //  |  \ |  \
  // (2)--(3)-(4)
  G = BuildGraph(5, {{0, 1, 0.1},
                     {0, 2, 0.2},
                     {0, 3, 0.3},
                     {1, 3, 0.4},
                     {1, 4, 0.5},
                     {2, 3, 0.6},
                     {3, 4, 0.7}});
  inc_w = {0.6, 1, 0.8, 2, 1.2};
  ASSERT_TRUE(inc_w_check(G, inc_w));
  EXPECT_NEAR(G.TotalWeight(), 2.8, 1e-6);

  // Graph with self loop [*nodes have self loops]
  // (0)--(1*)
  //  | \  | \
  //  |  \ |  \
  // (2*)--(3)-(4*)
  G = BuildGraph(5, {{0, 1, 0.1},
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
  ASSERT_TRUE(inc_w_check(G, inc_w));
  EXPECT_NEAR(G.TotalWeight(), 5.5, 1e-6);

  // Try to get incident weight of non-existing node
  ASSERT_DEATH({ G.IncidentWeight(5); }, "");
  ASSERT_DEATH({ G.IncidentWeight(100); }, "");
}

TEST(Graph, Modularity) {
  // Graph without edges
  comdata::Graph G = GenRandomUnweightedGraph(5, 0);
  ASSERT_EQ(G.Modularity(), 0);

  // Chain
  // (0)--(1)--(2)--(3)--(4)
  G = BuildGraph(5, {{0, 1, 0.1},
                     {1, 2, 0.5},
                     {2, 3, 2.3},
                     {3, 4, 4.2}});
  std::vector<uint32_t> c = {0, 1, 1, 2, 2};
  set_communities(&G, c);
  EXPECT_NEAR(G.Modularity(), 0.37452886332076973, 1e-6);

  // Tree
  //      (0)--(3)
  //     /   \
  //   (1)   (2)
  //    |   /   \
  //   (4) (5)  (6)
  G = BuildGraph(7, {{0, 1, 1.3},
                     {0, 2, 0.2},
                     {0, 3, 1},
                     {1, 4, 3.2},
                     {2, 5, 4.2},
                     {2, 6, 0.7}});
  c = {0, 0, 1, 0, 0, 1, 2};
  set_communities(&G, c);
  EXPECT_NEAR(G.Modularity(), 0.6945087219651122, 1e-6);

  // Graph without self-loops
  // (0)--(1)
  //  | \  | \
  //  |  \ |  \
  // (2)--(3)-(4)
  G = BuildGraph(5, {{0, 1, 0.1},
                     {0, 2, 0.2},
                     {0, 3, 0.3},
                     {1, 3, 0.4},
                     {1, 4, 0.5},
                     {2, 3, 0.6},
                     {3, 4, 0.7}});
  c = {0, 1, 1, 1, 1};
  set_communities(&G, c);
  EXPECT_NEAR(G.Modularity(), 0.32653061224489793, 1e-6);

  // Graph with self loop [*nodes have self loops]
  // (0)--(1*)
  //  | \  | \
  //  |  \ |  \
  // (2*)--(3)-(4*)
  G = BuildGraph(5, {{0, 1, 0.1},
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
  set_communities(&G, c);
  EXPECT_NEAR(G.Modularity(), 0.2754545454545455, 1e-6);
}
