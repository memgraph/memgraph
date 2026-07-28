// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Unit tests for the node2vec Word2Vec core and the second-order random walk.
// These exercise the algorithms directly (no Memgraph graph context needed) and
// assert functional properties rather than exact vectors (which are seed- and
// thread-dependent, like gensim's).

#include <cmath>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include <node2vec/word2vec.hpp>

#include "algorithm/second_order_random_walk.hpp"

namespace {

using node2vec_alg::N2vGraph;
using node2vec_alg::NodeId;
using node2vec_alg::SecondOrderRandomWalk;
using node2vec_alg::Word2Vec;
using node2vec_alg::Word2VecParams;

constexpr int kTopics = 4;
constexpr int kWordsPerTopic = 10;

int TopicOf(int64_t word) { return static_cast<int>(word) / kWordsPerTopic; }

// Builds a corpus where each sentence is drawn from a single topic, so a correct
// embedding clusters same-topic words together.
std::vector<std::vector<int64_t>> StructuredCorpus(int n_sentences, int sentence_len, uint64_t seed) {
  std::mt19937_64 rng(seed);
  std::uniform_int_distribution<int> topic_dist(0, kTopics - 1);
  std::uniform_int_distribution<int> word_dist(0, kWordsPerTopic - 1);
  std::vector<std::vector<int64_t>> sents;
  sents.reserve(n_sentences);
  for (int i = 0; i < n_sentences; ++i) {
    const int topic = topic_dist(rng);
    std::vector<int64_t> s;
    s.reserve(sentence_len);
    for (int j = 0; j < sentence_len; ++j) s.push_back((topic * kWordsPerTopic) + word_dist(rng));
    sents.push_back(std::move(s));
  }
  return sents;
}

double Cosine(const std::vector<float> &a, const std::vector<float> &b) {
  double dot = 0;
  double na = 0;
  double nb = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    dot += static_cast<double>(a[i]) * b[i];
    na += static_cast<double>(a[i]) * a[i];
    nb += static_cast<double>(b[i]) * b[i];
  }
  if (na == 0 || nb == 0) return 0;
  return dot / (std::sqrt(na) * std::sqrt(nb));
}

// Fraction of words whose nearest neighbour (by cosine) is in the same topic.
double NearestNeighbourTopicPurity(const std::unordered_map<int64_t, std::vector<float>> &emb) {
  int correct = 0;
  int total = 0;
  for (const auto &qkv : emb) {
    double best = -2;
    int64_t best_word = qkv.first;
    for (const auto &okv : emb) {
      if (okv.first == qkv.first) continue;
      const double c = Cosine(qkv.second, okv.second);
      if (c > best) {
        best = c;
        best_word = okv.first;
      }
    }
    ++total;
    if (TopicOf(best_word) == TopicOf(qkv.first)) ++correct;
  }
  return total == 0 ? 0.0 : static_cast<double>(correct) / total;
}

Word2VecParams BaseParams(bool sg) {
  Word2VecParams p;
  p.vector_size = 32;
  p.window = 5;
  p.min_count = 1;
  p.workers = 1;
  p.alpha = 0.025;
  p.min_alpha = 0.0001;
  p.seed = 1;
  p.epochs = 5;
  p.sg = sg;
  p.hs = false;
  p.negative = 5;
  p.sample = 0.0;  // disable subsampling so the small test corpus trains strongly
  return p;
}

TEST(Word2VecTest, SkipGramRecoversTopicStructure) {
  auto corpus = StructuredCorpus(2000, 8, /*seed=*/7);
  Word2Vec model(BaseParams(/*sg=*/true));
  model.Train(corpus);
  auto emb = model.GetEmbeddings();

  EXPECT_EQ(emb.size(), static_cast<size_t>(kTopics * kWordsPerTopic));
  EXPECT_GT(NearestNeighbourTopicPurity(emb), 0.9);
}

TEST(Word2VecTest, CBOWRecoversTopicStructure) {
  auto corpus = StructuredCorpus(2000, 8, /*seed=*/7);
  Word2Vec model(BaseParams(/*sg=*/false));
  model.Train(corpus);
  auto emb = model.GetEmbeddings();

  EXPECT_EQ(emb.size(), static_cast<size_t>(kTopics * kWordsPerTopic));
  EXPECT_GT(NearestNeighbourTopicPurity(emb), 0.9);
}

TEST(Word2VecTest, HierarchicalSoftmaxTrains) {
  auto corpus = StructuredCorpus(2000, 8, /*seed=*/7);
  auto p = BaseParams(/*sg=*/true);
  p.negative = 0;
  p.hs = true;
  Word2Vec model(p);
  model.Train(corpus);
  auto emb = model.GetEmbeddings();

  EXPECT_EQ(emb.size(), static_cast<size_t>(kTopics * kWordsPerTopic));
  EXPECT_GT(NearestNeighbourTopicPurity(emb), 0.9);
}

TEST(Word2VecTest, EmbeddingShape) {
  auto corpus = StructuredCorpus(200, 8, /*seed=*/3);
  auto p = BaseParams(/*sg=*/true);
  p.vector_size = 16;
  Word2Vec model(p);
  model.Train(corpus);
  auto emb = model.GetEmbeddings();
  ASSERT_FALSE(emb.empty());
  for (const auto &kv : emb) EXPECT_EQ(kv.second.size(), 16U);
}

TEST(Word2VecTest, DeterministicSingleThreaded) {
  auto corpus = StructuredCorpus(500, 8, /*seed=*/11);
  Word2Vec a(BaseParams(true));
  Word2Vec b(BaseParams(true));
  a.Train(corpus);
  b.Train(corpus);
  auto ea = a.GetEmbeddings();
  auto eb = b.GetEmbeddings();
  ASSERT_EQ(ea.size(), eb.size());
  for (const auto &kv : ea) {
    const auto &other = eb.at(kv.first);
    for (size_t i = 0; i < kv.second.size(); ++i) EXPECT_FLOAT_EQ(kv.second[i], other[i]);
  }
}

TEST(Word2VecTest, IncrementalExtendsVocabAndPreservesUntouched) {
  auto p = BaseParams(true);
  p.epochs = 1;
  Word2Vec model(p);

  // First batch only contains tokens {0, 1, 2}.
  model.PartialFit({{0, 1, 2}, {2, 1, 0}, {1, 0, 2}});
  auto after_first = model.GetEmbeddings();
  ASSERT_EQ(after_first.count(0), 1U);
  auto token0_before = after_first.at(0);

  // Second batch introduces {3, 4} and never mentions token 0.
  model.PartialFit({{3, 4, 3}, {4, 3, 4}});
  auto after_second = model.GetEmbeddings();

  // New tokens were added, old ones retained.
  EXPECT_EQ(after_second.count(3), 1U);
  EXPECT_EQ(after_second.count(4), 1U);
  EXPECT_EQ(after_second.count(0), 1U);

  // Token 0 was absent from the second batch, so its vector is unchanged.
  const auto &token0_after = after_second.at(0);
  ASSERT_EQ(token0_before.size(), token0_after.size());
  for (size_t i = 0; i < token0_before.size(); ++i) EXPECT_FLOAT_EQ(token0_before[i], token0_after[i]);
}

TEST(Word2VecTest, VocabularyIsFrequencySorted) {
  // Token i occurs (i + 1) times, so descending-frequency order is 4,3,2,1,0.
  const std::vector<std::vector<int64_t>> corpus = {{0}, {1, 1}, {2, 2, 2}, {3, 3, 3, 3}, {4, 4, 4, 4, 4}};
  Word2Vec model(BaseParams(/*sg=*/true));
  model.Train(corpus);
  EXPECT_EQ(model.Vocabulary(), (std::vector<int64_t>{4, 3, 2, 1, 0}));
}

// Builds an undirected graph: triangle 0-1-2, plus 1-3 and 3-4.
N2vGraph MakeTestGraph() {
  N2vGraph g(/*is_directed=*/false);
  g.AddEdge(0, 1, 1.0);
  g.AddEdge(1, 2, 1.0);
  g.AddEdge(2, 0, 1.0);
  g.AddEdge(1, 3, 1.0);
  g.AddEdge(3, 4, 1.0);
  g.Build();
  return g;
}

TEST(SecondOrderRandomWalkTest, WalkStructure) {
  auto g = MakeTestGraph();
  const int num_walks = 3;
  const int walk_length = 5;
  SecondOrderRandomWalk walk(/*p=*/1.0, /*q=*/1.0, num_walks, walk_length, /*seed=*/42);
  auto walks = walk.SampleNodeWalks(g);

  // One walk per (node, walk index).
  EXPECT_EQ(walks.size(), g.Nodes().size() * static_cast<size_t>(num_walks));

  for (const auto &w : walks) {
    ASSERT_FALSE(w.empty());
    EXPECT_LE(static_cast<int>(w.size()), walk_length);
    // Consecutive nodes must be connected.
    for (size_t i = 1; i < w.size(); ++i) EXPECT_TRUE(g.HasEdge(w[i - 1], w[i]));
  }

  // Walks are grouped per start node, in node order.
  const auto &nodes = g.Nodes();
  for (size_t n = 0; n < nodes.size(); ++n)
    for (int k = 0; k < num_walks; ++k) EXPECT_EQ(walks[(n * num_walks) + k][0], nodes[n]);
}

TEST(SecondOrderRandomWalkTest, DeterministicWithSeed) {
  auto g1 = MakeTestGraph();
  auto g2 = MakeTestGraph();
  SecondOrderRandomWalk w1(2.0, 0.5, 4, 6, /*seed=*/123);
  SecondOrderRandomWalk w2(2.0, 0.5, 4, 6, /*seed=*/123);
  auto a = w1.SampleNodeWalks(g1);
  auto b = w2.SampleNodeWalks(g2);
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) EXPECT_EQ(a[i], b[i]);
}

// --- Transition-probability value tests (ported from test_second_order_random_walk.py)
// These verify the p/q biasing math, not just that walks are structurally valid.

constexpr double kP = 2.0;
constexpr double kQ = 0.5;

// Weighted edge set from test_second_order_random_walk.py.
const std::vector<std::tuple<NodeId, NodeId, double>> kProbEdges = {{0, 1, 1.5},
                                                                    {0, 2, 3.0},
                                                                    {0, 4, 4.1},
                                                                    {1, 5, 1.7},
                                                                    {1, 6, 2.6},
                                                                    {2, 5, 1.8},
                                                                    {3, 0, 1.9},
                                                                    {4, 6, 10.0},
                                                                    {7, 1, 21.0},
                                                                    {7, 5, 21.0},
                                                                    {7, 6, 13.0},
                                                                    {0, 5, 14.0},
                                                                    {6, 0, 17.0}};

double W(NodeId a, NodeId b) {
  for (const auto &[from, to, weight] : kProbEdges)
    if (from == a && to == b) return weight;
  return 0.0;
}

N2vGraph BuildProbGraph(bool is_directed) {
  N2vGraph g(is_directed);
  for (const auto &[from, to, weight] : kProbEdges) g.AddEdge(from, to, weight);
  g.Build();
  return g;
}

std::vector<double> Normalize(std::vector<double> v) {
  double sum = 0.0;
  for (const double x : v) sum += x;
  for (double &x : v) x /= sum;
  return v;
}

bool AllClose(const std::vector<double> &a, const std::vector<double> &b, double tol = 1e-9) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i)
    if (std::fabs(a[i] - b[i]) > tol) return false;
  return true;
}

TEST(SecondOrderRandomWalkTest, EdgeTransitionProbsUndirected) {
  auto g = BuildProbGraph(/*is_directed=*/false);
  SecondOrderRandomWalk walk(kP, kQ, /*num_walks=*/3, /*walk_length=*/2, /*seed=*/1);
  walk.Precompute(g);
  // neighbours of 1 (sorted): 0(=src)/p, 5, 6, 7(not adjacent to src)/q
  EXPECT_TRUE(AllClose(walk.EdgeTransitionProbs(0, 1), Normalize({W(0, 1) / kP, W(1, 5), W(1, 6), W(7, 1) / kQ})));
  // neighbours of 0 (sorted): 1,2,3(=src)/p,4,5,6 — none except src adjacent to src → /q
  EXPECT_TRUE(
      AllClose(walk.EdgeTransitionProbs(3, 0),
               Normalize({W(0, 1) / kQ, W(0, 2) / kQ, W(3, 0) / kP, W(0, 4) / kQ, W(0, 5) / kQ, W(6, 0) / kQ})));
}

TEST(SecondOrderRandomWalkTest, EdgeTransitionProbsDirected) {
  auto g = BuildProbGraph(/*is_directed=*/true);
  SecondOrderRandomWalk walk(kP, kQ, /*num_walks=*/3, /*walk_length=*/2, /*seed=*/1);
  walk.Precompute(g);
  // successors of 1: 5(not adjacent to src)/q, 6(6->0 edge exists)*1
  EXPECT_TRUE(AllClose(walk.EdgeTransitionProbs(0, 1), Normalize({W(1, 5) / kQ, W(1, 6)})));
  // successors of 0: 1,2,4,5 — none adjacent to src=3 → /q
  EXPECT_TRUE(
      AllClose(walk.EdgeTransitionProbs(3, 0), Normalize({W(0, 1) / kQ, W(0, 2) / kQ, W(0, 4) / kQ, W(0, 5) / kQ})));
}

TEST(SecondOrderRandomWalkTest, FirstPassProbsUndirected) {
  auto g = BuildProbGraph(/*is_directed=*/false);
  SecondOrderRandomWalk walk(kP, kQ, /*num_walks=*/3, /*walk_length=*/2, /*seed=*/1);
  walk.Precompute(g);
  EXPECT_TRUE(AllClose(walk.FirstPassTransitionProbs(1), Normalize({W(0, 1), W(1, 5), W(1, 6), W(7, 1)})));
  EXPECT_TRUE(
      AllClose(walk.FirstPassTransitionProbs(0), Normalize({W(0, 1), W(0, 2), W(3, 0), W(0, 4), W(0, 5), W(6, 0)})));
}

TEST(SecondOrderRandomWalkTest, FirstPassProbsDirected) {
  auto g = BuildProbGraph(/*is_directed=*/true);
  SecondOrderRandomWalk walk(kP, kQ, /*num_walks=*/3, /*walk_length=*/2, /*seed=*/1);
  walk.Precompute(g);
  EXPECT_TRUE(AllClose(walk.FirstPassTransitionProbs(1), Normalize({W(1, 5), W(1, 6)})));
  EXPECT_TRUE(AllClose(walk.FirstPassTransitionProbs(0), Normalize({W(0, 1), W(0, 2), W(0, 4), W(0, 5)})));
}

// --- N2vGraph tests (ported from the old test_basic_graph.py) ---------------

// Edge set from test_basic_graph.py (edge 0->1 has a non-default weight of 0.5).
const std::vector<std::tuple<NodeId, NodeId, double>> kBasicEdges = {
    {0, 1, 0.5},
    {0, 2, 1.0},
    {0, 4, 1.0},
    {1, 5, 1.0},
    {1, 6, 1.0},
    {2, 5, 1.0},
    {3, 0, 1.0},
    {4, 6, 1.0},
    {7, 1, 1.0},
    {7, 5, 1.0},
    {7, 6, 1.0},
};

N2vGraph BuildBasicGraph(bool is_directed) {
  N2vGraph g(is_directed);
  for (const auto &[from, to, weight] : kBasicEdges) g.AddEdge(from, to, weight);
  g.Build();
  return g;
}

std::set<NodeId> NodeSet(const N2vGraph &g) { return {g.Nodes().begin(), g.Nodes().end()}; }

TEST(N2vGraphTest, EdgeCount) {
  // Directed keeps each edge once; undirected also stores the reverse.
  EXPECT_EQ(BuildBasicGraph(true).Edges().size(), kBasicEdges.size());
  EXPECT_EQ(BuildBasicGraph(false).Edges().size(), kBasicEdges.size() * 2);
}

TEST(N2vGraphTest, HasEdgeDirected) {
  auto g = BuildBasicGraph(true);
  for (const auto &[from, to, weight] : kBasicEdges) {
    EXPECT_TRUE(g.HasEdge(from, to));
    EXPECT_FALSE(g.HasEdge(to, from));  // no reciprocal edges in this set
  }
}

TEST(N2vGraphTest, HasEdgeUndirected) {
  auto g = BuildBasicGraph(false);
  for (const auto &[from, to, weight] : kBasicEdges) {
    EXPECT_TRUE(g.HasEdge(from, to));
    EXPECT_TRUE(g.HasEdge(to, from));
  }
}

TEST(N2vGraphTest, NeighborsDirected) {
  auto g = BuildBasicGraph(true);
  // Only nodes that are an edge source appear (5 and 6 are targets only).
  const std::map<NodeId, std::vector<NodeId>> expected = {
      {0, {1, 2, 4}}, {1, {5, 6}}, {2, {5}}, {3, {0}}, {4, {6}}, {7, {1, 5, 6}}};
  std::set<NodeId> expected_nodes;
  for (const auto &[node, nbrs] : expected) expected_nodes.insert(node);
  EXPECT_EQ(NodeSet(g), expected_nodes);
  for (const auto &[node, nbrs] : expected) EXPECT_EQ(g.Neighbors(node), nbrs);
}

TEST(N2vGraphTest, NeighborsUndirected) {
  auto g = BuildBasicGraph(false);
  const std::map<NodeId, std::vector<NodeId>> expected = {{0, {1, 2, 3, 4}},
                                                          {1, {0, 5, 6, 7}},
                                                          {2, {0, 5}},
                                                          {3, {0}},
                                                          {4, {0, 6}},
                                                          {5, {1, 2, 7}},
                                                          {6, {1, 4, 7}},
                                                          {7, {1, 5, 6}}};
  std::set<NodeId> expected_nodes;
  for (const auto &[node, nbrs] : expected) expected_nodes.insert(node);
  EXPECT_EQ(NodeSet(g), expected_nodes);
  for (const auto &[node, nbrs] : expected) EXPECT_EQ(g.Neighbors(node), nbrs);
}

TEST(N2vGraphTest, EdgeWeightUndirectedIsSymmetric) {
  auto g = BuildBasicGraph(false);
  EXPECT_DOUBLE_EQ(g.EdgeWeight(0, 1), 0.5);
  EXPECT_DOUBLE_EQ(g.EdgeWeight(1, 0), 0.5);
}

TEST(N2vGraphTest, EdgeWeightDirectedThrowsOnReverse) {
  auto g = BuildBasicGraph(true);
  EXPECT_DOUBLE_EQ(g.EdgeWeight(0, 1), 0.5);
  EXPECT_THROW(g.EdgeWeight(1, 0), std::logic_error);  // reverse is not an edge in a directed graph
}

TEST(N2vGraphTest, AddEdgeAccumulatesParallelWeights) {
  N2vGraph g(/*is_directed=*/true);
  g.AddEdge(0, 1, 1.5);
  g.AddEdge(0, 1, 2.0);  // same (from, to): weights accumulate
  g.Build();
  EXPECT_DOUBLE_EQ(g.EdgeWeight(0, 1), 3.5);
  EXPECT_EQ(g.Neighbors(0).size(), 1U);  // still a single neighbour entry
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
