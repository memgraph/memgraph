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
#include <random>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include "algorithm/second_order_random_walk.hpp"
#include "algorithm/word2vec.hpp"

namespace {

using node2vec_alg::N2vGraph;
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
    int topic = topic_dist(rng);
    std::vector<int64_t> s;
    s.reserve(sentence_len);
    for (int j = 0; j < sentence_len; ++j) s.push_back(topic * kWordsPerTopic + word_dist(rng));
    sents.push_back(std::move(s));
  }
  return sents;
}

double Cosine(const std::vector<float> &a, const std::vector<float> &b) {
  double dot = 0, na = 0, nb = 0;
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
  int correct = 0, total = 0;
  for (const auto &qkv : emb) {
    double best = -2;
    int64_t best_word = qkv.first;
    for (const auto &okv : emb) {
      if (okv.first == qkv.first) continue;
      double c = Cosine(qkv.second, okv.second);
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
  for (const auto &kv : emb) EXPECT_EQ(kv.second.size(), 16u);
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
  ASSERT_EQ(after_first.count(0), 1u);
  auto token0_before = after_first.at(0);

  // Second batch introduces {3, 4} and never mentions token 0.
  model.PartialFit({{3, 4, 3}, {4, 3, 4}});
  auto after_second = model.GetEmbeddings();

  // New tokens were added, old ones retained.
  EXPECT_EQ(after_second.count(3), 1u);
  EXPECT_EQ(after_second.count(4), 1u);
  EXPECT_EQ(after_second.count(0), 1u);

  // Token 0 was absent from the second batch, so its vector is unchanged.
  const auto &token0_after = after_second.at(0);
  ASSERT_EQ(token0_before.size(), token0_after.size());
  for (size_t i = 0; i < token0_before.size(); ++i) EXPECT_FLOAT_EQ(token0_before[i], token0_after[i]);
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
  const int num_walks = 3, walk_length = 5;
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
    for (int k = 0; k < num_walks; ++k) EXPECT_EQ(walks[n * num_walks + k][0], nodes[n]);
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

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
