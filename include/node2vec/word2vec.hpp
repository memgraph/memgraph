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
// Word2Vec implementation used by the node2vec and node2vec_online query
// modules. This is a from-scratch, dependency-free replacement for the
// `gensim.models.Word2Vec` model that those modules previously relied on.
//
// It implements skip-gram and CBOW training with negative sampling and
// hierarchical softmax, in both a batch mode (`Train`) and an incremental
// mode (`PartialFit`) that extends the vocabulary while preserving previously
// learned vectors. The goal is functional equivalence with gensim (same
// parameters and qualitative behaviour), not bit-for-bit identical vectors.
//
// Shared by the batch node2vec module (src/mage/cpp/node2vec_module) and the
// online node2vec_online module (query_modules/node2vec_online_module); it lives
// in the common include/ directory so there is a single source of truth.

#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <random>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace node2vec_alg {

// Parameters mirror the subset of gensim.models.Word2Vec arguments that the
// node2vec modules pass through.
struct Word2VecParams {
  int vector_size = 100;
  int window = 5;
  int min_count = 1;
  int workers = 1;
  double alpha = 0.025;
  double min_alpha = 0.0001;
  int seed = 1;
  int epochs = 5;
  bool sg = true;        // true: skip-gram, false: CBOW
  bool hs = false;       // hierarchical softmax
  int negative = 5;      // number of negative samples (0 disables negative sampling)
  double sample = 1e-3;  // frequent-word subsampling threshold (gensim default; 0 disables)
};

class Word2Vec {
 public:
  using Token = int64_t;

  explicit Word2Vec(const Word2VecParams &params) : p_(params), dim_(params.vector_size) {
    if (dim_ <= 0) dim_ = 1;
    seed_ = static_cast<uint64_t>(p_.seed);
    InitExpTable();
  }

  // Batch training: rebuilds the vocabulary from scratch and trains for
  // `epochs` epochs over `sentences`.
  void Train(const std::vector<std::vector<Token>> &sentences) {
    BuildVocab(sentences, /*update=*/false);
    if (vocab_size_ == 0) return;
    TrainEpochs(sentences, p_.epochs);
  }

  // Incremental training: on the first call this is equivalent to building a
  // model from `sentences`; on subsequent calls it extends the vocabulary with
  // any previously unseen tokens (keeping the vectors learned so far) and then
  // trains for `epochs` epochs on the supplied sentences.
  void PartialFit(const std::vector<std::vector<Token>> &sentences) {
    const bool first = (vocab_size_ == 0);
    BuildVocab(sentences, /*update=*/!first);
    if (vocab_size_ == 0) return;
    TrainEpochs(sentences, p_.epochs);
  }

  // Returns a mapping from token (node id) to its embedding vector, mirroring
  // gensim's `{wv.index_to_key[i]: wv.vectors[i]}`.
  std::unordered_map<Token, std::vector<float>> GetEmbeddings() const {
    std::unordered_map<Token, std::vector<float>> out;
    out.reserve(vocab_size_);
    for (int i = 0; i < vocab_size_; ++i) {
      const float *row = &syn0_[static_cast<size_t>(i) * dim_];
      out.emplace(index2token_[i], std::vector<float>(row, row + dim_));
    }
    return out;
  }

  bool Empty() const { return vocab_size_ == 0; }

  int Dim() const { return dim_; }

  // Vocabulary tokens in index order. After a fresh Train this is sorted by
  // descending frequency (ties broken by ascending token id).
  const std::vector<Token> &Vocabulary() const { return index2token_; }

 private:
  static constexpr int kExpTableSize = 1000;
  static constexpr double kMaxExp = 6.0;
  static constexpr double kNsExponent = 0.75;

  Word2VecParams p_;
  int dim_;
  uint64_t seed_;

  // Vocabulary.
  std::unordered_map<Token, int> token2index_;
  std::vector<Token> index2token_;
  std::vector<int64_t> counts_;
  int vocab_size_ = 0;
  int64_t total_words_ = 0;        // sum of vocabulary counts
  std::vector<double> keep_prob_;  // per-token subsampling keep probability

  // Weight matrices (row-major, vocab_size_ * dim_).
  std::vector<float> syn0_;     // input/embedding vectors
  std::vector<float> syn1neg_;  // output vectors for negative sampling
  std::vector<float> syn1_;     // inner-node vectors for hierarchical softmax

  // Hierarchical softmax Huffman encoding (per token).
  std::vector<std::vector<uint8_t>> hs_codes_;
  std::vector<std::vector<int>> hs_points_;

  // Negative sampling cumulative distribution over the vocabulary.
  std::vector<double> cum_table_;

  // Sigmoid lookup table.
  std::vector<float> exp_table_;

  void InitExpTable() {
    exp_table_.resize(kExpTableSize + 1);
    for (int i = 0; i <= kExpTableSize; ++i) {
      double x = (static_cast<double>(i) / kExpTableSize * 2.0 - 1.0) * kMaxExp;
      double e = std::exp(x);
      exp_table_[i] = static_cast<float>(e / (e + 1.0));  // sigmoid(x)
    }
  }

  inline float Sigmoid(float f) const {
    if (f >= kMaxExp) return 1.0f;
    if (f <= -kMaxExp) return 0.0f;
    int idx = static_cast<int>((f + kMaxExp) * (kExpTableSize / kMaxExp / 2.0));
    if (idx < 0) idx = 0;
    if (idx > kExpTableSize) idx = kExpTableSize;
    return exp_table_[idx];
  }

  // Deterministic per-token initialization of an embedding row, independent of
  // insertion order (so incremental updates are reproducible). Mirrors gensim's
  // uniform(-0.5/dim, 0.5/dim) initialization seeded per word.
  void InitRow(int index, Token token) {
    uint64_t s = seed_ ^ (static_cast<uint64_t>(token) * 0x9E3779B97F4A7C15ULL + 0x632BE59BD9B4E019ULL);
    std::mt19937_64 rng(s);  // NOSONAR
    std::uniform_real_distribution<float> dist(-0.5f / dim_, 0.5f / dim_);
    float *row = &syn0_[static_cast<size_t>(index) * dim_];
    for (int d = 0; d < dim_; ++d) row[d] = dist(rng);
  }

  void BuildVocab(const std::vector<std::vector<Token>> &sentences, bool update) {
    // Accumulate counts from the supplied sentences.
    std::unordered_map<Token, int64_t> freq;
    for (const auto &sent : sentences)
      for (Token t : sent) ++freq[t];

    if (!update) {
      token2index_.clear();
      index2token_.clear();
      counts_.clear();
      vocab_size_ = 0;
    }

    // Assign vocabulary indices in a deterministic order: descending count, then
    // ascending token id. On a fresh build this makes the vocabulary
    // frequency-sorted, so BuildHuffman produces an optimal tree (matching
    // gensim). On an update, existing tokens keep their index (matched below);
    // only newly-seen tokens are appended, in this order.
    std::vector<std::pair<Token, int64_t>> ordered(freq.begin(), freq.end());
    std::sort(ordered.begin(), ordered.end(), [](const auto &a, const auto &b) {
      if (a.second != b.second) return a.second > b.second;
      return a.first < b.first;
    });

    int old_size = vocab_size_;
    for (const auto &kv : ordered) {
      Token t = kv.first;
      int64_t c = kv.second;
      auto it = token2index_.find(t);
      if (it == token2index_.end()) {
        if (c < p_.min_count) continue;
        int idx = vocab_size_++;
        token2index_.emplace(t, idx);
        index2token_.push_back(t);
        counts_.push_back(c);
      } else {
        counts_[it->second] += c;
      }
    }
    if (vocab_size_ == 0) return;

    // Grow weight matrices, initializing only the newly added rows.
    syn0_.resize(static_cast<size_t>(vocab_size_) * dim_);
    syn1neg_.resize(static_cast<size_t>(vocab_size_) * dim_, 0.0f);
    for (int i = old_size; i < vocab_size_; ++i) InitRow(i, index2token_[i]);

    if (p_.negative > 0) BuildNegTable();
    if (p_.hs) BuildHuffman();
    BuildSubsamplingTable();
  }

  // Computes per-token keep probabilities for frequent-word subsampling,
  // matching gensim/word2vec.c (sample * total_words threshold).
  void BuildSubsamplingTable() {
    total_words_ = 0;
    for (int i = 0; i < vocab_size_; ++i) total_words_ += counts_[i];
    keep_prob_.assign(vocab_size_, 1.0);
    if (p_.sample <= 0.0 || total_words_ == 0) return;
    double threshold_count = p_.sample * static_cast<double>(total_words_);
    for (int i = 0; i < vocab_size_; ++i) {
      double c = static_cast<double>(counts_[i]);
      double prob = (std::sqrt(c / threshold_count) + 1.0) * (threshold_count / c);
      keep_prob_[i] = prob < 1.0 ? prob : 1.0;
    }
  }

  void BuildNegTable() {
    cum_table_.assign(vocab_size_, 0.0);
    double total = 0.0;
    for (int i = 0; i < vocab_size_; ++i) total += std::pow(static_cast<double>(counts_[i]), kNsExponent);
    double cum = 0.0;
    for (int i = 0; i < vocab_size_; ++i) {
      cum += std::pow(static_cast<double>(counts_[i]), kNsExponent) / total;
      cum_table_[i] = cum;
    }
    cum_table_[vocab_size_ - 1] = 1.0;  // guard against fp drift
  }

  inline int SampleNegative(std::mt19937_64 &rng) const {  // NOSONAR
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double r = dist(rng);
    int lo = 0, hi = vocab_size_ - 1;
    while (lo < hi) {
      int mid = (lo + hi) / 2;
      if (cum_table_[mid] < r)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  // Builds the Huffman tree for hierarchical softmax and derives, for every
  // token, its root->leaf path: hs_codes_ (the 0/1 branch bits) and hs_points_
  // (the syn1_ inner-node rows visited). Frequent tokens get shorter paths.
  // Standard word2vec construction.
  void BuildHuffman() {
    int n = vocab_size_;  // number of leaves (vocabulary words)
    // A binary tree over n leaves has n-1 inner nodes; syn1_ holds one weight
    // vector per inner node.
    syn1_.assign(static_cast<size_t>(n > 0 ? n - 1 : 0) * dim_, 0.0f);
    hs_codes_.assign(n, {});
    hs_points_.assign(n, {});
    if (n <= 1) return;

    // Work arrays cover leaves at indices [0, n) and the inner nodes at
    // [n, 2n) that are created while merging.
    std::vector<int64_t> count(2 * n);                  // node weight (leaf frequency, or merged sum)
    std::vector<int> binary(2 * n, 0);                  // which branch (0/1) this node is of its parent
    std::vector<int> parent(2 * n, 0);                  // index of this node's parent
    for (int i = 0; i < n; ++i) count[i] = counts_[i];  // leaf frequencies
    for (int i = n; i < 2 * n; ++i) count[i] = static_cast<int64_t>(1e18);  // inner nodes: "not created yet"

    // Repeatedly merge the two lowest-count nodes into a new inner node. Two
    // cursors give O(n) selection because both candidate lists are already
    // sorted by count: pos1 scans leaves right-to-left (the vocab is sorted by
    // descending frequency, so this ascends in count), pos2 scans inner nodes
    // left-to-right (they are created in ascending count order). The next
    // smallest node is therefore always at one of the two cursors.
    int pos1 = n - 1, pos2 = n;
    for (int a = 0; a < n - 1; ++a) {
      // Pick the two smallest available nodes: min1, then min2.
      int min1;
      if (pos1 >= 0) {
        if (count[pos1] < count[pos2]) {
          min1 = pos1--;
        } else {
          min1 = pos2++;
        }
      } else {
        min1 = pos2++;  // leaves exhausted: take an inner node
      }
      int min2;
      if (pos1 >= 0) {
        if (count[pos1] < count[pos2]) {
          min2 = pos1--;
        } else {
          min2 = pos2++;
        }
      } else {
        min2 = pos2++;
      }
      // New inner node (n + a) becomes the parent of the two picked nodes.
      count[n + a] = count[min1] + count[min2];
      parent[min1] = n + a;
      parent[min2] = n + a;
      binary[min2] = 1;  // second child is the "1" branch; min1 keeps the default 0
    }

    // For each leaf, walk up to the root collecting branch bits and inner-node
    // indices, then store them root->leaf.
    for (int a = 0; a < n; ++a) {
      std::vector<uint8_t> code;  // branch bits,        leaf -> root
      std::vector<int> point;     // inner-node indices, leaf -> root
      int b = a;
      while (true) {
        code.push_back(static_cast<uint8_t>(binary[b]));
        point.push_back(b);
        b = parent[b];
        if (b == 2 * n - 2) break;  // 2n-2 is the root (the last inner node created)
      }
      // Reverse into root->leaf order. Inner-node indices are shifted by -n to
      // address syn1_ rows; the path always starts at the root row (n-2).
      hs_points_[a].push_back(n - 2);
      for (int i = static_cast<int>(point.size()) - 1; i >= 0; --i) {
        hs_codes_[a].push_back(code[i]);
        if (i > 0) hs_points_[a].push_back(point[i] - n);
      }
    }
  }

  void TrainEpochs(const std::vector<std::vector<Token>> &sentences, int epochs) {
    // Convert tokens to vocabulary indices once, dropping out-of-vocab tokens.
    std::vector<std::vector<int>> coded;
    coded.reserve(sentences.size());
    int64_t total_tokens = 0;
    for (const auto &sent : sentences) {
      std::vector<int> c;
      c.reserve(sent.size());
      for (Token t : sent) {
        auto it = token2index_.find(t);
        if (it != token2index_.end()) c.push_back(it->second);
      }
      total_tokens += static_cast<int64_t>(c.size());
      coded.push_back(std::move(c));
    }
    if (total_tokens == 0) return;

    int workers = p_.workers > 0 ? p_.workers : 1;
    workers = std::min<int>(workers, std::max<int>(1, static_cast<int>(coded.size())));

    int64_t total_train_words = total_tokens * static_cast<int64_t>(epochs);
    std::atomic<int64_t> words_done{0};

    for (int epoch = 0; epoch < epochs; ++epoch) {
      if (workers <= 1) {
        TrainRange(
            coded, 0, coded.size(), words_done, total_train_words, seed_ + static_cast<uint64_t>(epoch) * 7919u + 1u);
      } else {
        std::vector<std::thread> pool;
        size_t chunk = (coded.size() + workers - 1) / workers;
        for (int w = 0; w < workers; ++w) {
          size_t begin = static_cast<size_t>(w) * chunk;
          if (begin >= coded.size()) break;
          size_t end = std::min(begin + chunk, coded.size());
          uint64_t rng_seed = seed_ + static_cast<uint64_t>(epoch) * 7919u + static_cast<uint64_t>(w) * 104729u + 1u;
          pool.emplace_back(
              [&, begin, end, rng_seed]() { TrainRange(coded, begin, end, words_done, total_train_words, rng_seed); });
        }
        for (auto &th : pool) th.join();
      }
    }
  }

  // Trains over coded[begin, end). Weight updates are intentionally lock-free
  // (Hogwild-style), matching gensim's multithreaded training.
  void TrainRange(const std::vector<std::vector<int>> &coded, size_t begin, size_t end,
                  std::atomic<int64_t> &words_done, int64_t total_train_words, uint64_t rng_seed) {
    std::mt19937_64 rng(rng_seed);         // NOSONAR
    std::vector<float> neu1(dim_, 0.0f);   // CBOW hidden layer
    std::vector<float> neu1e(dim_, 0.0f);  // accumulated gradient

    double start_alpha = p_.alpha;
    double end_alpha = p_.min_alpha;
    int64_t local_processed = 0;

    std::uniform_real_distribution<double> keep_dist(0.0, 1.0);
    std::vector<int> sentence;

    for (size_t si = begin; si < end; ++si) {
      const std::vector<int> &raw = coded[si];
      // Apply frequent-word subsampling while reading the sentence (gensim
      // drops sampled-out words entirely for this pass).
      sentence.clear();
      sentence.reserve(raw.size());
      for (int idx : raw) {
        if (keep_prob_[idx] >= 1.0 || keep_dist(rng) <= keep_prob_[idx]) sentence.push_back(idx);
      }
      const int len = static_cast<int>(sentence.size());
      if (len == 0) continue;

      // Refresh the learning rate roughly once per sentence.
      int64_t seen = words_done.load(std::memory_order_relaxed) + local_processed;
      double progress = total_train_words > 0 ? static_cast<double>(seen) / total_train_words : 1.0;
      if (progress > 1.0) progress = 1.0;
      float alpha = static_cast<float>(start_alpha - (start_alpha - end_alpha) * progress);
      if (alpha < end_alpha) alpha = static_cast<float>(end_alpha);

      for (int i = 0; i < len; ++i) {
        int b = static_cast<int>(rng() % static_cast<uint64_t>(p_.window));  // reduced window in [0, window)
        if (p_.sg)
          TrainSGWord(sentence, i, b, alpha, neu1e, rng);
        else
          TrainCBOWWord(sentence, i, b, alpha, neu1, neu1e, rng);
      }
      local_processed += static_cast<int64_t>(raw.size());
    }
    words_done.fetch_add(local_processed, std::memory_order_relaxed);
  }

  // Skip-gram: for each context word around position i, use that context word's
  // input vector to predict the center word, then apply the resulting gradient
  // back to the context word's vector.
  //   i     - position of the center word in the sentence
  //   b     - random window shrink for this center word (word2vec's dynamic window)
  //   neu1e - reusable per-call gradient accumulator (caller-owned scratch)
  void TrainSGWord(const std::vector<int> &sentence, int i, int b, float alpha, std::vector<float> &neu1e,
                   std::mt19937_64 &rng) {  // NOSONAR
    const int len = static_cast<int>(sentence.size());
    const int center = sentence[i];  // the word being predicted (output/target)
    // `a` scans the window [-window+b, +window-b] around i; a == window is the
    // center itself (skipped), and out-of-sentence positions are skipped too.
    for (int a = b; a < 2 * p_.window + 1 - b; ++a) {
      int c = i - p_.window + a;  // sentence position of the context word
      if (a == p_.window || c < 0 || c >= len) continue;
      int context = sentence[c];
      float *l1 = &syn0_[static_cast<size_t>(context) * dim_];  // context word's vector = the hidden layer
      std::fill(neu1e.begin(), neu1e.end(), 0.0f);              // reset gradient accumulator for l1
      TrainPair(center, l1, alpha, neu1e.data(), rng);
      for (int d = 0; d < dim_; ++d) l1[d] += neu1e[d];  // apply the accumulated gradient to the context vector
    }
  }

  // CBOW: average the input vectors of all context words around position i into
  // a single hidden vector, use it to predict the center word, then apply the
  // resulting gradient back to every context word's vector.
  //   neu1  - reusable scratch for the averaged context (the hidden layer)
  //   neu1e - reusable scratch for the gradient shared by all context words
  void TrainCBOWWord(const std::vector<int> &sentence, int i, int b, float alpha, std::vector<float> &neu1,
                     std::vector<float> &neu1e, std::mt19937_64 &rng) {  // NOSONAR
    const int len = static_cast<int>(sentence.size());
    const int center = sentence[i];  // the word being predicted (output/target)
    std::fill(neu1.begin(), neu1.end(), 0.0f);
    std::fill(neu1e.begin(), neu1e.end(), 0.0f);

    // Sum the input vectors of the context words in the (dynamically shrunk)
    // window; a == window is the center (skipped), as are out-of-range positions.
    int cw = 0;  // number of context words summed
    for (int a = b; a < 2 * p_.window + 1 - b; ++a) {
      int c = i - p_.window + a;
      if (a == p_.window || c < 0 || c >= len) continue;
      const float *row = &syn0_[static_cast<size_t>(sentence[c]) * dim_];
      for (int d = 0; d < dim_; ++d) neu1[d] += row[d];
      ++cw;
    }
    if (cw == 0) return;                           // no context words -> nothing to train
    for (int d = 0; d < dim_; ++d) neu1[d] /= cw;  // mean of the context vectors (cbow_mean = 1)

    TrainPair(center, neu1.data(), alpha, neu1e.data(), rng);

    // Back-propagate the same gradient to every context word's input vector.
    for (int a = b; a < 2 * p_.window + 1 - b; ++a) {
      int c = i - p_.window + a;
      if (a == p_.window || c < 0 || c >= len) continue;
      float *row = &syn0_[static_cast<size_t>(sentence[c]) * dim_];
      for (int d = 0; d < dim_; ++d) row[d] += neu1e[d];
    }
  }

  // One binary logistic-regression update between the hidden vector `l1` and a
  // single output row `l2`, toward the given `label` (the probability the model
  // should assign to the "1" branch: 1 for a positive, 0 for a negative). The
  // input-side gradient is accumulated into `neu1e`; `l2` is updated in place.
  // Skips saturated scores (|f| >= kMaxExp), matching gensim's inner loops.
  inline void UpdateOutput(float *l2, float label, const float *l1, float alpha, float *neu1e) {
    float f = 0.0f;
    for (int d = 0; d < dim_; ++d) f += l1[d] * l2[d];
    if (f >= kMaxExp || f <= -kMaxExp) return;
    float g = (label - Sigmoid(f)) * alpha;
    for (int d = 0; d < dim_; ++d) neu1e[d] += g * l2[d];
    for (int d = 0; d < dim_; ++d) l2[d] += g * l1[d];
  }

  // Trains the output layer to predict `target` from the hidden vector `l1`,
  // accumulating the input-side gradient into `neu1e`. Uses hierarchical softmax
  // and/or negative sampling depending on parameters (mirrors gensim's
  // w2v_fast_sentence_*_hs / _neg).
  void TrainPair(int target, const float *l1, float alpha, float *neu1e, std::mt19937_64 &rng) {  // NOSONAR
    // Hierarchical softmax: one binary decision per internal node on `target`'s
    // Huffman path. code[j] is the bit toward `target`, so the target label for
    // the "1" branch is (1 - code[j]).
    if (p_.hs && !hs_codes_.empty()) {
      const std::vector<uint8_t> &code = hs_codes_[target];
      const std::vector<int> &point = hs_points_[target];
      for (size_t j = 0; j < code.size(); ++j) {
        float *l2 = &syn1_[static_cast<size_t>(point[j]) * dim_];
        UpdateOutput(l2, 1.0f - static_cast<float>(code[j]), l1, alpha, neu1e);
      }
    }

    // Negative sampling: pull the true target up (label 1) and push `negative`
    // random noise words down (label 0).
    if (p_.negative > 0) {
      for (int n = 0; n < p_.negative + 1; ++n) {
        int sample = target;
        float label = 1.0f;  // n == 0: the positive
        if (n != 0) {
          sample = SampleNegative(rng);
          if (sample == target) continue;  // never train the target as its own negative
          label = 0.0f;
        }
        UpdateOutput(&syn1neg_[static_cast<size_t>(sample) * dim_], label, l1, alpha, neu1e);
      }
    }
  }
};

}  // namespace node2vec_alg
