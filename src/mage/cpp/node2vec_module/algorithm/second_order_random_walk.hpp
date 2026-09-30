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
// Second-order (biased) random walk used by the batch node2vec module.

#pragma once

#include <algorithm>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace node2vec_alg {

using NodeId = int64_t;

// Weighted graph supporting node2vec's transition-probability queries.
class N2vGraph {
 public:
  N2vGraph(bool is_directed) : is_directed_(is_directed) {}

  void AddEdge(NodeId from, NodeId to, double weight) {
    auto &inner = edges_[from];
    auto it = inner.find(to);
    if (it == inner.end()) {
      inner.emplace(to, weight);
      edge_order_.emplace_back(from, to);
    } else {
      it->second += weight;
    }
  }

  // Builds adjacency (sorted neighbors) and node ordering from the edges.
  void Build() {
    for (const auto &e : edge_order_) {
      NodeId from = e.first, to = e.second;
      AddNeighbor(from, to);
      if (!is_directed_) AddNeighbor(to, from);
    }
    for (auto &kv : adj_) {
      std::sort(kv.second.begin(), kv.second.end());
    }
  }

  const std::vector<NodeId> &Nodes() const { return node_order_; }

  bool IsDirected() const { return is_directed_; }

  const std::vector<NodeId> &Neighbors(NodeId node) const {
    static const std::vector<NodeId> kEmpty;
    auto it = adj_.find(node);
    return it == adj_.end() ? kEmpty : it->second;
  }

  bool HasEdge(NodeId src, NodeId dest) const {
    if (HasDirected(src, dest)) return true;
    return !is_directed_ && HasDirected(dest, src);
  }

  double EdgeWeight(NodeId src, NodeId dest) const {
    auto it = edges_.find(src);
    if (it != edges_.end()) {
      auto jt = it->second.find(dest);
      if (jt != it->second.end()) return jt->second;
    }
    if (!is_directed_) {
      auto it2 = edges_.find(dest);
      if (it2 != edges_.end()) {
        auto jt2 = it2->second.find(src);
        if (jt2 != it2->second.end()) return jt2->second;
      }
    }
    // EdgeWeight is only defined for existing edges; callers pass (node, neighbor)
    // pairs, so this should be unreachable.
    throw std::logic_error("EdgeWeight called for a non-existent edge");
  }

  // (prev, cur) directed pairs over which edge transition probabilities are
  // defined.
  std::vector<std::pair<NodeId, NodeId>> Edges() const {
    std::vector<std::pair<NodeId, NodeId>> out = edge_order_;
    if (!is_directed_) {
      out.reserve(edge_order_.size() * 2);
      for (const auto &e : edge_order_) out.emplace_back(e.second, e.first);
    }
    return out;
  }

 private:
  bool is_directed_;
  std::unordered_map<NodeId, std::unordered_map<NodeId, double>> edges_;
  std::vector<std::pair<NodeId, NodeId>> edge_order_;
  std::unordered_map<NodeId, std::vector<NodeId>> adj_;
  std::unordered_map<NodeId, std::unordered_set<NodeId>> adj_set_;
  std::vector<NodeId> node_order_;

  bool HasDirected(NodeId src, NodeId dest) const {
    auto it = edges_.find(src);
    return it != edges_.end() && it->second.count(dest) > 0;
  }

  void AddNeighbor(NodeId node, NodeId nbr) {
    auto &set = adj_set_[node];
    if (set.insert(nbr).second) {
      if (adj_.find(node) == adj_.end()) {
        adj_.emplace(node, std::vector<NodeId>{});
        node_order_.push_back(node);
      }
      adj_[node].push_back(nbr);
    }
  }
};

class SecondOrderRandomWalk {
 public:
  SecondOrderRandomWalk(double p, double q, int num_walks, int walk_length, uint64_t seed)
      : p_(p), q_(q), num_walks_(num_walks), walk_length_(walk_length), seed_(seed) {}

  std::vector<std::vector<NodeId>> SampleNodeWalks(N2vGraph &graph) {
    Precompute(graph);

    std::mt19937_64 rng(seed_);  // NOSONAR
    std::vector<std::vector<NodeId>> walks;
    for (NodeId node : graph.Nodes()) {
      for (int i = 0; i < num_walks_; ++i) {
        walks.push_back(SampleWalk(graph, node, rng));
      }
    }
    return walks;
  }

  // Computes the first-pass and second-order transition probabilities for the
  // graph without sampling any walks. Called by SampleNodeWalks; exposed
  // (together with the accessors below) so the biasing can be inspected/tested.
  void Precompute(N2vGraph &graph) {
    SetFirstPassTransitionProbs(graph);
    SetGraphTransitionProbs(graph);
  }

  // First-pass transition probabilities for `node`, aligned with its sorted
  // neighbours. Only valid after Precompute/SampleNodeWalks.
  const std::vector<double> &FirstPassTransitionProbs(NodeId node) const { return first_pass_.at(node); }

  // Second-order transition probabilities for the (prev -> cur) step, aligned
  // with cur's sorted neighbours. Only valid after Precompute/SampleNodeWalks.
  const std::vector<double> &EdgeTransitionProbs(NodeId prev, NodeId cur) const { return edge_probs_.at({prev, cur}); }

 private:
  double p_, q_;
  int num_walks_, walk_length_;
  uint64_t seed_;

  using Edge = std::pair<NodeId, NodeId>;  // (previous node, current node)

  // Hash for an (prev, cur) edge. A collision here is harmless: the unordered_map
  // still compares the full pair with operator==, so distinct edges never share
  // an entry (unlike packing both ids into a single integer key).
  struct EdgeHash {
    std::size_t operator()(const Edge &e) const noexcept {
      auto h = static_cast<std::uint64_t>(e.first) * 0x9E3779B97F4A7C15ULL;
      h ^= static_cast<std::uint64_t>(e.second) + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
      return static_cast<std::size_t>(h);
    }
  };

  std::unordered_map<NodeId, std::vector<double>> first_pass_;
  std::unordered_map<Edge, std::vector<double>, EdgeHash> edge_probs_;

  static std::vector<double> Normalize(const std::vector<double> &v) {
    double sum = 0.0;
    for (double x : v) sum += x;
    std::vector<double> out(v.size());
    if (sum == 0.0) return out;
    for (size_t i = 0; i < v.size(); ++i) out[i] = v[i] / sum;
    return out;
  }

  void SetFirstPassTransitionProbs(N2vGraph &graph) {
    for (NodeId src : graph.Nodes()) {
      const auto &nbrs = graph.Neighbors(src);
      std::vector<double> unnorm;
      unnorm.reserve(nbrs.size());
      for (NodeId nbr : nbrs) unnorm.push_back(graph.EdgeWeight(src, nbr));
      first_pass_[src] = Normalize(unnorm);
    }
  }

  std::vector<double> CalculateEdgeTransitionProbs(N2vGraph &graph, NodeId src, NodeId dest) {
    std::vector<double> unnorm;
    for (NodeId dn : graph.Neighbors(dest)) {
      double w = graph.EdgeWeight(dest, dn);
      if (dn == src)
        unnorm.push_back(w / p_);
      else if (graph.HasEdge(dn, src))
        unnorm.push_back(w);
      else
        unnorm.push_back(w / q_);
    }
    return Normalize(unnorm);
  }

  void SetGraphTransitionProbs(N2vGraph &graph) {
    // Edges() already yields both (u,v) and (v,u) for undirected graphs, so each
    // direction is computed exactly once here (no separate reverse pass, which
    // would recompute and overwrite every vector).
    for (const auto &e : graph.Edges()) {
      edge_probs_[{e.first, e.second}] = CalculateEdgeTransitionProbs(graph, e.first, e.second);
    }
  }

  static NodeId WeightedChoice(const std::vector<NodeId> &items, const std::vector<double> &probs,
                               std::mt19937_64 &rng) {  // NOSONAR
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double r = dist(rng);
    double cum = 0.0;
    for (size_t i = 0; i < items.size(); ++i) {
      cum += probs[i];
      if (r <= cum) return items[i];
    }
    return items.back();
  }

  std::vector<NodeId> SampleWalk(N2vGraph &graph, NodeId start, std::mt19937_64 &rng) {  // NOSONAR
    std::vector<NodeId> walk{start};
    while (static_cast<int>(walk.size()) < walk_length_) {
      NodeId current = walk.back();
      const auto &nbrs = graph.Neighbors(current);
      if (nbrs.empty()) break;

      if (walk.size() == 1) {
        walk.push_back(WeightedChoice(nbrs, first_pass_[current], rng));
        continue;
      }
      NodeId prev = walk[walk.size() - 2];
      auto it = edge_probs_.find({prev, current});
      if (it == edge_probs_.end()) break;
      walk.push_back(WeightedChoice(nbrs, it->second, rng));
    }
    return walk;
  }
};

}  // namespace node2vec_alg
