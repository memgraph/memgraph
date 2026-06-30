// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
// StreamWalk temporal random-walk sampler used by the node2vec_online module.
// Direct port of the previous Python implementation
// (query_modules/node2vec_online_module/node2vec_online_utils/walk_sampling.py).
//
// The probability of a walk z observed at time t is
//   p(z, t) = (beta ^ |z|) * exp(-c * (t - t1)),
// where t1 is the timestamp of the oldest edge in the walk and
//   c = -ln(0.5) / half_life.

#pragma once

#include <cmath>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

namespace node2vec_alg {

class StreamWalkUpdater {
 public:
  using NodeId = int64_t;

  StreamWalkUpdater(int64_t half_life, int max_length, double beta, int64_t cutoff, int sampled_walks, bool full_walks,
                    uint64_t seed = 1)
      : c_coef_(-std::log(0.5) / static_cast<double>(half_life)),
        beta_(beta),
        sampled_walks_(sampled_walks),
        cutoff_(cutoff),
        max_length_(max_length),
        full_walks_(full_walks),
        rng_(seed) {}

  // Processes a new (source -> target) edge observed at `time` and returns the
  // sampled walks (pairs of endpoints, or full walks if full_walks is set).
  std::vector<std::vector<NodeId>> ProcessNewEdge(NodeId source, NodeId target, int64_t time) {
    Update(source, target, time);

    std::vector<std::vector<NodeId>> walks;
    if (graph_.find(source) == graph_.end()) {
      // source is not reachable from any node within cutoff
      walks.assign(sampled_walks_, {source, target});
      return walks;
    }
    walks.reserve(sampled_walks_);
    for (int i = 0; i < sampled_walks_; ++i) walks.push_back(SampleSingleWalk(source, target, time));
    return walks;
  }

 private:
  struct InEdge {
    NodeId node;
    int64_t time;
    double centrality;
  };

  double c_coef_;
  double beta_;
  int sampled_walks_;
  int64_t cutoff_;
  int max_length_;
  bool full_walks_;
  std::mt19937_64 rng_;

  std::unordered_map<NodeId, std::vector<InEdge>> graph_;  // in-edges per node, arrival order
  std::unordered_map<NodeId, int64_t> last_timestamp_;
  std::unordered_map<NodeId, double> centrality_;

  double Uniform() {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng_);
  }

  double Get(const std::unordered_map<NodeId, double> &m, NodeId k, double def) const {
    auto it = m.find(k);
    return it == m.end() ? def : it->second;
  }

  std::vector<NodeId> SampleSingleWalk(NodeId source, NodeId target, int64_t time) {
    NodeId node = source;
    int64_t time_ = last_timestamp_.count(source) ? last_timestamp_[source] : 0;
    double cent = Get(centrality_, source, 0.0);

    std::vector<NodeId> walk{node};
    while (true) {
      if (Uniform() < 1.0 / (cent + 1.0) || graph_.find(node) == graph_.end() ||
          static_cast<int>(walk.size()) >= max_length_) {
        break;
      }
      double sum_target = cent * Uniform();
      double sum_acc = 0.0;
      bool broken = false;
      const auto &in_edges = graph_[node];
      InEdge chosen{};
      for (auto it = in_edges.rbegin(); it != in_edges.rend(); ++it) {
        if (it->time < time_) {
          sum_acc += (it->centrality + 1.0) * beta_ * std::exp(c_coef_ * static_cast<double>(it->time - time_));
          if (sum_acc >= sum_target) {
            chosen = *it;
            broken = true;
            break;
          }
        }
      }
      if (!broken) break;
      node = chosen.node;
      time_ = chosen.time;
      cent = chosen.centrality;
      walk.push_back(node);
    }

    if (full_walks_) {
      std::vector<NodeId> result;
      result.reserve(walk.size() + 1);
      result.push_back(target);
      result.insert(result.end(), walk.begin(), walk.end());
      return result;
    }
    return {node, target};
  }

  void Update(NodeId source, NodeId target, int64_t time) {
    // Decay walks that terminated at target before this edge arrived.
    if (centrality_.count(target)) {
      centrality_[target] *= std::exp(c_coef_ * static_cast<double>(last_timestamp_[target] - time));
    } else {
      centrality_[target] = 0.0;
    }

    if (last_timestamp_.count(source)) {
      centrality_[source] *= std::exp(c_coef_ * static_cast<double>(last_timestamp_[source] - time));
      last_timestamp_[source] = time;
      CleanInEdges(source, time);
    }

    centrality_[target] += (Get(centrality_, source, 0.0) + 1.0) * beta_;

    graph_[target].push_back(InEdge{source, time, Get(centrality_, source, 0.0)});
    last_timestamp_[target] = time;
    CleanInEdges(target, time);
  }

  // Drops in-edges of `node` that arrived more than cutoff seconds ago.
  void CleanInEdges(NodeId node, int64_t current_time) {
    auto &edges = graph_[node];
    size_t index = 0;
    for (const auto &e : edges) {
      if (current_time - e.time < cutoff_) break;
      ++index;
    }
    if (index > 0) edges.erase(edges.begin(), edges.begin() + index);
  }
};

}  // namespace node2vec_alg
