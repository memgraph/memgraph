#include "algorithms/algorithms.hpp"

#include <algorithm>
#include <map>
#include <random>
#include <unordered_map>

#include <glog/logging.h>

namespace {

void OptimizeLocally(comdata::Graph *G) {
  // We will consider local optimizations uniformly at random.
  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<uint32_t> p(G->Size());
  std::iota(p.begin(), p.end(), 0);
  std::shuffle(p.begin(), p.end(), g);

  double total_w = G->TotalWeight();
  bool stable = false;
  while (!stable) {
    stable = true;
    for (uint32_t node_id : p) {
      std::unordered_map<uint32_t, double> c_contrib;
      c_contrib[G->Community(node_id)] = 0;
      for (const auto &neigh : G->Neighbours(node_id)) {
        uint32_t nxt_id = neigh.dest;
        double weight = neigh.weight;
        double contrib = weight - G->IncidentWeight(node_id) *
                                  G->IncidentWeight(nxt_id) / (2 * total_w);
        c_contrib[G->Community(nxt_id)] += contrib;
      }

      auto best_c = std::max_element(c_contrib.begin(), c_contrib.end(),
                                     [](const std::pair<uint32_t, double> &p1,
                                        const std::pair<uint32_t, double> &p2) {
                                       return p1.second < p2.second;
                                     });

      if (best_c->second - c_contrib[G->Community(node_id)] > 0) {
        G->SetCommunity(node_id, best_c->first);
        stable = false;
      }
    }
  }
}

} // anonymous namespace

namespace algorithms {

void Louvain(comdata::Graph *G) {
  OptimizeLocally(G);

  // Collapse the locally optimized graph.
  uint32_t collapsed_nodes = G->NormalizeCommunities();
  if (collapsed_nodes == G->Size()) return;
  comdata::Graph collapsed_G(collapsed_nodes);
  std::map<std::pair<uint32_t, uint32_t>, double> collapsed_edges;

  for (uint32_t node_id = 0; node_id < G->Size(); ++node_id) {
    std::unordered_map<uint32_t, double> edges;
    for (const auto &neigh : G->Neighbours(node_id)) {
      uint32_t nxt_id = neigh.dest;
      double weight = neigh.weight;
      if (G->Community(nxt_id) < G->Community(node_id)) continue;
      edges[G->Community(nxt_id)] += weight;
    }
    for (const auto &neigh : edges) {
      uint32_t a = std::min(G->Community(node_id), neigh.first);
      uint32_t b = std::max(G->Community(node_id), neigh.first);
      collapsed_edges[{a, b}] += neigh.second;
    }
  }

  for (const auto &p : collapsed_edges)
    collapsed_G.AddEdge(p.first.first, p.first.second, p.second);

  // Repeat until no local optimizations can be found.
  Louvain(&collapsed_G);

  // Propagate results from collapsed graph.
  for (uint32_t node_id = 0; node_id < G->Size(); ++node_id)
    G->SetCommunity(node_id, collapsed_G.Community(G->Community(node_id)));
  G->NormalizeCommunities();
}

} // namespace algorithms
