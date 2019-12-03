#include "algorithms/algorithms.hpp"

#include <algorithm>
#include <map>
#include <random>
#include <unordered_map>

namespace {

void OptimizeLocally(comdata::Graph *graph) {
  // We will consider local optimizations uniformly at random.
  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<uint32_t> p(graph->Size());
  std::iota(p.begin(), p.end(), 0);
  std::shuffle(p.begin(), p.end(), g);

  double total_w = graph->TotalWeight();
  bool stable = false;
  while (!stable) {
    stable = true;
    for (uint32_t node_id : p) {
      std::unordered_map<uint32_t, double> c_contrib;
      c_contrib[graph->Community(node_id)] = 0;
      for (const auto &neigh : graph->Neighbours(node_id)) {
        uint32_t nxt_id = neigh.dest;
        double weight = neigh.weight;
        double contrib = weight - graph->IncidentWeight(node_id) *
                                  graph->IncidentWeight(nxt_id) / (2 * total_w);
        c_contrib[graph->Community(nxt_id)] += contrib;
      }

      auto best_c = std::max_element(c_contrib.begin(), c_contrib.end(),
                                     [](const std::pair<uint32_t, double> &p1,
                                        const std::pair<uint32_t, double> &p2) {
                                       return p1.second < p2.second;
                                     });

      if (best_c->second - c_contrib[graph->Community(node_id)] > 0) {
        graph->SetCommunity(node_id, best_c->first);
        stable = false;
      }
    }
  }
}

} // anonymous namespace

namespace algorithms {

void Louvain(comdata::Graph *graph) {
  OptimizeLocally(graph);

  // Collapse the locally optimized graph.
  uint32_t collapsed_nodes = graph->NormalizeCommunities();
  if (collapsed_nodes == graph->Size()) return;
  comdata::Graph collapsed_graph(collapsed_nodes);
  std::map<std::pair<uint32_t, uint32_t>, double> collapsed_edges;

  for (uint32_t node_id = 0; node_id < graph->Size(); ++node_id) {
    std::unordered_map<uint32_t, double> edges;
    for (const auto &neigh : graph->Neighbours(node_id)) {
      uint32_t nxt_id = neigh.dest;
      double weight = neigh.weight;
      if (graph->Community(nxt_id) < graph->Community(node_id)) continue;
      edges[graph->Community(nxt_id)] += weight;
    }
    for (const auto &neigh : edges) {
      uint32_t a = std::min(graph->Community(node_id), neigh.first);
      uint32_t b = std::max(graph->Community(node_id), neigh.first);
      collapsed_edges[{a, b}] += neigh.second;
    }
  }

  for (const auto &p : collapsed_edges)
    collapsed_graph.AddEdge(p.first.first, p.first.second, p.second);

  // Repeat until no local optimizations can be found.
  Louvain(&collapsed_graph);

  // Propagate results from collapsed graph.
  for (uint32_t node_id = 0; node_id < graph->Size(); ++node_id) {
    graph->SetCommunity(node_id,
                        collapsed_graph.Community(graph->Community(node_id)));
  }

  graph->NormalizeCommunities();
}

} // namespace algorithms
