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

  // Modularity of a graph can be expressed as:
  //
  // Q = 1 / (2m) * sum_over_pairs_of_nodes[(Aij - ki * kj / 2m) * delta(ci, cj)]
  //
  // where m is the sum of all weights in the graph,
  //       Aij is the weight on edge that connects i and j (i=j for a self-loop)
  //       ki is the sum of weights incident to node i
  //       ci is the community of node i
  //       delta(a, b) is the Kronecker delta function.
  //
  // With some simple algebraic manipulations, we can transform the formula into:
  //
  // Q = sum_over_components[M * ((sum_over_pairs(Aij + M * ki * kj)))] =
  //   = sum_over_components[M * (sum_over_pairs(Aij) + M * sum_over_nodes^2(ki))] =
  //   = sum_over_components[M * (w_contrib(ci) + M * k_contrib^2(ci))]
  //
  // where M = 1 / (2m)
  //
  // Therefore, we could store for each community the following:
  //    * Weight contribution (w_contrib)
  //    * Weighted degree contribution (k_contrib)
  //
  // This allows us to efficiently remove a node from one community and insert
  // it into a community of its neighbour without the need to recalculate
  // modularity from scratch.

  std::unordered_map<uint32_t, double> w_contrib;
  std::unordered_map<uint32_t, double> k_contrib;

  for (uint32_t node_id = 0; node_id < graph->Size(); ++node_id) {
    k_contrib[graph->Community(node_id)] += graph->IncidentWeight(node_id);
    for (const auto &neigh : graph->Neighbours(node_id)) {
      uint32_t nxt_id = neigh.dest;
      double w = neigh.weight;
      if (graph->Community(node_id) == graph->Community(nxt_id))
        w_contrib[graph->Community(node_id)] += w;
    }
  }

  bool stable = false;
  double total_w = graph->TotalWeight();

  while (!stable) {
    stable = true;
    for (uint32_t node_id : p) {
      std::unordered_map<uint32_t, double> sum_w;
      double self_loop = 0;
      sum_w[graph->Community(node_id)] = 0;
      for (const auto &neigh : graph->Neighbours(node_id)) {
        uint32_t nxt_id = neigh.dest;
        double weight = neigh.weight;
        if (nxt_id == node_id) {
          self_loop += weight;
          continue;
        }
        sum_w[graph->Community(nxt_id)] += weight;
      }

      uint32_t my_c = graph->Community(node_id);

      uint32_t best_c = my_c;
      double best_dq = 0;

      for (const auto &p : sum_w) {
        if (p.first == my_c) continue;
        uint32_t nxt_c = p.first;
        double dq = 0;

        // contributions before swap (dq = d_after - d_before)
        for (uint32_t c : {my_c, nxt_c})
          dq -= w_contrib[c] - k_contrib[c] * k_contrib[c] / (2.0 * total_w);

        // leave the current community
        dq += (w_contrib[my_c] - 2.0 * sum_w[my_c] - self_loop) -
              (k_contrib[my_c] - graph->IncidentWeight(node_id)) *
                  (k_contrib[my_c] - graph->IncidentWeight(node_id)) /
                  (2.0 * total_w);

        // join a new community
        dq += (w_contrib[nxt_c] + 2.0 * sum_w[nxt_c] + self_loop) -
              (k_contrib[nxt_c] + graph->IncidentWeight(node_id)) *
                  (k_contrib[nxt_c] + graph->IncidentWeight(node_id)) /
                  (2.0 * total_w);

        if (dq > best_dq) {
          best_dq = dq;
          best_c = nxt_c;
        }
      }

      if (best_c != my_c) {
        graph->SetCommunity(node_id, best_c);
        w_contrib[my_c] -= 2.0 * sum_w[my_c] + self_loop;
        k_contrib[my_c] -= graph->IncidentWeight(node_id);
        w_contrib[best_c] += 2.0 * sum_w[best_c] + self_loop;
        k_contrib[best_c] += graph->IncidentWeight(node_id);
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
