#include "utils.hpp"

#include <random>

comdata::Graph BuildGraph(
    uint32_t nodes, std::vector<std::tuple<uint32_t, uint32_t, double>> edges) {
  comdata::Graph G(nodes);
  for (auto &edge : edges)
    G.AddEdge(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
  return G;
}

comdata::Graph GenRandomUnweightedGraph(uint32_t nodes, uint32_t edges) {
  auto seed =
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 rng(seed);
  std::uniform_int_distribution<uint32_t> dist(0, nodes - 1);
  std::set<std::tuple<uint32_t, uint32_t, double>> E;
  for (uint32_t i = 0; i < edges; ++i) {
    int u;
    int v;
    do {
      u = dist(rng);
      v = dist(rng);
      if (u > v) std::swap(u, v);
    } while (u == v || E.find({u, v, 1}) != E.end());
    E.insert({u, v, 1});
  }
  return BuildGraph(nodes, std::vector<std::tuple<uint32_t, uint32_t, double>>(
                               E.begin(), E.end()));
}

