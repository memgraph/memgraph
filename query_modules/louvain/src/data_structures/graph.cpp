#include "data_structures/graph.hpp"

#include <exception>
#include <numeric>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace comdata {

Graph::Graph(uint32_t n_nodes) : n_nodes_(n_nodes), total_w_(0) {
  adj_list_.resize(n_nodes, {});
  inc_w_.resize(n_nodes, 0);

  // each node starts as its own separate community.
  community_.resize(n_nodes);
  std::iota(community_.begin(), community_.end(), 0);
}

uint32_t Graph::Size() const { return n_nodes_; }

uint32_t Graph::Community(uint32_t node) const { return community_.at(node); }

void Graph::SetCommunity(uint32_t node, uint32_t c) { community_.at(node) = c; }

uint32_t Graph::NormalizeCommunities() {
  std::set<uint32_t> c_id(community_.begin(), community_.end());
  std::unordered_map<uint32_t, uint32_t> cmap;
  uint32_t id = 0;
  for (uint32_t c : c_id) {
    cmap[c] = id;
    ++id;
  }
  for (uint32_t node_id = 0; node_id < n_nodes_; ++node_id)
    community_[node_id] = cmap[community_[node_id]];
  return id;
}

void Graph::AddEdge(uint32_t node1, uint32_t node2, double weight) {
  if (node1 >= n_nodes_ || node2 >= n_nodes_)
    throw std::out_of_range("Node index out of range");
  if (weight <= 0) throw std::out_of_range("Weights must be positive");
  if (edges_.find({node1, node2}) != edges_.end())
    throw std::invalid_argument("Edge already exists");

  edges_.emplace(node1, node2);
  edges_.emplace(node2, node1);

  total_w_ += weight;

  adj_list_[node1].emplace_back(node2, weight);
  inc_w_[node1] += weight;

  if (node1 != node2) {
    adj_list_[node2].emplace_back(node1, weight);
    inc_w_[node2] += weight;
  }
}

uint32_t Graph::Degree(uint32_t node) const {
  return static_cast<uint32_t>(adj_list_.at(node).size());
}

double Graph::IncidentWeight(uint32_t node) const { return inc_w_.at(node); }

double Graph::TotalWeight() const { return total_w_; }

double Graph::Modularity() const {
  double ret = 0;
  // Since all weights should be positive, this implies that our graph has
  // no edges.
  if (total_w_ == 0) return 0;

  std::unordered_map<uint32_t, double> weight_c;
  std::unordered_map<uint32_t, double> degree_c;

  for (uint32_t i = 0; i < n_nodes_; ++i) {
    degree_c[Community(i)] += IncidentWeight(i);
    for (const auto &neigh : adj_list_[i]) {
      uint32_t j = neigh.dest;
      double w = neigh.weight;
      if (Community(i) != Community(j)) continue;
      weight_c[Community(i)] += w;
    }
  }

  for (const auto &p : degree_c)
    ret += weight_c[p.first] - (p.second * p.second) / (2 * total_w_);

  ret /= 2 * total_w_;
  return ret;
}

const std::vector<Neighbour> &Graph::Neighbours(uint32_t node) const {
  return adj_list_.at(node);
}

}  // namespace comdata
