// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mgp.hpp>

#include <cmath>
#include <set>
#include <unordered_set>

namespace node_similarity_util {

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(std::pair<T1, T2> const &pair) const {
    std::size_t h1 = std::hash<T1>()(pair.first);
    std::size_t h2 = std::hash<T2>()(pair.second);

    return h1 ^ h2;
  }
};

enum Similarity { jaccard, overlap, cosine };

}  // namespace node_similarity_util

namespace node_similarity_algs {

// Lambda functions for computing concrete similarities
/*
Calculates Jaccard similarity between two nodes based on their neighbours.
ns1 are neighbours of the first node
ns2 are neighbours of the second node
*/

double JaccardFunc(const std::set<uint64_t> &ns1, const std::set<uint64_t> &ns2) {
  std::set<uint64_t> elem_union, elem_intersection;
  std::set_intersection(ns1.begin(), ns1.end(), ns2.begin(), ns2.end(),
                        std::inserter(elem_intersection, elem_intersection.begin()));
  std::set_union(ns1.begin(), ns1.end(), ns2.begin(), ns2.end(), std::inserter(elem_union, elem_union.begin()));
  if (elem_union.size() == 0) {
    return 0.0;
  }
  return elem_intersection.size() / (double)elem_union.size();
}

/*
Calculates overlap function between two set of neighbours.
ns1 are neighbours of the first node.
ns2 are neighbours of the second node.
*/
double OverlapFunc(const std::set<uint64_t> &ns1, const std::set<uint64_t> &ns2) {
  std::set<uint64_t> elem_intersection;
  std::set_intersection(ns1.begin(), ns1.end(), ns2.begin(), ns2.end(),
                        std::inserter(elem_intersection, elem_intersection.begin()));
  double denominator = std::min(ns1.size(), ns2.size());
  if (denominator < 1e-9) {
    return 0.0;
  }
  return elem_intersection.size() / denominator;
}

double CosineFunc(const mgp::List &prop1, const mgp::List &prop2) {
  int size = prop1.Size();
  double similarity = 0.0, node1_sum = 0.0, node2_sum = 0.0;
  for (int i = 0; i < size; ++i) {
    double val1 = prop1[i].ValueDouble(), val2 = prop2[i].ValueDouble();
    similarity += val1 * val2;
    node1_sum += val1 * val1;
    node2_sum += val2 * val2;
  }
  double denominator = sqrt(node1_sum) * sqrt(node2_sum);
  if (denominator < 1e-9) {
    return 0.0;
  }
  return similarity / denominator;
}

/*
Calculates cosine similarity function between two nodes for a given property.
*/
double CosineFuncWrapper(const mgp::Node &node1, const mgp::Node &node2, const std::string &property) {
  const auto &prop1_it = node1.GetProperty(property);
  const auto &prop2_it = node2.GetProperty(property);
  if (prop1_it.IsNull() || prop2_it.IsNull()) {
    throw mgp::ValueException("All nodes should have property " + property);
  }
  const auto &prop1 = prop1_it.ValueList();
  const auto &prop2 = prop2_it.ValueList();
  int size1 = prop1.Size(), size2 = prop2.Size();
  if (size1 != size2) {
    throw mgp::ValueException("The vectors should be of the same size.");
  }
  return CosineFunc(prop1, prop2);
}

/*
Extract node neighbours.
*/
std::set<uint64_t> GetNeighbors(std::unordered_map<uint64_t, std::set<uint64_t>> &all_node_neighbors,
                                const mgp::Node &node) {
  uint64_t node_id = node.Id().AsUint();
  const auto &result_it = all_node_neighbors.find(node_id);
  if (result_it != all_node_neighbors.end()) {
    return result_it->second;
  }
  all_node_neighbors[node_id] = std::set<uint64_t>();
  auto &ns = all_node_neighbors[node_id];
  const auto &rels = node.OutRelationships();
  for (const auto &rel : rels) {
    ns.insert(rel.To().Id().AsUint());
  }
  return ns;
}

/*
Calculates similarity between pairs of nodes given by src_nodes and dst_nodes.
*/
std::vector<std::tuple<mgp::Node, mgp::Node, double>> CalculateSimilarityPairwise(
    const mgp::List &src_nodes, const mgp::List &dst_nodes, node_similarity_util::Similarity similarity_mode,
    const std::string &property = "") {
  if (src_nodes.Size() != dst_nodes.Size()) {
    throw mgp::ValueException("The node lists must be the same length.");
  }
  int num_nodes = src_nodes.Size();
  std::vector<std::tuple<mgp::Node, mgp::Node, double>> results;
  std::unordered_map<uint64_t, std::set<uint64_t>> all_node_neighbors;
  for (int i = 0; i < num_nodes; ++i) {
    const mgp::Node &src_node = src_nodes[i].ValueNode(), &dst_node = dst_nodes[i].ValueNode();
    double similarity = 0.0;
    if (similarity_mode == node_similarity_util::Similarity::cosine) {
      similarity = node_similarity_algs::CosineFuncWrapper(src_node, dst_node, property);
      results.emplace_back(src_node, dst_node, similarity);
      continue;
    }
    const auto &ns1 = GetNeighbors(all_node_neighbors, src_node);
    const auto &ns2 = GetNeighbors(all_node_neighbors, dst_node);
    switch (similarity_mode) {
      case node_similarity_util::Similarity::jaccard:
        similarity = node_similarity_algs::JaccardFunc(ns1, ns2);
        break;
      case node_similarity_util::Similarity::overlap:
        similarity = node_similarity_algs::OverlapFunc(ns1, ns2);
        break;
      default:
        break;
    }
    results.emplace_back(src_node, dst_node, similarity);
  }
  return results;
}

/*
Calculates similarity between all pairs of nodes, in a cartesian mode.
*/
std::vector<std::tuple<mgp::Node, mgp::Node, double>> CalculateSimilarityCartesian(
    const mgp::Graph &graph, node_similarity_util::Similarity similarity_mode, const std::string &property = "") {
  std::unordered_set<std::pair<uint64_t, uint64_t>, node_similarity_util::pair_hash> visited_node_pairs;
  std::unordered_map<uint64_t, std::set<uint64_t>> all_node_neighbors;
  std::vector<std::tuple<mgp::Node, mgp::Node, double>> results;
  for (const auto &node1 : graph.Nodes()) {
    uint64_t node1_id = node1.Id().AsUint();
    const std::set<uint64_t> &ns1 = (similarity_mode != node_similarity_util::Similarity::cosine)
                                        ? GetNeighbors(all_node_neighbors, node1)
                                        : std::set<uint64_t>();
    for (const auto &node2 : graph.Nodes()) {
      uint64_t node2_id = node2.Id().AsUint();
      if (node1 == node2 || visited_node_pairs.count(std::make_pair<>(node2_id, node1_id))) {
        continue;
      }
      visited_node_pairs.emplace(node1_id, node2_id);
      double similarity = 0.0;
      if (similarity_mode == node_similarity_util::Similarity::cosine) {
        similarity = node_similarity_algs::CosineFuncWrapper(node1, node2, property);
        results.emplace_back(node1, node2, similarity);
        continue;
      }
      const auto &ns2 = GetNeighbors(all_node_neighbors, node2);
      switch (similarity_mode) {
        case node_similarity_util::Similarity::jaccard:
          similarity = node_similarity_algs::JaccardFunc(ns1, ns2);
          break;
        case node_similarity_util::Similarity::overlap:
          similarity = node_similarity_algs::OverlapFunc(ns1, ns2);
          break;
        default:
          break;
      }
      results.emplace_back(node1, node2, similarity);
    }
  }
  return results;
}

}  // namespace node_similarity_algs
