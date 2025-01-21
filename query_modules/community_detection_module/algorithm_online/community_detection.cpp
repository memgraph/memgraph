// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include "community_detection.hpp"

#include <algorithm>
#include <cmath>
#include <set>

namespace LabelRankT {
/* #region helper_methods */
///@brief Checks if unordered_set is subset of another unordered_set.
///
///@tparam T -- set element data type
///
///@param a -- subset candidate
///@param b -- set against which the check is performed
///
///@return -- whether a is subset of b
template <class T>
bool IsSubset(const std::unordered_set<T> &a, const std::unordered_set<T> &b) {
  return std::all_of(a.begin(), a.end(), [&](const T &value) { return b.find(value) != b.end(); });
}

std::vector<std::uint64_t> LabelRankT::NodesMemgraphIDs() const {
  std::vector<std::uint64_t> nodes;
  nodes.reserve(graph->Nodes().size());

  for (const auto node : graph->Nodes()) {
    nodes.push_back(graph->GetMemgraphNodeId(node.id));
  }

  return nodes;
}

std::unordered_set<std::uint64_t> LabelRankT::InNeighborsMemgraphIDs(std::uint64_t node_id) const {
  std::unordered_set<std::uint64_t> neighbors;

  const auto neighbor_set = is_directed ? graph->InNeighbours(graph->GetInnerNodeId(node_id))
                                        : graph->Neighbours(graph->GetInnerNodeId(node_id));
  for (const auto node_i : neighbor_set) {
    neighbors.insert(graph->GetMemgraphNodeId(node_i.node_id));
  }

  return neighbors;
}

void LabelRankT::SetStructures(std::uint64_t node_id) {
  times_updated[node_id] = 0;
  std::unordered_map<std::uint64_t, double> node_label_Ps;

  const auto in_neighbors = InNeighborsMemgraphIDs(node_id);

  double sum_w_i = w_selfloop;
  for (const auto node_j_id : in_neighbors) {
    sum_w_i += GetTotalWeightBetween(node_j_id, node_id);
  }

  // add self-loop
  node_label_Ps[node_id] = w_selfloop / sum_w_i;

  // add other edges
  for (const auto node_j_id : in_neighbors) {
    node_label_Ps[node_j_id] += GetTotalWeightBetween(node_j_id, node_id) / sum_w_i;
  }

  label_Ps[node_id] = std::move(node_label_Ps);
  sum_w[node_id] = sum_w_i;
}

void LabelRankT::RemoveDeletedNodes(const std::unordered_set<std::uint64_t> &deleted_nodes_ids) {
  for (const auto node_id : deleted_nodes_ids) {
    label_Ps.erase(node_id);
    sum_w.erase(node_id);
    times_updated.erase(node_id);
  }
}

double LabelRankT::GetTotalWeightBetween(std::uint64_t from_node_id, std::uint64_t to_node_id) const {
  double total_weight = 0;
  for (const auto edge_id :
       graph->GetEdgesBetweenNodes(graph->GetInnerNodeId(from_node_id), graph->GetInnerNodeId(to_node_id))) {
    const auto edge = graph->GetEdge(edge_id);

    // edge direction check
    if (is_directed &&
        (graph->GetMemgraphNodeId(edge.from) != from_node_id || graph->GetMemgraphNodeId(edge.to) != to_node_id))
      continue;

    total_weight += GetWeight(edge_id);
  }

  return total_weight;
}

double LabelRankT::GetWeight(std::uint64_t edge_id) const {
  if (!is_weighted) return DEFAULT_WEIGHT;

  return graph->GetWeight(edge_id);
}

void LabelRankT::ResetTimesUpdated() {
  for (auto &[_, count] : times_updated) {
    count = 0;
  }
}

std::int64_t LabelRankT::NodeLabel(std::uint64_t node_id) {
  std::int64_t node_label = -1;
  double max_P = 0;

  for (const auto [label, P] : label_Ps[node_id]) {
    if (P > max_P || (P == max_P && static_cast<std::int64_t>(label) < node_label)) {
      max_P = P;
      node_label = label;
    }
  }

  return node_label;
}

std::unordered_map<std::uint64_t, std::int64_t> LabelRankT::AllLabels() {
  std::unordered_map<std::uint64_t, std::int64_t> labels;

  std::set<std::int64_t> labels_ordered;
  std::unordered_map<std::int64_t, std::int64_t> lookup;

  for (const auto &[node, _] : label_Ps) {
    labels_ordered.insert(NodeLabel(node));
  }

  int64_t label_i = 1;
  labels_ordered.erase(-1);
  for (const auto label : labels_ordered) {
    lookup[label] = label_i;
    label_i++;
  }
  lookup[-1] = -1;

  for (const auto &[node, _] : label_Ps) {
    labels.insert({node, lookup[NodeLabel(node)]});
  }

  graph.reset();

  return labels;
}

std::unordered_set<std::uint64_t> LabelRankT::MostProbableLabels(std::uint64_t node_id) {
  double max_P = 0;

  for (const auto [label, P] : label_Ps[node_id]) {
    if (P > max_P) max_P = P;
  }

  std::unordered_set<std::uint64_t> most_probable_labels;

  for (const auto [label, P] : label_Ps[node_id]) {
    if (P == max_P) most_probable_labels.insert(label);
  }

  return most_probable_labels;
}
/* #endregion */

/* #region label_propagation_steps */
bool LabelRankT::DistinctEnough(std::uint64_t node_i_id, double similarity_threshold) {
  const auto labels_i = MostProbableLabels(node_i_id);
  std::uint64_t label_similarity = 0;

  const auto in_neighbors_ids = InNeighborsMemgraphIDs(node_i_id);
  for (const auto node_j_id : in_neighbors_ids) {
    const auto labels_j = MostProbableLabels(node_j_id);
    if (IsSubset(labels_i, labels_j)) label_similarity++;
  }

  int node_i_in_degree = in_neighbors_ids.size();

  return label_similarity <= node_i_in_degree * similarity_threshold;
}

std::unordered_map<std::uint64_t, double> LabelRankT::Propagate(std::uint64_t node_i_id) {
  std::unordered_map<std::uint64_t, double> new_label_Ps_i;

  // propagate own probabilities (handle self-loops)
  for (const auto [label, P] : label_Ps[node_i_id]) {
    new_label_Ps_i.insert({label, w_selfloop / sum_w[node_i_id] * P});
  }

  // propagate neighbors’ probabilities
  for (const auto node_j_id : InNeighborsMemgraphIDs(node_i_id)) {
    const double contribution = GetTotalWeightBetween(node_j_id, node_i_id) / sum_w[node_i_id];
    for (const auto [label, P] : label_Ps[node_j_id]) {
      new_label_Ps_i[label] += contribution * P;
    }
  }

  return new_label_Ps_i;
}

void LabelRankT::Inflate(std::unordered_map<std::uint64_t, double> &node_label_Ps, double exponent) const {
  double sum_Ps = 0;

  for (const auto [label, _] : node_label_Ps) {
    const double inflated_node_label_Ps = pow(node_label_Ps[label], exponent);
    node_label_Ps[label] = inflated_node_label_Ps;
    sum_Ps += inflated_node_label_Ps;
  }

  for (const auto [label, _] : node_label_Ps) {
    node_label_Ps[label] /= sum_Ps;
  }
}

void LabelRankT::Cutoff(std::unordered_map<std::uint64_t, double> &node_label_Ps, double min_value) const {
  for (auto node_label_P = node_label_Ps.begin(); node_label_P != node_label_Ps.end();) {
    auto P = node_label_P->second;
    if (P < min_value)
      node_label_P = node_label_Ps.erase(node_label_P);
    else
      node_label_P++;
  }
}

std::pair<bool, std::uint64_t> LabelRankT::Iteration(const std::vector<uint64_t> &nodes_memgraph_ids, bool incremental,
                                                     const std::unordered_set<std::uint64_t> &changed_nodes,
                                                     const std::unordered_set<std::uint64_t> &to_delete) {
  std::unordered_map<std::uint64_t, std::unordered_map<std::uint64_t, double>> updated_label_Ps;

  bool none_updated = true;
  std::uint64_t most_updates = 0;

  for (const auto node_id : nodes_memgraph_ids) {
    if (incremental) {
      const bool was_updated = changed_nodes.count(node_id) > 0;
      const bool was_deleted = to_delete.count(node_id) > 0;

      if (!was_updated || was_deleted) continue;
    }

    // node selection (a.k.a. conditional update)
    if (!DistinctEnough(node_id, similarity_threshold)) continue;
    none_updated = false;

    // label propagation
    std::unordered_map<std::uint64_t, double> updated_node_label_Ps = Propagate(node_id);

    // inflation
    Inflate(updated_node_label_Ps, exponent);

    // cutoff
    Cutoff(updated_node_label_Ps, min_value);

    updated_label_Ps.insert({node_id, std::move(updated_node_label_Ps)});
  }

  for (const auto &[node, updated_node_label_Ps] : updated_label_Ps) {
    label_Ps[node] = updated_node_label_Ps;
    times_updated[node]++;

    if (times_updated[node] > most_updates) most_updates = times_updated[node];
  }

  return {none_updated, most_updates};
}
/* #endregion */

std::unordered_map<std::uint64_t, std::int64_t> LabelRankT::CalculateLabels(
    std::unique_ptr<mg_graph::Graph<>> &&graph, const std::unordered_set<std::uint64_t> &changed_nodes,
    const std::unordered_set<std::uint64_t> &to_delete, bool persist) {
  this->graph = std::move(graph);
  const auto nodes_memgraph_ids = NodesMemgraphIDs();

  const bool incremental = changed_nodes.size() >= 1;
  if (incremental) {
    RemoveDeletedNodes(to_delete);
    for (const auto node_id : changed_nodes) {
      SetStructures(node_id);
    }
  } else {
    label_Ps.clear();
    sum_w.clear();
    times_updated.clear();
    for (const auto node_id : nodes_memgraph_ids) {
      SetStructures(node_id);
    }
  }

  for (std::uint64_t i = 0; i < max_iterations; i++) {
    const auto [none_updated, most_updates] = Iteration(nodes_memgraph_ids, incremental, changed_nodes, to_delete);
    if (none_updated || most_updates > max_updates) break;
  }

  if (persist) calculated = true;
  ResetTimesUpdated();

  return AllLabels();
}

std::unordered_map<std::uint64_t, std::int64_t> LabelRankT::GetLabels(std::unique_ptr<mg_graph::Graph<>> &&graph) {
  if (!calculated) return CalculateLabels(std::move(graph), {}, {}, false);

  return AllLabels();
}

std::unordered_map<std::uint64_t, std::int64_t> LabelRankT::SetLabels(std::unique_ptr<mg_graph::Graph<>> &&graph,
                                                                      bool is_directed, bool is_weighted,
                                                                      double similarity_threshold, double exponent,
                                                                      double min_value, std::string weight_property,
                                                                      double w_selfloop, std::uint64_t max_iterations,
                                                                      std::uint64_t max_updates) {
  this->is_directed = is_directed;
  this->is_weighted = is_weighted;
  this->similarity_threshold = similarity_threshold;
  this->exponent = exponent;
  this->min_value = min_value;
  this->weight_property = std::move(weight_property);
  this->w_selfloop = w_selfloop;
  this->max_iterations = max_iterations;
  this->max_updates = max_updates;

  return CalculateLabels(std::move(graph));
}

std::unordered_map<std::uint64_t, std::int64_t> LabelRankT::UpdateLabels(
    std::unique_ptr<mg_graph::Graph<>> &&graph, const std::vector<std::uint64_t> &modified_nodes,
    const std::vector<std::pair<std::uint64_t, std::uint64_t>> &modified_edges,
    const std::vector<std::uint64_t> &deleted_nodes,
    const std::vector<std::pair<std::uint64_t, std::uint64_t>> &deleted_edges) {
  if (!calculated) return CalculateLabels(std::move(graph), {}, {}, false);

  std::unordered_set<std::uint64_t> changed_nodes(modified_nodes.begin(), modified_nodes.end());
  std::unordered_set<std::uint64_t> to_delete(deleted_nodes.begin(), deleted_nodes.end());

  for (const auto &[from, to] : modified_edges) {
    changed_nodes.insert(from);
    changed_nodes.insert(to);
  }

  for (const auto &[from, to] : deleted_edges) {
    if (to_delete.count(from) == 0) changed_nodes.insert(from);
    if (to_delete.count(to) == 0) changed_nodes.insert(to);
  }

  return CalculateLabels(std::move(graph), changed_nodes, to_delete);
}
}  // namespace LabelRankT
