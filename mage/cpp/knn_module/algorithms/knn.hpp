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

#include <mgp.hpp>

#include <fmt/format.h>
#include <omp.h>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <numeric>
#include <random>
#include <ranges>
#include <unordered_set>
#include <vector>

namespace knn_util {

// Configuration for KNN algorithm
struct KNNConfig {
  uint64_t top_k = 1;
  double similarity_cutoff = 0.0;
  double delta_threshold = 0.001;
  int max_iterations = 100;
  int random_seed = 42;  // the value is being set from the knn_module.cpp file
  double sample_rate = 0.5;
  int concurrency = 1;
  std::vector<std::string> node_properties;
};

struct KNNNeighbour {
  uint64_t neighbour_id;
  double similarity{0.0};
  bool is_new_neighbour{true};

  KNNNeighbour(const uint64_t id, double sim) : neighbour_id(id), similarity(sim) {}
};

struct WorseNeighbour {
  bool operator()(const KNNNeighbour &a, const KNNNeighbour &b) const { return a.similarity > b.similarity; }
};

}  // namespace knn_util

namespace knn_algs {

inline double CosineSimilarity(const std::vector<double> &vec1, const std::vector<double> &vec2, const double norm1,
                               const double norm2) {
  const double dot =
      std::transform_reduce(vec1.begin(), vec1.end(), vec2.begin(), 0.0, std::plus<>(), std::multiplies<>());

  const double denom = norm1 * norm2;
  if (denom < 1e-9) return 0.0;
  const auto cosine_distance = dot / denom;
  return (cosine_distance + 1) / 2.0;
}

// Structure to hold pre-loaded node data for efficient comparison
struct NodeData {
  mgp::Id node_id;
  std::vector<std::vector<double>> property_values;  // One vector per property
  std::vector<double> norms;                         // Norms for each property

  NodeData(const mgp::Node &n, const std::vector<std::vector<double>> &prop_values)
      : node_id(n.Id()), property_values(prop_values) {}
};

// Pre-load node properties into memory for efficient comparison
std::vector<NodeData> PreloadNodeData(const std::vector<mgp::Node> &nodes, const knn_util::KNNConfig &config) {
  std::vector<NodeData> node_data;
  node_data.reserve(nodes.size());
  const auto properties_size = config.node_properties.size();

  for (const auto &node : nodes) {
    // Collect all property values first
    std::vector<std::vector<double>> property_values(properties_size);

    // Load all properties into temporary vectors
    for (size_t prop_idx = 0; prop_idx < properties_size; ++prop_idx) {
      const std::string &prop_name = config.node_properties[prop_idx];
      mgp::Value prop_value = node.GetProperty(prop_name);
      std::vector<double> values;

      if (!prop_value.IsList()) {
        throw mgp::ValueException(
            fmt::format("Property {} must be a list of doubles for similarity calculation", prop_name));
      }

      const auto &list = prop_value.ValueList();
      const auto size = list.Size();
      values.reserve(size);

      for (size_t i = 0; i < size; ++i) {
        if (!list[i].IsDouble()) {
          throw mgp::ValueException(
              fmt::format("Property {} must be a list of doubles for similarity calculation", prop_name));
        }
        values.push_back(list[i].ValueDouble());
      }

      if (values.empty()) {
        throw mgp::ValueException(fmt::format("Invalid property values: empty lists for property {}", prop_name));
      }

      property_values[prop_idx] = values;
    }

    // Create node_info at the end with the final property_values
    node_data.emplace_back(node, std::move(property_values));
  }

  // Validate vector sizes
  if (node_data.size() > 1) {
    // Validate that all property vectors have the same size
    for (size_t prop_idx = 0; prop_idx < node_data[0].property_values.size(); ++prop_idx) {
      size_t expected_size = node_data[0].property_values[prop_idx].size();
      for (size_t i = 1; i < node_data.size(); ++i) {
        if (node_data[i].property_values[prop_idx].size() != expected_size) {
          throw mgp::ValueException("Property vectors must have the same size for similarity calculation");
        }
      }
    }
  }

  return node_data;
}

void PreloadNorms(std::vector<NodeData> &node_data, const knn_util::KNNConfig &config) {
#pragma omp parallel for
  for (size_t ni = 0; ni < node_data.size(); ++ni) {
    auto &node = node_data[ni];

    // Calculate norms for each property vector
    node.norms.resize(node.property_values.size(), 0.0);
    for (size_t i = 0; i < node.property_values.size(); ++i) {
      const auto &v = node.property_values[i];
      node.norms[i] = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0));
    }
  }
}

// Calculate similarity between pre-loaded node data
double CalculateNodeSimilarity(const NodeData &node1_data, const NodeData &node2_data) {
  double total_similarity = 0.0;
  const size_t num_properties = node1_data.property_values.size();

  for (size_t prop_idx = 0; prop_idx < num_properties; ++prop_idx) {
    const auto &values1 = node1_data.property_values[prop_idx];
    const auto &values2 = node2_data.property_values[prop_idx];

    // Use cosine similarity for each property
    double property_similarity =
        CosineSimilarity(values1, values2, node1_data.norms[prop_idx], node2_data.norms[prop_idx]);

    total_similarity += property_similarity;
  }

  // Return the mean of all property similarities
  return total_similarity / num_properties;
}

std::vector<uint64_t> SampleKDistinctWithOmitted(uint64_t n, uint64_t k, uint64_t omit, std::mt19937 &rng) {  // NOSONAR
  // [0, n) with ommited index
  auto filtered = std::views::iota(uint64_t{0}, n) | std::views::filter([omit](uint64_t x) { return x != omit; });
  std::vector<uint64_t> population(filtered.begin(), filtered.end());

  std::vector<uint64_t> result;
  result.reserve(k);
  std::ranges::sample(population, std::back_inserter(result), k, rng);

  return result;
}

std::vector<uint64_t> SampleKDistinct(uint64_t n, uint64_t k, std::mt19937 &rng) {  // NOSONAR
  // [0, n)
  auto filtered = std::views::iota(uint64_t{0}, n);
  std::vector<uint64_t> population(filtered.begin(), filtered.end());

  std::vector<uint64_t> result;
  result.reserve(k);
  std::ranges::sample(population, std::back_inserter(result), k, rng);

  return result;
}

std::vector<uint64_t> SampleFromVector(const std::vector<uint64_t> &indices, uint64_t k,
                                       std::mt19937 &rng) {  // NOSONAR
  auto sample = SampleKDistinct(indices.size(), k, rng);

  std::vector<uint64_t> result;
  result.reserve(sample.size());

  for (auto idx : sample) {
    result.push_back(indices[idx]);
  }

  return result;
}

std::vector<std::vector<knn_util::KNNNeighbour>> InitializeNeighborhoodLists(const std::vector<NodeData> &node_data,
                                                                             const knn_util::KNNConfig &config,
                                                                             size_t num_nodes, std::mt19937 &rng) {
  std::vector<std::vector<knn_util::KNNNeighbour>> neighbour_list(num_nodes);

#pragma omp parallel for
  for (size_t node_id = 0; node_id < num_nodes; node_id++) {
    std::vector<knn_util::KNNNeighbour> heap;
    // Sample K random neighbors for this node
    std::vector<uint64_t> sampled_neighbours = SampleKDistinctWithOmitted(num_nodes, config.top_k, node_id, rng);
    for (auto sample : sampled_neighbours) {
      double similarity = CalculateNodeSimilarity(node_data[node_id], node_data[sample]);
      heap.emplace_back(sample, similarity);
    }

    std::make_heap(heap.begin(), heap.end(), knn_util::WorseNeighbour{});

    // trick to ensure parallelization for OpenMP
    neighbour_list[node_id] = std::move(heap);
  }

  return neighbour_list;
}

std::vector<uint64_t> Union(const std::vector<uint64_t> &first, const std::vector<uint64_t> &second) {
  std::unordered_set<uint64_t> seen;
  seen.reserve(first.size() + second.size());
  seen.insert(first.begin(), first.end());
  seen.insert(second.begin(), second.end());

  return {seen.begin(), seen.end()};
}

uint64_t UpdateNN(std::vector<knn_util::KNNNeighbour> &neighborhood, uint64_t new_neighbour, double sim) {
  if (sim < neighborhood.front().similarity) {
    return 0;
  }

  for (const auto &nb : neighborhood) {
    if (nb.neighbour_id == new_neighbour) {
      return 0;
    }
  }

  std::pop_heap(neighborhood.begin(), neighborhood.end(), knn_util::WorseNeighbour{});
  neighborhood.pop_back();
  neighborhood.emplace_back(new_neighbour, sim);
  std::push_heap(neighborhood.begin(), neighborhood.end(), knn_util::WorseNeighbour{});
  return 1;
}

// Main KNN algorithm implementation
std::vector<std::tuple<mgp::Node, mgp::Node, double>> CalculateKNN(const mgp::Graph &graph,
                                                                   const knn_util::KNNConfig &config) {
  // we can't reserve here because it's an iterator
  std::vector<mgp::Node> nodes;
  for (const auto &node : graph.Nodes()) {
    nodes.push_back(node);
  }

  if (nodes.size() < 2) {
    // Need at least 2 nodes for similarity
    return {};
  }

  omp_set_dynamic(0);
  omp_set_num_threads(config.concurrency);

  // Pre-load node properties into memory for efficient comparison
  std::vector<NodeData> node_data = PreloadNodeData(nodes, config);
  PreloadNorms(node_data, config);

  const auto num_nodes = nodes.size();
  uint64_t sample_rate_size =
      std::max<uint64_t>(1ULL, static_cast<uint64_t>(config.sample_rate * static_cast<double>(config.top_k)));
  double update_termination_convergence = config.delta_threshold * num_nodes * config.top_k;
  std::mt19937 rng{static_cast<std::mt19937::result_type>(config.random_seed)};  // NOSONAR

  std::vector<std::vector<knn_util::KNNNeighbour>> B = InitializeNeighborhoodLists(node_data, config, num_nodes, rng);

  std::vector<std::vector<uint64_t>> olds(num_nodes);
  std::vector<std::vector<uint64_t>> news(num_nodes);
  std::vector<std::vector<uint64_t>> olds_rev(num_nodes);
  std::vector<std::vector<uint64_t>> news_rev(num_nodes);
  for (size_t v = 0; v < num_nodes; v++) {
    olds[v].reserve(config.top_k);
    news[v].reserve(config.top_k);
    olds_rev[v].reserve(config.top_k);
    news_rev[v].reserve(config.top_k);
  }

  for (auto iter = 0; iter < config.max_iterations; iter++) {
    for (size_t v = 0; v < num_nodes; v++) {
      olds[v].clear();
      news[v].clear();

      std::vector<uint64_t> new_neighbour_idx;
      new_neighbour_idx.reserve(config.top_k);
      for (size_t i = 0; i < B[v].size(); i++) {
        const auto &nb = B[v][i];
        if (!nb.is_new_neighbour) {
          // Collect all old items
          // Construct the reverse neighborhood as well for olds
          olds[v].push_back(nb.neighbour_id);
          olds_rev[nb.neighbour_id].push_back(v);
        } else {
          new_neighbour_idx.push_back(i);
        }
      }

      // For the new items, sample rho * K which are going to new, and mark them as old
      // Construct the reverse neighborhood as well for news
      std::vector<uint64_t> sampled_news = SampleFromVector(new_neighbour_idx, sample_rate_size, rng);
      for (const auto sampled_idx : sampled_news) {
        auto &sampled_new_neighbour = B[v][sampled_idx];
        news[v].push_back(sampled_new_neighbour.neighbour_id);
        news_rev[sampled_new_neighbour.neighbour_id].push_back(v);
        sampled_new_neighbour.is_new_neighbour = false;
      }
    }

    // logic after updates
    uint64_t updates = 0;

#pragma omp parallel for reduction(+ : updates) schedule(dynamic)
    for (size_t v = 0; v < num_nodes; v++) {
      const auto sampled_old_rev = SampleFromVector(olds_rev[v], sample_rate_size, rng);
      olds[v] = Union(olds[v], sampled_old_rev);
      const auto sampled_new_rev = SampleFromVector(news_rev[v], sample_rate_size, rng);
      news[v] = Union(news[v], sampled_new_rev);

      const auto news_size = news[v].size();
      for (size_t u1 = 0; u1 + 1 < news_size; u1++) {
        for (size_t u2 = u1 + 1; u2 < news_size; u2++) {
          double sim = CalculateNodeSimilarity(node_data[news[v][u1]], node_data[news[v][u2]]);
#pragma omp critical
          {
            updates += UpdateNN(B[news[v][u1]], news[v][u2], sim);
            updates += UpdateNN(B[news[v][u2]], news[v][u1], sim);
          }
        }
      }
      for (auto new_el : news[v]) {
        for (auto old_el : olds[v]) {
          if (new_el == old_el) continue;
          double sim = CalculateNodeSimilarity(node_data[new_el], node_data[old_el]);
#pragma omp critical
          {
            updates += UpdateNN(B[new_el], old_el, sim);
            updates += UpdateNN(B[old_el], new_el, sim);
          }
        }
      }
    }

    if (updates < update_termination_convergence) {
      break;
    }
  }

  double cutoff = config.similarity_cutoff;
  std::vector<std::tuple<mgp::Node, mgp::Node, double>> results;
  for (size_t v = 0; v < num_nodes; v++) {
    for (const auto &nb : B[v]) {
      if (nb.similarity < cutoff) continue;
      results.emplace_back(nodes[v], nodes[nb.neighbour_id], nb.similarity);
    }
  }

  return results;
}

}  // namespace knn_algs
