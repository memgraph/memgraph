#pragma once

#include <functional>
#include <random>
#include <unordered_map>

#include "cppitertools/itertools.hpp"
#include "json/json.hpp"

#include "storage/gid.hpp"
#include "storage/property_value.hpp"
#include "utils/string.hpp"

#include "value_generator.hpp"

namespace snapshot_generation {

nlohmann::json GetWithDefault(const nlohmann::json &object,
                              const std::string &key,
                              const nlohmann::json &default_value) {
  const auto &found = object.find(key);
  if (found == object.end()) return default_value;
  return *found;
}

struct Node {
  gid::Gid gid;
  std::vector<std::string> labels;
  std::unordered_map<std::string, PropertyValue> props;
  std::vector<gid::Gid> in_edges;
  std::vector<gid::Gid> out_edges;
};

struct Edge {
  gid::Gid gid;
  gid::Gid from;
  gid::Gid to;
  std::string type;
  std::unordered_map<std::string, PropertyValue> props;
};

/// Helper class for tracking info about the generated graph.
class GraphState {
 public:
  explicit GraphState(int num_workers)
      : num_workers_(num_workers),
        worker_nodes_(num_workers),
        worker_edges_(num_workers) {
    for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
      edge_generators_.emplace_back(
          std::make_unique<gid::Generator>(worker_id));
      node_generators_.emplace_back(
          std::make_unique<gid::Generator>(worker_id));
    }
  }

  int NumWorkers() { return num_workers_; }

  int64_t NumNodesOnWorker(int worker_id) {
    return node_generators_[worker_id]->LocalCount();
  }

  int64_t NumEdgesOnWorker(int worker_id) {
    return edge_generators_[worker_id]->LocalCount();
  }

  auto &NodesWithLabel(const std::string &label, int worker_id) {
    return worker_nodes_[worker_id][label];
  }

  auto NodesWithLabel(const std::string &label) {
    return iter::chain.from_iterable(
        iter::imap([ this, label ](int worker_id) -> auto & {
          return NodesWithLabel(label, worker_id);
        },
                   iter::range(num_workers_)));
  }

  auto NodesOnWorker(int worker_id) {
    return iter::chain.from_iterable(
        iter::imap([](auto &p) -> auto & { return p.second; },
                   worker_nodes_[worker_id]));
  }

  auto EdgesOnWorker(int worker_id) {
    return iter::chain.from_iterable(
        iter::imap([](auto &p) -> auto & { return p.second; },
                   worker_edges_[worker_id]));
  }

  gid::Gid &RandomNode(const std::string &label, int worker_id) {
    CHECK(0 <= worker_id && worker_id < (int)worker_nodes_.size())
        << "Worker ID should be between 0 and " << worker_nodes_.size() - 1;
    auto &label_nodes = worker_nodes_[worker_id];
    auto found = label_nodes.find(label);
    CHECK(found != label_nodes.end()) << "Label not found";
    return found->second[rand_(gen_) * found->second.size()];
  }

  gid::Gid &RandomNode(const std::string &label) {
    return RandomNode(label, rand_(gen_) * worker_nodes_.size());
  }

  gid::Gid &RandomNodeOnOtherWorker(const std::string &label, int worker_id) {
    int worker_id2 = rand_(gen_) * (worker_nodes_.size() - 1);
    if (worker_id2 >= worker_id) ++worker_id2;
    return RandomNode(label, worker_id2);
  }

  gid::Gid CreateNode(
      int worker_id, const std::vector<std::string> &labels,
      const std::unordered_map<std::string, PropertyValue> &props) {
    auto node_gid =
        node_generators_[worker_id]->Next(std::experimental::nullopt);
    nodes_[node_gid] = {node_gid, labels, props, {}, {}};

    for (const auto &label : labels) {
      worker_nodes_[worker_id][label].push_back(node_gid);
    }

    return node_gid;
  }

  gid::Gid CreateEdge(
      gid::Gid from, gid::Gid to, const std::string &type,
      const std::unordered_map<std::string, PropertyValue> &props) {
    int worker_id = gid::CreatorWorker(from);
    auto edge_gid =
        edge_generators_[worker_id]->Next(std::experimental::nullopt);
    nodes_[from].out_edges.emplace_back(edge_gid);
    nodes_[to].in_edges.emplace_back(edge_gid);
    edges_[edge_gid] = Edge{edge_gid, from, to, type, props};
    worker_edges_[worker_id][type].push_back(edge_gid);
    return edge_gid;
  }

  auto &GetNode(gid::Gid gid) { return nodes_[gid]; }
  auto &GetEdge(gid::Gid gid) { return edges_[gid]; }
  auto &GetNodes() { return nodes_; }
  auto &GetEdges() { return edges_; }

  void CreateIndex(std::string label, std::string property) {
    indices_.emplace_back(std::move(label));
    indices_.emplace_back(std::move(property));
  }

  auto &Indices() { return indices_; }

 private:
  typedef std::unordered_map<std::string, std::vector<gid::Gid>> LabelGid;

  int num_workers_;

  std::vector<std::string> indices_;

  std::vector<LabelGid> worker_nodes_;
  std::vector<LabelGid> worker_edges_;

  std::unordered_map<gid::Gid, Node> nodes_;
  std::unordered_map<gid::Gid, Edge> edges_;

  std::vector<std::unique_ptr<gid::Generator>> edge_generators_;
  std::vector<std::unique_ptr<gid::Generator>> node_generators_;

  std::mt19937 gen_{std::random_device{}()};
  std::uniform_real_distribution<> rand_{0.0, 1.0};
};

int Worker(gid::Gid gid) { return gid::CreatorWorker(gid); }

GraphState BuildFromConfig(int num_workers, const nlohmann::json &config) {
  ValueGenerator value_generator;
  GraphState state(num_workers);

  for (const auto &index : GetWithDefault(config, "indexes", {})) {
    auto index_parts = utils::Split(index, ".");
    CHECK(index_parts.size() == 2) << "Index format should be Label.Property";
    state.CreateIndex(index_parts[0], index_parts[1]);
  }

  CHECK(config["nodes"].is_array() && config["nodes"].size() > 0)
      << "Generator config must have 'nodes' array with at least one "
         "element";
  for (const auto &node_config : config["nodes"]) {
    CHECK(node_config.is_object()) << "Node config must be a dict";

    const auto &labels = node_config["labels"];
    CHECK(labels.is_array()) << "Must provide an array of node labels";
    CHECK(node_config.size() > 0)
        << "Node labels array must contain at least one element";

    for (int i = 0; i < node_config["count_per_worker"]; ++i) {
      for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
        const auto properties =
            value_generator.MakeProperties(node_config["properties"]);
        state.CreateNode(worker_id, labels, properties);
      }
    }
  }

  int num_hops = 0;
  auto get_edge_endpoint = [num_workers, &state, &num_hops, &value_generator](
                               gid::Gid from, std::string label_to,
                               double hop_probability) {
    if (num_workers > 1 && value_generator.Bernoulli(hop_probability)) {
      ++num_hops;
      return state.RandomNodeOnOtherWorker(label_to, Worker(from));
    }
    return state.RandomNode(label_to, Worker(from));
  };

  for (const auto &edge_config : config["edges"]) {
    CHECK(edge_config.is_object()) << "Edge config must be a dict";
    const std::string &label_from = edge_config["from"];
    const std::string &label_to = edge_config["to"];
    const std::string &type = edge_config["type"];
    const double hop_probability = edge_config["hop_probability"];

    if (edge_config["kind"] == "random") {
      for (int i = 0; i < edge_config["count"]; i++) {
        gid::Gid from = state.RandomNode(label_from);
        gid::Gid to = get_edge_endpoint(from, label_to, hop_probability);

        const auto &props = value_generator.MakeProperties(
            GetWithDefault(edge_config, "properties", nullptr));
        state.CreateEdge(from, to, type, props);
      }
    }

    if (edge_config["kind"] == "unique") {
      for (const auto &from : state.NodesWithLabel(label_from)) {
        gid::Gid to = get_edge_endpoint(from, label_to, hop_probability);
        const auto &props = value_generator.MakeProperties(
            GetWithDefault(edge_config, "properties", nullptr));
        state.CreateEdge(from, to, type, props);
      }
    }
  }

  for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
    LOG(INFO) << "-- Summary for worker: " << worker_id;
    LOG(INFO) << "---- Total nodes: " << state.NumNodesOnWorker(worker_id);
    LOG(INFO) << "---- Total edges: " << state.NumEdgesOnWorker(worker_id);
  }
  LOG(INFO) << "-- Total number of hops: " << num_hops;

  return state;
}

}  // namespace snapshot_generation
