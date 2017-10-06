#include <algorithm>
#include <fstream>
#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.hpp>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "durability/file_writer_buffer.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

DEFINE_string(out, "", "Destination for the created snapshot file");
DEFINE_string(config, "", "Path to config JSON file");

/**
 * This file contains the program for generating a snapshot based on a JSON
 * definition. The JSON config has the following form:
 *
 * {
 *   "indexes" : ["Person.id", "Company.id"],
 *   "nodes" : [
 *     {
 *       "count" : 10000,
 *       "labels" : ["Person"],
 *       "properties" : {
 *         "is_happy" :  { "type" : "bernoulli", "param" : 0.01 },
 *         "id" : { "type" : "counter", "Person.id" }
 *       }
 *     },
 *     {
 *       "count" : 200,
 *       "labels" : ["Company"]
 *     }
 *   ],
 *   "edges" : [
 *     {
 *       "count" : 5000,
 *       "from" : "Person",
 *       "to" : "Company",
 *       "type" : "WORKS_IN"
 *     },
 *     {
 *       "count" : 20,
 *       "from" : "Person",
 *       "to" : "Company",
 *       "type" : "LIKES"
 *     }
 *   ]
 * }
 */

// Utilities for writing to the snapshot file.
class Writer {
 public:
  /**
   * Creates a writer.
   *
   * @param path - Path to the output file.
   * @Param indexes - A list of (label, property) indexes to create, each in the
   * "Label.property" form.
   */
  Writer(const std::string &path, const std::vector<std::string> &indexes)
      : buffer_(path), encoder_(buffer_) {
    std::vector<std::string> indexes_flat;
    for (const auto &index : indexes)
      for (const auto &index_part : utils::Split(index, "."))
        indexes_flat.emplace_back(index_part);

    encoder_.WriteList(std::vector<query::TypedValue>(indexes_flat.begin(),
                                                      indexes_flat.end()));
  }

  int64_t WriteNode(
      const std::vector<std::string> &labels,
      std::unordered_map<std::string, query::TypedValue> properties) {
    encoder_.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                      3);
    encoder_.WriteRAW(underlying_cast(communication::bolt::Signature::Node));
    auto id = node_counter++;
    encoder_.WriteInt(id);
    encoder_.WriteList(
        std::vector<query::TypedValue>{labels.begin(), labels.end()});
    encoder_.WriteMap(properties);
    return id;
  }

  int64_t WriteEdge(
      const std::string &edge_type,
      const std::unordered_map<std::string, query::TypedValue> properties,
      int64_t bolt_id_from, int64_t bolt_id_to) {
    encoder_.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                      5);
    encoder_.WriteRAW(
        underlying_cast(communication::bolt::Signature::Relationship));
    auto id = edge_counter_++;
    encoder_.WriteInt(id);
    encoder_.WriteInt(bolt_id_from);
    encoder_.WriteInt(bolt_id_to);
    encoder_.WriteString(edge_type);
    encoder_.WriteMap(properties);
    return id;
  }

  void Close() { buffer_.WriteSummary(node_counter, edge_counter_); }

 private:
  int64_t node_counter{0};
  int64_t edge_counter_{0};
  FileWriterBuffer buffer_;
  communication::bolt::BaseEncoder<FileWriterBuffer> encoder_;
};

// Helper class for tracking info about the generated graph.
class GraphState {
 public:
  // Tracks that the given node has the given label.
  void AddNode(const std::string &label, int64_t node_bolt_id) {
    auto found = label_nodes_.find(label);
    if (found == label_nodes_.end())
      label_nodes_.emplace(label, std::vector<int64_t>{node_bolt_id});
    else
      found->second.emplace_back(node_bolt_id);
  }

  // Gets the ID of a random node that has the given label.
  int64_t RandomNode(const std::string &label) {
    auto found = label_nodes_.find(label);
    permanent_assert(found != label_nodes_.end(), "Label not found");
    return found->second[rand_(gen_) * found->second.size()];
  }

 private:
  // Maps labels to node bolt_ids
  std::unordered_map<std::string, std::vector<int64_t>> label_nodes_;

  // Random generator
  std::mt19937 gen_{std::random_device{}()};
  std::uniform_real_distribution<> rand_{0.0, 1.0};
};

// Helper class for property value generation.
class ValueGenerator {
 public:
  // Generates the whole property map based on the given config.
  std::unordered_map<std::string, query::TypedValue> MakeProperties(
      const nlohmann::json &config) {
    std::unordered_map<std::string, query::TypedValue> props;
    if (config.is_null()) return props;

    permanent_assert(config.is_object(), "Properties config must be a dict");
    for (auto it = config.begin(); it != config.end(); it++) {
      if (it.value().is_object())
        props.emplace(it.key(), MakeValue(it.value()));
      else
        permanent_fail("Unsupported value type");
    }
    return props;
  }

  // Generates a single value based on the given config.
  query::TypedValue MakeValue(const nlohmann::json &config) {
    permanent_assert(config.is_object(),
                     "Random value gen config must be a dict");
    const std::string &type = config["type"];
    const auto &param = config["param"];
    if (type == "bernoulli") {
      return Bernoulli(param);
    } else if (type == "counter")
      return Counter(param);
    else
      permanent_fail("Unknown distribution");
  }

  int64_t Counter(const std::string &name) {
    auto found = counters_.find(name);
    if (found == counters_.end()) {
      counters_.emplace(name, 1);
      return 0;
    } else {
      return found->second++;
    }
  }

  bool Bernoulli(double p) { return rand_(gen_) < p; }

 private:
  std::mt19937 gen_{std::random_device{}()};
  std::uniform_real_distribution<> rand_{0.0, 1.0};
  std::unordered_map<std::string, int64_t> counters_;
};

nlohmann::json GetWithDefault(const nlohmann::json &object,
                              const std::string &key,
                              const nlohmann::json &default_value) {
  const auto &found = object.find(key);
  if (found == object.end()) return default_value;
  return *found;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Read the config JSON
  nlohmann::json config;
  {
    std::ifstream config_file(FLAGS_config);
    config_file >> config;
  }

  std::vector<std::string> indexes;
  for (const auto &index : GetWithDefault(config, "indexes", {}))
    indexes.push_back(index);
  Writer writer(FLAGS_out, indexes);
  GraphState state;
  ValueGenerator value_generator;

  // Create nodes
  const auto &nodes_config = config["nodes"];
  permanent_assert(
      nodes_config.is_array() && nodes_config.size() > 0,
      "Generator config must have 'nodes' array with at least one element");
  for (const auto &node_config : config["nodes"]) {
    permanent_assert(node_config.is_object(), "Node config must be a dict");

    for (int i = 0; i < node_config["count"]; i++) {
      const auto &labels_config = node_config["labels"];
      permanent_assert(labels_config.is_array(),
                       "Must provide an array of node labels");
      permanent_assert(node_config.size() > 0,
                       "Node labels array must contain at lest one element");
      auto node_bolt_id = writer.WriteNode(
          labels_config,
          value_generator.MakeProperties(node_config["properties"]));

      for (const auto &label : labels_config)
        state.AddNode(label, node_bolt_id);
    }
  }

  // Create edges
  for (const auto &edge_config : config["edges"]) {
    permanent_assert(edge_config.is_object(), "Edge config must be a dict");
    const std::string &from = edge_config["from"];
    const std::string &to = edge_config["to"];
    for (int i = 0; i < edge_config["count"]; i++)
      writer.WriteEdge(edge_config["type"],
                       value_generator.MakeProperties(
                           GetWithDefault(edge_config, "properties", nullptr)),
                       state.RandomNode(from), state.RandomNode(to));
  }

  writer.Close();
  return 0;
}
