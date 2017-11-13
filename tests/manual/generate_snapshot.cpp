#include <algorithm>
#include <experimental/optional>
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
#include "durability/hashed_file_writer.hpp"
#include "durability/version.hpp"
#include "transactions/type.hpp"
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
 *         "id" : { "type" : "counter", "param": "Person.id" },
 *         "name" : { "type" : "randstring", "param" :
 *            { "type": "randint", "param" : [10, 20]}},
 *         "is_happy" :  { "type" : "bernoulli", "param" : 0.2 }
 *       }
 *     },
 *     {
 *       "count" : 200,
 *       "labels" : ["Company"],
 *       "properties" : {
 *         "id" : { "type" : "counter", "param": "Company.id" },
 *         "name" : { "type" : "randstring", "param" :
 *            { "type": "randint", "param" : [10, 20]}},
 *         "description" : { "type" : "optional", "param" :
 *            [0.2, { "type" : "randstring", "param": 1024 }]}
 *        }
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
  Writer(const std::string &path) : buffer_(path) {
    encoder_.WriteRAW(durability::kMagicNumber.data(),
                      durability::kMagicNumber.size());
    encoder_.WriteTypedValue(durability::kVersion);

    // Transactional ID of the snapshooter.
    encoder_.WriteInt(0);
    // Transactional Snapshot is an empty list of transaction IDs.
    encoder_.WriteList(std::vector<query::TypedValue>{});
  }

  template <typename TValue>
  void WriteList(const std::vector<TValue> &list) {
    encoder_.WriteList(
        std::vector<query::TypedValue>(list.begin(), list.end()));
  }

  int64_t WriteNode(
      const std::vector<std::string> &labels,
      std::unordered_map<std::string, query::TypedValue> properties) {
    encoder_.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                      3);
    encoder_.WriteRAW(underlying_cast(communication::bolt::Signature::Node));
    auto id = node_counter_++;
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

  void Close() {
    buffer_.WriteValue(node_counter_);
    buffer_.WriteValue(edge_counter_);
    buffer_.WriteValue(buffer_.hash());
    buffer_.Close();
  }

 private:
  int64_t node_counter_{0};
  int64_t edge_counter_{0};
  HashedFileWriter buffer_;
  communication::bolt::BaseEncoder<HashedFileWriter> encoder_{buffer_};
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
    CHECK(found != label_nodes_.end()) << "Label not found";
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
  using json = nlohmann::json;
  using TypedValue = query::TypedValue;

 public:
  // Generates the whole property map based on the given config.
  std::unordered_map<std::string, query::TypedValue> MakeProperties(
      const json &config) {
    std::unordered_map<std::string, query::TypedValue> props;
    if (config.is_null()) return props;

    CHECK(config.is_object()) << "Properties config must be a dict";
    for (auto it = config.begin(); it != config.end(); it++) {
      auto value = MakeValue(it.value());
      if (value) props.emplace(it.key(), *value);
    }
    return props;
  }

  // Generates a single value based on the given config.
  std::experimental::optional<TypedValue> MakeValue(const json &config) {
    if (config.is_object()) {
      const std::string &type = config["type"];
      const auto &param = config["param"];
      if (type == "primitive")
        return Primitive(param);
      else if (type == "counter")
        return TypedValue(Counter(param));
      else if (type == "optional")
        return Optional(param);
      else if (type == "bernoulli")
        return TypedValue(Bernoulli(param));
      else if (type == "randint")
        return TypedValue(RandInt(param));
      else if (type == "randstring")
        return TypedValue(RandString(param));
      else
        LOG(FATAL) << "Unknown value type";
    } else
      return Primitive(config);
  }

  TypedValue Primitive(const json &config) {
    if (config.is_string()) return config.get<std::string>();
    if (config.is_number_integer()) return config.get<int64_t>();
    if (config.is_number_float()) return config.get<double>();
    if (config.is_boolean()) return config.get<bool>();

    LOG(FATAL) << "Unsupported primitive type";
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

  int64_t RandInt(const json &range) {
    CHECK(range.is_array() && range.size() == 2)
        << "RandInt value gen config must be a list with 2 elements";
    auto from = MakeValue(range[0])->ValueInt();
    auto to = MakeValue(range[1])->ValueInt();
    CHECK(from < to) << "RandInt lower range must be lesser then upper range";
    return (int64_t)(rand_(gen_) * (to - from)) + from;
  }

  std::string RandString(const json &length) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    int length_int = MakeValue(length)->ValueInt();
    std::string r_val(length_int, 'a');
    for (int i = 0; i < length_int; ++i)
      r_val[i] = alphanum[(int64_t)(rand_(gen_) * (sizeof(alphanum) - 1))];

    return r_val;
  }

  bool Bernoulli(double p) { return rand_(gen_) < p; }

  std::experimental::optional<TypedValue> Optional(const json &config) {
    CHECK(config.is_array() && config.size() == 2)
        << "Optional value gen config must be a list with 2 elements";
    return Bernoulli(config[0]) ? MakeValue(config[1])
                                : std::experimental::nullopt;
  }

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

  Writer writer(FLAGS_out);
  GraphState state;
  ValueGenerator value_generator;

  std::vector<std::string> indexes;
  for (const auto &index : GetWithDefault(config, "indexes", {}))
    for (const auto &index_part : utils::Split(index, "."))
      indexes.push_back(index_part);
  writer.WriteList(indexes);

  // Create nodes
  const auto &nodes_config = config["nodes"];
  CHECK(nodes_config.is_array() && nodes_config.size() > 0)
      << "Generator config must have 'nodes' array with at least one element";
  for (const auto &node_config : config["nodes"]) {
    CHECK(node_config.is_object()) << "Node config must be a dict";

    for (int i = 0; i < node_config["count"]; i++) {
      const auto &labels_config = node_config["labels"];
      CHECK(labels_config.is_array()) << "Must provide an array of node labels";
      CHECK(node_config.size() > 0)
          << "Node labels array must contain at lest one element";
      auto node_bolt_id = writer.WriteNode(
          labels_config,
          value_generator.MakeProperties(node_config["properties"]));

      for (const auto &label : labels_config)
        state.AddNode(label, node_bolt_id);
    }
  }

  // Create edges
  for (const auto &edge_config : config["edges"]) {
    CHECK(edge_config.is_object()) << "Edge config must be a dict";
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
