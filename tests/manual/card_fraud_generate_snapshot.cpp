#include <experimental/filesystem>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.hpp>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "durability/paths.hpp"
#include "durability/snapshot_encoder.hpp"
#include "durability/version.hpp"
#include "storage/address_types.hpp"
#include "utils/datetime/timestamp.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

DEFINE_string(num_workers, "1",
              "Number of distributed workers (including master)");
DEFINE_string(dir, "tmp",
              "Directory for storing workers durability directories.");
DEFINE_string(config, "", "Path to config JSON file");

/**
 * Config file should defined as follows.
 *
 *{
 *  "indexes" : ["Card.id", "Pos.id", "Transaction.fraud_reported"],
 *  "nodes" : [
 *    {
 *      "count_per_worker" : 10,
 *      "label" : "Card"
 *    },
 *    {
 *      "count_per_worker" : 10,
 *      "label" : "Pos"
 *    },
 *    {
 *      "count_per_worker" : 20,
 *      "label" : "Transaction"
 *    }
 *  ],
 *  "compromised_pos_probability" : 0.2,
 *  "fraud_reported_probability" : 0.1,
 *  "hop_percentage" : 0.1
 *}
 */

namespace fs = std::experimental::filesystem;

/// Helper class for tracking info about the generated graph.
class GraphState {
  typedef std::unordered_map<std::string, std::vector<gid::Gid>> LabelNodes;

 public:
  GraphState(int num_workers)
      : worker_nodes(num_workers),
        compromised_pos(num_workers),
        compromised_card(num_workers),
        out_edges(num_workers),
        in_edges(num_workers) {}

  // Gets the ID of a random node on worker that has the given label.
  gid::Gid RandomNode(int worker_id, const std::string &label) {
    auto &label_nodes = worker_nodes[worker_id];
    auto found = label_nodes.find(label);
    CHECK(found != label_nodes.end()) << "Label not found";
    return found->second[rand_(gen_) * found->second.size()];
  }

  bool IsCompromisedPos(int worker_id, gid::Gid pos_id) {
    std::unordered_set<gid::Gid> &compromised = compromised_pos[worker_id];
    return compromised.find(pos_id) != compromised.end();
  }

  bool IsCompromisedCard(int worker_id, gid::Gid card_id) {
    std::unordered_set<gid::Gid> &compromised = compromised_card[worker_id];
    return compromised.find(card_id) != compromised.end();
  }

  struct Edge {
    enum class Type { USING, AT };
    gid::Gid gid;
    storage::VertexAddress from;
    storage::VertexAddress to;
    Type type;
  };

  void AddEdge(gid::Gid edge_gid, storage::VertexAddress from,
               storage::VertexAddress to, Edge::Type type) {
    out_edges[from.worker_id()][from.gid()].emplace_back(edges.size());
    in_edges[to.worker_id()][to.gid()].emplace_back(edges.size());
    edges.emplace_back(Edge{edge_gid, from, to, type});
  }

  // Maps worker node labels to node bolt_ids.
  std::vector<LabelNodes> worker_nodes;

  // Compromised cards and pos.
  std::vector<std::unordered_set<gid::Gid>> compromised_pos;
  std::vector<std::unordered_set<gid::Gid>> compromised_card;

  // In/Out Vertex Edges.
  std::vector<Edge> edges;
  std::vector<std::unordered_map<gid::Gid, std::vector<int>>> out_edges;
  std::vector<std::unordered_map<gid::Gid, std::vector<int>>> in_edges;

  // Random generator
  std::mt19937 gen_{std::random_device{}()};
  std::uniform_real_distribution<> rand_{0.0, 1.0};
};

// Utilities for writing to the snapshot file.
// Snapshot file has the following contents in order:
//   1) Magic number.
//   2) Worker Id
//   3) Internal Id of vertex generator
//   4) Internal Id of edge generator
//   5) Transaction ID of the snapshooter. When generated set to 0.
//   6) Transactional snapshot of the snapshoter. When the snapshot is
//   generated it's an empty list.
//   7) List of label+property index.
//   8) All nodes, sequentially, but not encoded as a list.
//   9) All relationships, sequentially, but not encoded as a list.
//   10) Summary with node count, relationship count and hash digest.
class Writer {
  using DecodedValue = communication::bolt::DecodedValue;
  const std::string kEdgeUsing = "Using";
  const std::string kEdgeAt = "At";

 public:
  Writer(const std::string &path, int worker_id) : buffer_(path) {
    // 1) Magic number
    encoder_.WriteRAW(durability::kMagicNumber.data(),
                      durability::kMagicNumber.size());
    encoder_.WriteTypedValue(durability::kVersion);

    // 2) WorkerId - important for distributed storage
    worker_id_ = worker_id;
    encoder_.WriteInt(worker_id_);

    // The following two entries indicate the starting points for generating new
    // Vertex/Edge IDs in the DB. They are important when there are
    // vertices/edges that were moved to another worker (in distributed
    // Memgraph), so be careful! In this case we don't have to worry
    // because we'll never move vertices/edges between workers.
    // 3) ID of the vertex generator.
    encoder_.WriteInt(0);
    // 4) ID of the edge generator.
    encoder_.WriteInt(0);

    // 5) Transactional ID of the snapshooter.
    encoder_.WriteInt(0);
    // 6) Transactional Snapshot is an empty list of transaction IDs.
    encoder_.WriteList(std::vector<query::TypedValue>{});
  }

  template <typename TValue>
  void WriteList(const std::vector<TValue> &list) {
    encoder_.WriteList(
        std::vector<query::TypedValue>(list.begin(), list.end()));
  }

  void WriteNode(gid::Gid id, const std::string &label,
                 std::unordered_map<std::string, DecodedValue> &properties,
                 const std::vector<GraphState::Edge> &edges,
                 const std::vector<int> &out_edges,
                 const std::vector<int> &in_edges) {
    encoder_.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                      3);
    encoder_.WriteRAW(underlying_cast(communication::bolt::Signature::Node));
    encoder_.WriteInt(id);
    encoder_.WriteList(std::vector<query::TypedValue>{label});
    encoder_.WriteMap(properties);

    encoder_.WriteInt(in_edges.size());
    for (auto edge_num : in_edges) {
      auto &edge = edges[edge_num];
      auto edge_addr = storage::EdgeAddress(edge.gid, edge.from.worker_id());
      encoder_.WriteInt(edge_addr.raw());
      encoder_.WriteInt(edge.from.raw());
      encoder_.WriteString(
          edge.type == GraphState::Edge::Type::USING ? kEdgeUsing : kEdgeAt);
    }

    encoder_.WriteInt(out_edges.size());
    for (auto edge_num : out_edges) {
      auto &edge = edges[edge_num];
      auto edge_addr = storage::EdgeAddress(edge.gid, edge.from.worker_id());
      encoder_.WriteInt(edge_addr.raw());
      encoder_.WriteInt(edge.to.raw());
      encoder_.WriteString(
          edge.type == GraphState::Edge::Type::USING ? kEdgeUsing : kEdgeAt);
    }
  }

  void WriteEdge(const GraphState::Edge &edge) {
    encoder_.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                      5);
    encoder_.WriteRAW(
        underlying_cast(communication::bolt::Signature::Relationship));
    encoder_.WriteInt(edge.gid);
    encoder_.WriteInt(edge.from.gid());
    encoder_.WriteInt(edge.to.gid());
    encoder_.WriteString(edge.type == GraphState::Edge::Type::USING ? kEdgeUsing
                                                                    : kEdgeAt);
    // Write edges to snapshot.
    std::unordered_map<std::string, communication::bolt::DecodedValue>
        empty_props{};
    encoder_.WriteMap(empty_props);
  }

  void Close(uint64_t node_count, uint64_t edge_count, uint64_t hops_count) {
    // 10) Summary with node count, relationship count and hash digest.
    buffer_.WriteValue(node_count);
    buffer_.WriteValue(edge_count);
    buffer_.WriteValue(buffer_.hash());
    buffer_.Close();
    // Log summary
    LOG(INFO) << fmt::format("-- Summary for worker: {}", worker_id_);
    LOG(INFO) << fmt::format("---- Total nodes: {}", node_count);
    LOG(INFO) << fmt::format("---- Total edges: {}", edge_count);
    LOG(INFO) << fmt::format("---- Hop edges: {}", hops_count);
  }

 private:
  HashedFileWriter buffer_;
  durability::SnapshotEncoder<HashedFileWriter> encoder_{buffer_};
  int worker_id_;
};

// Helper class for property value generation.
class ValueGenerator {
  using json = nlohmann::json;
  using DecodedValue = communication::bolt::DecodedValue;

 public:
  std::unordered_map<std::string, DecodedValue> MakePosProperties(
      bool compromised, int worker_id) {
    std::unordered_map<std::string, DecodedValue> props;
    props.emplace("id", DecodedValue(Counter("Pos.id")));
    props.emplace("worker_id", DecodedValue(worker_id));
    props.emplace("compromised", DecodedValue(compromised));
    props.emplace("connected_frauds", DecodedValue(0));
    return props;
  }

  std::unordered_map<std::string, DecodedValue> MakeTxProperties(
      bool fraud_reported, int worker_id, int id) {
    std::unordered_map<std::string, DecodedValue> props;
    props.emplace("id", DecodedValue(id));
    props.emplace("worker_id", DecodedValue(worker_id));
    props.emplace("fraud_reported", DecodedValue(fraud_reported));
    return props;
  }

  std::unordered_map<std::string, DecodedValue> MakeCardProperties(
      bool compromised, int worker_id) {
    std::unordered_map<std::string, DecodedValue> props;
    props.emplace("id", DecodedValue(Counter("Card.id")));
    props.emplace("worker_id", DecodedValue(worker_id));
    props.emplace("compromised", DecodedValue(compromised));
    return props;
  }

  int64_t Counter(const std::string &name) { return counters_[name]++; }

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
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  nlohmann::json config;
  {
    std::ifstream config_file(FLAGS_config);
    config_file >> config;
  }

  // Vector IDs.
  int num_workers = std::atoi(FLAGS_num_workers.c_str());
  std::vector<int> worker_ids(num_workers);
  std::iota(worker_ids.begin(), worker_ids.end(), 0);

  // Generated node and edge counters.
  std::vector<int> nodes_count(num_workers, 0);
  std::vector<int> hops_count(num_workers, 0);

  GraphState state(num_workers);
  ValueGenerator value_generator;

  const std::string kLabelTransaction = "Transaction";
  const std::string kLabelCard = "Card";
  const std::string kLabelPos = "Pos";
  const fs::path kSnapshotDir = "snapshots";

  double compromised_pos_probability = config["compromised_pos_probability"];
  double fraud_reported_probability = config["fraud_reported_probability"];
  double hop_probability = config["hop_probability"];

  LOG(INFO) << "Creating snapshots with config: ";
  LOG(INFO) << fmt::format("-- Compromised Pos probability: {}",
                           compromised_pos_probability);
  LOG(INFO) << fmt::format("-- Hop probability : {}", hop_probability);

  // Allocate ids for nodes and write them to state.
  LOG(INFO) << "Creating nodes...";
  for (auto worker_id : worker_ids) {
    gid::Generator node_generator{worker_id};

    const auto &nodes_config = config["nodes"];
    CHECK(nodes_config.is_array() && nodes_config.size() > 0)
        << "Generator config must have 'nodes' array with at least one element";
    for (const auto &node_config : config["nodes"]) {
      CHECK(node_config.is_object()) << "Node config must be a dict";
      const auto &label = node_config["label"];
      CHECK(label.is_string() && !label.empty())
          << "Node must have label specified";
      for (int i = 0; i < node_config["count_per_worker"]; i++) {
        auto node_gid = node_generator.Next(std::experimental::nullopt);
        if (label == kLabelPos &&
            value_generator.Bernoulli(compromised_pos_probability)) {
          // Write compromised to state.
          state.compromised_pos[worker_id].insert(node_gid);
        }
        // Write node to state.
        state.worker_nodes[worker_id][label].emplace_back(node_gid);
      }
    }
    nodes_count[worker_id] = node_generator.LocalCount();
  }
  LOG(INFO) << "Creating nodes...DONE";

  std::random_device random_device;
  std::mt19937 engine{random_device()};

  // Create edges for each transaction.
  LOG(INFO) << "Creating edges...";
  for (auto &worker_id : worker_ids) {
    gid::Generator edge_generator{worker_id};

    auto filter = [worker_id, num_workers](const int other_worker) {
      if (num_workers == 1) return true;
      return other_worker != worker_id;
    };
    std::vector<int> hop_workers;
    std::copy_if(worker_ids.begin(), worker_ids.end(),
                 std::back_inserter(hop_workers), filter);
    std::uniform_int_distribution<int> dist(0, hop_workers.size() - 1);

    // Create and write edges to state.
    // Write compromised cards to state.
    const auto &transactions = state.worker_nodes[worker_id][kLabelTransaction];
    for (auto &transaction_id : transactions) {
      int card_worker_id = worker_id;
      if (value_generator.Bernoulli(hop_probability)) {
        card_worker_id = hop_workers[dist(engine)];
        ++hops_count[worker_id];
      }
      auto card_id = state.RandomNode(card_worker_id, kLabelCard);

      int pos_worker_id = worker_id;
      if (value_generator.Bernoulli(hop_probability)) {
        pos_worker_id = hop_workers[dist(engine)];
        ++hops_count[worker_id];
      }
      auto pos_id = state.RandomNode(pos_worker_id, kLabelPos);

      auto edge_using_id = edge_generator.Next(std::experimental::nullopt);
      state.AddEdge(edge_using_id,
                    storage::VertexAddress(transaction_id, worker_id),
                    storage::VertexAddress(card_id, card_worker_id),
                    GraphState::Edge::Type::USING);
      auto edge_at_id = edge_generator.Next(std::experimental::nullopt);
      state.AddEdge(edge_at_id,
                    storage::VertexAddress(transaction_id, worker_id),
                    storage::VertexAddress(pos_id, pos_worker_id),
                    GraphState::Edge::Type::AT);

      if (state.IsCompromisedPos(pos_worker_id, pos_id)) {
        state.compromised_card[card_worker_id].insert(card_id);
      }
    }
  }
  LOG(INFO) << "Creating edges...DONE";

  // Write snapshot files.
  LOG(INFO) << "Writing snapshots...";
  for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
    const fs::path durability_dir =
        FLAGS_dir / fs::path("worker_" + std::to_string(worker_id));
    if (!durability::EnsureDir(durability_dir / kSnapshotDir)) {
      LOG(ERROR) << "Unable to create durability directory!";
      exit(0);
    }
    const auto snapshot_file =
        durability::MakeSnapshotPath(durability_dir, worker_id);

    Writer writer(snapshot_file, worker_id);

    // Write indexes to snapshot.
    std::vector<std::string> indexes;
    for (const auto &index : GetWithDefault(config, "indexes", {}))
      for (const auto &index_part : utils::Split(index, "."))
        indexes.push_back(index_part);
    writer.WriteList(indexes);

    // Write Cards to snapshot.
    for (auto &card_id : state.worker_nodes[worker_id][kLabelCard]) {
      bool is_compromised = state.IsCompromisedCard(worker_id, card_id);
      auto props =
          value_generator.MakeCardProperties(is_compromised, worker_id);
      DCHECK(state.out_edges[worker_id][card_id].size() == 0);
      writer.WriteNode(card_id, kLabelCard, props, state.edges,
                       state.out_edges[worker_id][card_id],
                       state.in_edges[worker_id][card_id]);
    }

    // Write Pos to snapshot.
    for (auto &pos_id : state.worker_nodes[worker_id][kLabelPos]) {
      bool is_compromised = state.IsCompromisedPos(worker_id, pos_id);
      auto props = value_generator.MakePosProperties(is_compromised, worker_id);
      DCHECK(state.out_edges[worker_id][pos_id].size() == 0);
      writer.WriteNode(pos_id, kLabelPos, props, state.edges,
                       state.out_edges[worker_id][pos_id],
                       state.in_edges[worker_id][pos_id]);
    }
    // Write Transactions to snapshot.
    int transaction_id = 0;
    for (auto &tx_id : state.worker_nodes[worker_id][kLabelTransaction]) {
      transaction_id++;
      const auto &edges = state.edges;
      const auto &out_edges = state.out_edges[worker_id][tx_id];
      DCHECK(out_edges.size() == 2);
      DCHECK(edges[out_edges[0]].type == GraphState::Edge::Type::USING);
      DCHECK(edges[out_edges[1]].type == GraphState::Edge::Type::AT);
      DCHECK(state.in_edges[worker_id][tx_id].size() == 0);
      bool fraud_reported = false;
      if (state.IsCompromisedCard(edges[out_edges[0]].to.worker_id(),
                                  edges[out_edges[0]].to.gid())) {
        fraud_reported = value_generator.Bernoulli(fraud_reported_probability);
      }
      auto props = value_generator.MakeTxProperties(
          fraud_reported, worker_id, transaction_id * num_workers + worker_id);
      writer.WriteNode(tx_id, kLabelTransaction, props, state.edges,
                       state.out_edges[worker_id][tx_id],
                       state.in_edges[worker_id][tx_id]);
    }
    // Write edges to snapshot.
    int edge_count{0};
    for (auto &edge : state.edges) {
      if (edge.from.worker_id() == worker_id) {
        writer.WriteEdge(edge);
        ++edge_count;
      }
    }
    writer.Close(nodes_count[worker_id], edge_count, hops_count[worker_id]);
  }
  LOG(INFO) << "Writing snapshots...DONE";
  return 0;
}
