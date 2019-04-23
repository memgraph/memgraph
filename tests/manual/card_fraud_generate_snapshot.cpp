#include <filesystem>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.hpp>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "durability/distributed/paths.hpp"
#include "durability/distributed/snapshot_encoder.hpp"
#include "durability/distributed/version.hpp"
#include "storage/distributed/address_types.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

#include "snapshot_generation/graph_state.hpp"
#include "snapshot_generation/snapshot_writer.hpp"

DEFINE_int32(num_workers, 1,
             "Number of distributed workers (including master)");
DEFINE_string(dir, "tmp",
              "Directory for storing workers durability directories.");
DEFINE_string(config, "", "Path to config JSON file");

/**
 * Config file should defined as follows.
 * {
 *   "cards_per_worker" : 10000,
 *   "pos_per_worker" : 1000,
 *   "transactions_per_worker" : 50000,
 *   "compromised_pos_probability" : 0.2,
 *   "fraud_reported_probability" : 0.1,
 *   "hop_probability" : 0.1
 * }
 */

using namespace snapshot_generation;
using nlohmann::json;

json BuildGenericConfig(const json &config) {
  json indices = json::array(
      {"Card.id", "Pos.id", "Transaction.id", "Transaction.fraud_reported"});

  json cards;
  cards["labels"] = {"Card"};
  cards["count_per_worker"] = config["cards_per_worker"];

  cards["properties"] = json::object();
  cards["properties"].push_back(
      {"id", {{"type", "counter"}, {"param", "Card.id"}}});
  cards["properties"].push_back(
      {"compromised", {{"type", "primitive"}, {"param", false}}});

  json pos;
  pos["labels"] = {"Pos"};
  pos["count_per_worker"] = config["pos_per_worker"];

  pos["properties"] = json::object();
  pos["properties"].push_back(
      {"id", {{"type", "counter"}, {"param", "Pos.id"}}});
  pos["properties"].push_back(
      {"compromised",
       {{"type", "bernoulli"},
        {"param", config["compromised_pos_probability"]}}});
  pos["properties"].push_back(
      {"connected_frauds", {{"type", "primitive"}, {"param", 0}}});

  json txs;
  txs["labels"] = {"Transaction"};
  txs["count_per_worker"] = config["transactions_per_worker"];

  txs["properties"] = json::object();
  txs["properties"].push_back(
      {"id", {{"type", "counter"}, {"param", "Transaction.id"}}});
  txs["properties"].push_back(
      {"fraud_reported", {{"type", "primitive"}, {"param", false}}});

  json edges;
  edges = json::array();
  edges.push_back({{"kind", "unique"},
                   {"from", "Transaction"},
                   {"to", "Card"},
                   {"type", "Using"},
                   {"hop_probability", config["hop_probability"]}});
  edges.push_back({{"kind", "unique"},
                   {"from", "Transaction"},
                   {"to", "Pos"},
                   {"type", "At"},
                   {"hop_probability", config["hop_probability"]}});

  return json::object(
      {{"indexes", indices}, {"nodes", {cards, pos, txs}}, {"edges", edges}});
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  nlohmann::json config;
  {
    std::ifstream config_file(FLAGS_config);
    config_file >> config;
  }

  GraphState state =
      BuildFromConfig(FLAGS_num_workers, BuildGenericConfig(config));

  // TODO(mtomic): this is not currently used in the demo, maybe remove?

  // A card is compromised if it was used on a compromised POS.
  for (auto &tx_gid : state.NodesWithLabel("Transaction")) {
    auto &node = state.GetNode(tx_gid);
    auto &edge1 = state.GetEdge(node.out_edges[0]);
    auto &edge2 = state.GetEdge(node.out_edges[1]);
    if (edge1.type != "Using") {
      std::swap(edge1, edge2);
    }
    DCHECK(edge1.type == "Using" && edge2.type == "At");

    if (state.GetNode(edge2.to).props.at("compromised").Value<bool>()) {
      state.GetNode(edge1.to).props.at("compromised") = true;
    }
  }

  // Transaction is reported as fraudulent with some probability if a
  // compromised card was used.
  std::mt19937 gen{std::random_device{}()};
  std::uniform_real_distribution<double> dist(0, 1);

  for (auto &tx_gid : state.NodesWithLabel("Transaction")) {
    auto &node = state.GetNode(tx_gid);
    auto &edge = state.GetEdge(node.out_edges[0]);
    DCHECK(edge.type == "Using");

    if (state.GetNode(edge.to).props.at("compromised").Value<bool>()) {
      node.props.at("fraud_reported") =
          node.props.at("fraud_reported").Value<bool>() ||
          (dist(gen) < config["fraud_reported_probability"]);
    }
  }

  WriteToSnapshot(state, FLAGS_dir);

  return 0;
}
