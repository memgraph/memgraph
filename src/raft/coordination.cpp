#include "raft/coordination.hpp"

#include <gflags/gflags.h>
#include <json/json.hpp>

#include "utils/file.hpp"
#include "utils/string.hpp"

DEFINE_string(rpc_cert_file, "", "Certificate file to use (RPC).");
DEFINE_string(rpc_key_file, "", "Key file to use (RPC).");

namespace raft {

namespace fs = std::filesystem;

std::unordered_map<uint16_t, io::network::Endpoint> LoadNodesFromFile(
    const std::string &coordination_config_file) {
  if (!fs::exists(coordination_config_file))
    throw RaftCoordinationConfigException("file (" + coordination_config_file +
                                          ") doesn't exist");

  std::unordered_map<uint16_t, io::network::Endpoint> nodes;
  nlohmann::json data;
  try {
    data = nlohmann::json::parse(
        utils::Join(utils::ReadLines(coordination_config_file), ""));
  } catch (const nlohmann::json::parse_error &e) {
    throw RaftCoordinationConfigException("invalid json");
  }

  if (!data.is_array()) throw RaftCoordinationConfigException("not an array");

  for (auto &it : data) {
    if (!it.is_array())
      throw RaftCoordinationConfigException("element not an array");

    if (it.size() != 3)
      throw RaftCoordinationConfigException("invalid number of subelements");

    if (!it[0].is_number_unsigned() || !it[1].is_string() ||
        !it[2].is_number_unsigned())
      throw RaftCoordinationConfigException("subelement data is invalid");

    nodes[it[0]] = io::network::Endpoint{it[1], it[2]};
  }

  return nodes;
}

Coordination::Coordination(
    uint16_t node_id,
    std::unordered_map<uint16_t, io::network::Endpoint> all_nodes)
    : node_id_(node_id), cluster_size_(all_nodes.size()) {
  // Create and initialize all server elements.
  if (!FLAGS_rpc_cert_file.empty() && !FLAGS_rpc_key_file.empty()) {
    server_context_.emplace(FLAGS_rpc_key_file, FLAGS_rpc_cert_file);
  } else {
    server_context_.emplace();
  }
  server_.emplace(all_nodes[node_id_], &server_context_.value(),
                  all_nodes.size() * 2);

  // Create all client elements.
  endpoints_.resize(cluster_size_);
  clients_.resize(cluster_size_);
  client_locks_.resize(cluster_size_);

  // Initialize all client elements.
  client_context_.emplace(server_context_->use_ssl());
  for (uint16_t i = 1; i <= cluster_size_; ++i) {
    auto it = all_nodes.find(i);
    if (it == all_nodes.end()) {
      throw RaftCoordinationConfigException("missing endpoint for node " +
                                            std::to_string(i));
    }
    endpoints_[i - 1] = it->second;
    client_locks_[i - 1] = std::make_unique<std::mutex>();
  }
}

Coordination::~Coordination() {
  CHECK(!alive_) << "You must call Shutdown and AwaitShutdown on Coordination!";
}

std::vector<uint16_t> Coordination::GetAllNodeIds() {
  std::vector<uint16_t> ret;
  ret.reserve(cluster_size_);
  for (uint16_t i = 1; i <= cluster_size_; ++i) {
    ret.push_back(i);
  }
  return ret;
}

std::vector<uint16_t> Coordination::GetOtherNodeIds() {
  std::vector<uint16_t> ret;
  ret.reserve(cluster_size_ - 1);
  for (uint16_t i = 1; i <= cluster_size_; ++i) {
    if (i == node_id_) continue;
    ret.push_back(i);
  }
  return ret;
}

uint16_t Coordination::GetAllNodeCount() { return cluster_size_; }

uint16_t Coordination::GetOtherNodeCount() { return cluster_size_ - 1; }

io::network::Endpoint Coordination::GetOtherNodeEndpoint(uint16_t other_id) {
  CHECK(other_id != node_id_) << "Trying to execute RPC on self!";
  CHECK(other_id >= 1 && other_id <= cluster_size_) << "Invalid node id!";
  return endpoints_[other_id - 1];
}

communication::ClientContext *Coordination::GetRpcClientContext() {
  return &client_context_.value();
}

bool Coordination::Start() { return server_->Start(); }

void Coordination::AwaitShutdown(
    std::function<void(void)> call_before_shutdown) {
  // Wait for a shutdown notification.
  while (alive_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Call the before shutdown callback.
  call_before_shutdown();

  // Shutdown our RPC server.
  server_->Shutdown();
  server_->AwaitShutdown();
}

void Coordination::Shutdown() { alive_.store(false); }

}  // namespace raft
