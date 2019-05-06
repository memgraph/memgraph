/// @file

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "communication/rpc/client.hpp"
#include "communication/rpc/server.hpp"
#include "io/network/endpoint.hpp"
#include "raft/exceptions.hpp"

namespace raft {

/// Loads raft cluster configuration from file.
///
/// File format:
/// [[node_id, "node_address", node_port], ...]
std::unordered_map<uint16_t, io::network::Endpoint> LoadNodesFromFile(
    const std::string &coordination_config_file);

/// This class is responsible for coordination between nodes within the Raft
/// cluster. Its implementation is quite similar to coordination in distributed
/// Memgraph apart from slight modifications which align more closely to Raft.
///
/// It should be noted that, in the context of communication, all nodes within
/// the Raft cluster are considered equivalent and are henceforth known simply
/// as nodes.
///
/// This class is thread safe.
class Coordination final {
 public:
  /// Class constructor
  ///
  /// @param node_id ID of Raft node on this machine.
  /// @param node mapping from node_id to endpoint information (for the whole
  ///        cluster).
  Coordination(uint16_t node_id,
               std::unordered_map<uint16_t, io::network::Endpoint> all_nodes);

  ~Coordination();

  Coordination(const Coordination &) = delete;
  Coordination(Coordination &&) = delete;
  Coordination &operator=(const Coordination &) = delete;
  Coordination &operator=(Coordination &&) = delete;

  /// Returns all node IDs.
  std::vector<uint16_t> GetAllNodeIds();

  /// Returns other node IDs (excluding this node).
  std::vector<uint16_t> GetOtherNodeIds();

  /// Returns total number of nodes.
  uint16_t GetAllNodeCount();

  /// Returns number of other nodes.
  uint16_t GetOtherNodeCount();

  /// Executes a RPC on another node in the cluster. If the RPC execution
  /// fails (because of underlying network issues) it returns a `std::nullopt`.
  template <class TRequestResponse, class... Args>
  std::optional<typename TRequestResponse::Response> ExecuteOnOtherNode(
      uint16_t other_id, Args &&... args) {
    CHECK(other_id != node_id_) << "Trying to execute RPC on self!";
    CHECK(other_id >= 1 && other_id <= cluster_size_) << "Invalid node id!";

    auto &lock = *client_locks_[other_id - 1].get();
    auto &client = clients_[other_id - 1];

    std::lock_guard<std::mutex> guard(lock);

    if (!client) {
      const auto &endpoint = endpoints_[other_id - 1];
      client = std::make_unique<communication::rpc::Client>(endpoint);
    }

    try {
      return client->Call<TRequestResponse>(std::forward<Args>(args)...);
    } catch (...) {
      // Invalidate the client so that we reconnect next time.
      client = nullptr;
      return std::nullopt;
    }
  }

  /// Registers a RPC call on this node.
  template <class TRequestResponse>
  void Register(std::function<void(slk::Reader *, slk::Builder *)> callback) {
    server_.Register<TRequestResponse>(callback);
  }

  /// Registers an extended RPC call on this node.
  template <class TRequestResponse>
  void Register(std::function<void(const io::network::Endpoint &, slk::Reader *,
                                   slk::Builder *)>
                    callback) {
    server_.Register<TRequestResponse>(callback);
  }

  /// Starts the coordination and its servers.
  bool Start();

  /// Blocks until the coordination is shut down. Accepts a callback function
  /// that is called to clean up all services that should be stopped before the
  /// coordination.
  void AwaitShutdown(std::function<void(void)> call_before_shutdown);

  /// Hints that the coordination should start shutting down the whole cluster.
  void Shutdown();

 private:
  uint16_t node_id_;
  uint16_t cluster_size_;

  communication::rpc::Server server_;

  std::vector<io::network::Endpoint> endpoints_;
  std::vector<std::unique_ptr<communication::rpc::Client>> clients_;
  std::vector<std::unique_ptr<std::mutex>> client_locks_;

  std::atomic<bool> alive_{true};
};

}  // namespace raft
