/// @file

#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "communication/rpc/client.hpp"
#include "communication/rpc/server.hpp"
#include "io/network/endpoint.hpp"
#include "raft/exceptions.hpp"
#include "utils/thread.hpp"

namespace raft {

/// This class is responsible for coordination between workers (nodes) within
/// the Raft cluster. Its implementation is quite similar to coordination
/// in distributed Memgraph apart from slight modifications which align more
/// closely to Raft.
///
/// It should be noted that, in the context of communication, all nodes within
/// the Raft cluster are considered equivalent and are henceforth known simply
/// as workers.
///
/// This class is thread safe.
class Coordination final {
 public:
  /// Class constructor
  ///
  /// @param server_workers_count Number of workers in RPC Server.
  /// @param client_workers_count Number of workers in RPC Client.
  /// @param worker_id ID of Raft worker (node) on this machine.
  /// @param workers mapping from worker id to endpoint information.
  Coordination(uint16_t server_workers_count, uint16_t client_workers_count,
               uint16_t worker_id,
               std::unordered_map<uint16_t, io::network::Endpoint> workers);

  ~Coordination();

  Coordination(const Coordination &) = delete;
  Coordination(Coordination &&) = delete;
  Coordination &operator=(const Coordination &) = delete;
  Coordination &operator=(Coordination &&) = delete;

  /// Gets the endpoint for the given `worker_id`.
  io::network::Endpoint GetEndpoint(int worker_id);

  /// Gets the endpoint for this RPC server.
  io::network::Endpoint GetServerEndpoint();

  /// Returns all workers ids.
  std::vector<int> GetWorkerIds();

  uint16_t WorkerCount();

  /// Executes a RPC on another worker in the cluster. If the RPC execution
  /// fails (because of underlying network issues) it returns a `std::nullopt`.
  template <class TRequestResponse, class... Args>
  std::optional<typename TRequestResponse::Response> ExecuteOnOtherWorker(
      uint16_t worker_id, Args &&... args) {
    CHECK(worker_id != worker_id_) << "Trying to execute RPC on self!";

    communication::rpc::Client *client = nullptr;
    std::mutex *client_lock = nullptr;
    {
      std::lock_guard<std::mutex> guard(lock_);

      auto found = clients_.find(worker_id);
      if (found != clients_.end()) {
        client = &found->second;
      } else {
        auto found_endpoint = workers_.find(worker_id);
        CHECK(found_endpoint != workers_.end())
            << "No endpoint registered for worker id: " << worker_id;
        auto &endpoint = found_endpoint->second;
        auto it = clients_.emplace(worker_id, endpoint);
        client = &it.first->second;
      }

      auto lock_found = client_locks_.find(worker_id);
      CHECK(lock_found != client_locks_.end())
          << "No client lock for worker id: " << worker_id;
      client_lock = lock_found->second.get();
    }

    try {
      std::lock_guard<std::mutex> guard(*client_lock);
      return client->Call<TRequestResponse>(std::forward<Args>(args)...);
    } catch (...) {
      // Invalidate the client so that we reconnect next time.
      std::lock_guard<std::mutex> guard(lock_);
      CHECK(clients_.erase(worker_id) == 1)
          << "Couldn't remove client for worker id: " << worker_id;
      return std::nullopt;
    }
  }

  template <class TRequestResponse>
  void Register(std::function<
                void(const typename TRequestResponse::Request::Capnp::Reader &,
                     typename TRequestResponse::Response::Capnp::Builder *)>
                    callback) {
    server_.Register<TRequestResponse>(callback);
  }

  template <class TRequestResponse>
  void Register(std::function<
                void(const io::network::Endpoint &,
                     const typename TRequestResponse::Request::Capnp::Reader &,
                     typename TRequestResponse::Response::Capnp::Builder *)>
                    callback) {
    server_.Register<TRequestResponse>(callback);
  }

  static std::unordered_map<uint16_t, io::network::Endpoint> LoadFromFile(
      const std::string &coordination_config_file);

  /// Starts the coordination and its servers.
  bool Start();

  void AwaitShutdown(std::function<void(void)> call_before_shutdown);

  /// Hints that the coordination should start shutting down the whole cluster.
  void Shutdown();

  /// Gets a worker name for the given endpoint.
  std::string GetWorkerName(const io::network::Endpoint &endpoint);

 private:
  /// Adds a worker to the coordination. This function can be called multiple
  /// times to replace an existing worker.
  void AddWorker(int worker_id, const io::network::Endpoint &endpoint);

  communication::rpc::Server server_;
  uint16_t worker_id_;

  mutable std::mutex lock_;
  std::unordered_map<uint16_t, io::network::Endpoint> workers_;

  std::unordered_map<uint16_t, communication::rpc::Client> clients_;
  std::unordered_map<uint16_t, std::unique_ptr<std::mutex>> client_locks_;

  // Flags used for shutdown.
  std::atomic<bool> alive_{true};
};

}  // namespace raft
