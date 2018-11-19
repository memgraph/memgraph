/// @file

#pragma once

#include <atomic>
#include <experimental/filesystem>
#include <functional>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "io/network/endpoint.hpp"
#include "raft/exceptions.hpp"
#include "utils/future.hpp"
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

  /// Returns a cached `ClientPool` for the given `worker_id`.
  communication::rpc::ClientPool *GetClientPool(int worker_id);

  /// Asynchronously executes the given function on the RPC client for the
  /// given worker id. Returns an `utils::Future` of the given `execute`
  /// function's return type.
  template <typename TResult>
  auto ExecuteOnWorker(
      int worker_id,
      std::function<TResult(int worker_id, communication::rpc::ClientPool &)>
          execute) {
    auto client_pool = GetClientPool(worker_id);
    return thread_pool_.Run(execute, worker_id, std::ref(*client_pool));
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

  bool AwaitShutdown(std::function<bool(void)> call_before_shutdown =
                         []() -> bool { return true; });

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

  std::unordered_map<int, communication::rpc::ClientPool> client_pools_;
  utils::ThreadPool thread_pool_;

  // Flags used for shutdown.
  std::atomic<bool> alive_{true};
};

}  // namespace raft
