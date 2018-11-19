/// @file

#pragma once

#include <functional>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "io/network/endpoint.hpp"
#include "utils/future.hpp"
#include "utils/thread.hpp"

namespace raft {

/**
 * This class is responsible for coordination between workers (nodes) within
 * the Raft cluster. Its implementation is quite similar to coordination
 * in distributed Memgraph apart from slight modifications which align more
 * closely to Raft.
 *
 * It should be noted that, in the context of communication, all nodes within
 * the Raft cluster are considered equivalent and are henceforth known simply
 * as workers.
 *
 * This class is thread safe.
 */
class Coordination {
 protected:
  /**
   * Class constructor
   *
   * @param server_workers_count Number of workers in RPC Server.
   * @param client_workers_count Number of workers in RPC Client.
   * @param worker_id ID of Raft worker (node) on this machine.
   * @param workers Mapping between Raft worker_ids to their network addresses.
   */
  Coordination(uint16_t sever_workers_count, uint16_t client_workers_count,
               uint16_t worker_id,
               std::unordered_map<uint16_t, io::network::Endpoint> workers);

 public:
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

 protected:
  /// Adds a worker to the coordination. This function can be called multiple
  /// times to replace an existing worker.
  void AddWorker(int worker_id, const io::network::Endpoint &endpoint);

  /// Gets a worker name for the given endpoint.
  std::string GetWorkerName(const io::network::Endpoint &endpoint);

  communication::rpc::Server server_;

 private:
  std::unordered_map<uint16_t, io::network::Endpoint> workers_;
  mutable std::mutex lock_;
  uint16_t worker_id_;

  std::unordered_map<int, communication::rpc::ClientPool> client_pools_;
  utils::ThreadPool thread_pool_;
};

}  // namespace raft
