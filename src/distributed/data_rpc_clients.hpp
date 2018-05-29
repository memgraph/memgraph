#pragma once

#include <mutex>
#include <utility>

#include "distributed/rpc_worker_clients.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// Provides access to other worker's data.
class DataRpcClients {
 public:
  DataRpcClients(RpcWorkerClients &clients) : clients_(clients) {}
  /// Returns a remote worker's record (vertex/edge) data for the given params.
  /// That worker must own the vertex/edge for the given id, and that vertex
  /// must be visible in given transaction.
  template <typename TRecord>
  std::unique_ptr<TRecord> RemoteElement(int worker_id, tx::TransactionId tx_id,
                                         gid::Gid gid);

  /// Returns (worker_id, vertex_count) for each worker and the number of
  /// vertices on it from the perspective of transaction `tx_id`.
  std::unordered_map<int, int64_t> VertexCounts(tx::TransactionId tx_id);

 private:
  RpcWorkerClients &clients_;
};

}  // namespace distributed
