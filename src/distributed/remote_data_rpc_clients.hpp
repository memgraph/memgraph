#pragma once

#include <mutex>
#include <utility>

#include "distributed/rpc_worker_clients.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// Provides access to other worker's data.
class RemoteDataRpcClients {
 public:
  RemoteDataRpcClients(RpcWorkerClients &clients) : clients_(clients) {}
  /// Returns a remote worker's record (vertex/edge) data for the given params.
  /// That worker must own the vertex/edge for the given id, and that vertex
  /// must be visible in given transaction.
  template <typename TRecord>
  std::unique_ptr<TRecord> RemoteElement(int worker_id,
                                         tx::transaction_id_t tx_id,
                                         gid::Gid gid);

 private:
  RpcWorkerClients &clients_;
};

}  // namespace distributed
