#pragma once

#include <future>
#include <mutex>
#include <utility>

#include "distributed/rpc_worker_clients.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// Provides an ability to trigger snapshooting on other workers.
class DurabilityRpcClients {
 public:
  DurabilityRpcClients(RpcWorkerClients &clients) : clients_(clients) {}

  // Sends a snapshot request to workers and returns a future which becomes true
  // if all workers sucesfully completed their snapshot creation, false
  // otherwise
  // @param tx - transaction from which to take db snapshot
  utils::Future<bool> MakeSnapshot(tx::TransactionId tx);

 private:
  RpcWorkerClients &clients_;
};

}  // namespace distributed
