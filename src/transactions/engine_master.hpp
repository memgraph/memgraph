#pragma once

#include "communication/rpc/server.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "transactions/engine_single_node.hpp"

namespace tx {

/** Distributed master transaction engine. Has complete engine functionality and
 * exposes an RPC server to be used by distributed Workers. */
class MasterEngine : public SingleNodeEngine {
 public:
  /**
   * @param server - Required. Used for rpc::Server construction.
   * @param rpc_worker_clients - Required. Used for
   * OngoingProduceJoinerRpcClients construction.
   * @param wal - Optional. If present, the Engine will write tx
   * Begin/Commit/Abort atomically (while under lock).
   */
  MasterEngine(communication::rpc::Server &server,
               distributed::RpcWorkerClients &rpc_worker_clients,
               durability::WriteAheadLog *wal = nullptr);
  void Commit(const Transaction &t) override;
  void Abort(const Transaction &t) override;

 private:
  communication::rpc::Server &rpc_server_;
  distributed::OngoingProduceJoinerRpcClients ongoing_produce_joiner_;
};
}  // namespace tx
