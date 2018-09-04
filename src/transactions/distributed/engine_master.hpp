/// @file

#pragma once

#include "communication/rpc/server.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "transactions/distributed/engine_distributed.hpp"
#include "transactions/single_node/engine_single_node.hpp"

namespace tx {

/// Distributed master transaction engine. Has complete engine functionality and
/// exposes an RPC server to be used by distributed Workers.
class EngineMaster final : public EngineDistributed {
 public:
  /// @param server - Required. Used for rpc::Server construction.
  /// @param rpc_worker_clients - Required. Used for
  /// OngoingProduceJoinerRpcClients construction.
  /// @param wal - Optional. If present, the Engine will write tx
  /// Begin/Commit/Abort atomically (while under lock).
  EngineMaster(communication::rpc::Server &server,
               distributed::RpcWorkerClients &rpc_worker_clients,
               durability::WriteAheadLog *wal = nullptr);

  EngineMaster(const EngineMaster &) = delete;
  EngineMaster(EngineMaster &&) = delete;
  EngineMaster &operator=(const EngineMaster &) = delete;
  EngineMaster &operator=(EngineMaster &&) = delete;

  Transaction *Begin() override;
  CommandId Advance(TransactionId id) override;
  CommandId UpdateCommand(TransactionId id) override;
  void Commit(const Transaction &t) override;
  void Abort(const Transaction &t) override;
  CommitLog::Info Info(TransactionId tx) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  TransactionId GlobalLast() const override;
  TransactionId LocalLast() const override;
  TransactionId LocalOldestActive() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;
  Transaction *RunningTransaction(TransactionId tx_id) override;
  void EnsureNextIdGreater(TransactionId tx_id) override;
  void GarbageCollectCommitLog(TransactionId tx_id) override;
  void ClearTransactionalCache(TransactionId oldest_active) override;

 private:
  EngineSingleNode engine_single_node_;
  communication::rpc::Server &rpc_server_;
  distributed::RpcWorkerClients &rpc_worker_clients_;
};
}  // namespace tx
