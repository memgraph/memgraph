#pragma once

#include <vector>

#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/coordination.hpp"
#include "distributed/pull_produce_rpc_messages.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "transactions/type.hpp"
#include "utils/future.hpp"

namespace distributed {

class DataManager;

/// Provides means of calling for the execution of a plan on some remote worker,
/// and getting the results of that execution. The results are returned in
/// batches and are therefore accompanied with an enum indicator of the state of
/// remote execution.
class PullRpcClients {
  using ClientPool = communication::rpc::ClientPool;

 public:
  PullRpcClients(Coordination *coordination, DataManager *data_manager)
      : coordination_(coordination), data_manager_(data_manager) {}

  /// Calls a remote pull asynchroniously. IMPORTANT: take care not to call this
  /// function for the same (tx_id, worker_id, plan_id, command_id) before the
  /// previous call has ended.
  ///
  /// @todo: it might be cleaner to split Pull into {InitRemoteCursor,
  /// Pull, RemoteAccumulate}, but that's a lot of refactoring and more
  /// RPC calls.
  utils::Future<PullData> Pull(
      database::GraphDbAccessor *dba, int worker_id, int64_t plan_id,
      tx::CommandId command_id,
      const query::EvaluationContext &evaluation_context,
      const std::vector<query::Symbol> &symbols, bool accumulate,
      int batch_size = kDefaultBatchSize);

  utils::Future<void> ResetCursor(database::GraphDbAccessor *dba, int worker_id,
                                  int64_t plan_id, tx::CommandId command_id);

  auto GetWorkerIds() { return coordination_->GetWorkerIds(); }

  std::vector<utils::Future<void>> NotifyAllTransactionCommandAdvanced(
      tx::TransactionId tx_id);

 private:
  Coordination *coordination_;
  DataManager *data_manager_;
};

}  // namespace distributed
