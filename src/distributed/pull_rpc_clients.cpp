#include "distributed/pull_rpc_clients.hpp"

#include <functional>

#include "storage/distributed/edge.hpp"
#include "storage/distributed/vertex.hpp"

namespace distributed {

utils::Future<PullData> PullRpcClients::Pull(
    database::GraphDbAccessor *dba, int worker_id, int64_t plan_id,
    tx::CommandId command_id,
    const query::EvaluationContext &evaluation_context,
    const std::vector<query::Symbol> &symbols, bool accumulate,
    int batch_size) {
  return coordination_->ExecuteOnWorker<PullData>(
      worker_id, [data_manager = data_manager_, dba, plan_id, command_id,
                  evaluation_context, symbols, accumulate,
                  batch_size](int worker_id, ClientPool &client_pool) {
        auto load_pull_res = [data_manager, dba](auto *res_reader) {
          PullRes res;
          slk::Load(&res, res_reader, dba, data_manager);
          return res;
        };
        auto result = client_pool.CallWithLoad<PullRpc>(
            load_pull_res, dba->transaction_id(), dba->transaction().snapshot(),
            plan_id, command_id, evaluation_context.timestamp,
            evaluation_context.parameters, symbols, accumulate, batch_size,
            storage::SendVersions::BOTH);
        return PullData{result.data.pull_state, std::move(result.data.frames)};
      });
}

utils::Future<void> PullRpcClients::ResetCursor(database::GraphDbAccessor *dba,
                                                int worker_id, int64_t plan_id,
                                                tx::CommandId command_id) {
  return coordination_->ExecuteOnWorker<void>(
      worker_id, [dba, plan_id, command_id](int worker_id, auto &client) {
        client.template Call<ResetCursorRpc>(dba->transaction_id(), plan_id,
                                             command_id);
      });
}

std::vector<utils::Future<void>>
PullRpcClients::NotifyAllTransactionCommandAdvanced(tx::TransactionId tx_id) {
  return coordination_->ExecuteOnWorkers<void>(
      0, [tx_id](int worker_id, auto &client) {
        client.template Call<TransactionCommandAdvancedRpc>(tx_id);
      });
}

}  // namespace distributed
