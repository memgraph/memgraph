#include <functional>

#include "distributed/data_manager.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

utils::Future<PullData> PullRpcClients::Pull(
    database::GraphDbAccessor &dba, int worker_id, int64_t plan_id,
    tx::CommandId command_id, const Parameters &params,
    const std::vector<query::Symbol> &symbols, int64_t timestamp,
    bool accumulate, int batch_size) {
  return clients_.ExecuteOnWorker<
      PullData>(worker_id, [&dba, plan_id, command_id, params, symbols,
                            timestamp, accumulate, batch_size](
                               int worker_id, ClientPool &client_pool) {
    auto load_pull_res = [&dba](const auto &res_reader) {
      PullRes res;
      res.Load(res_reader, &dba);
      return res;
    };
    auto result = client_pool.CallWithLoad<PullRpc>(
        load_pull_res, dba.transaction_id(), dba.transaction().snapshot(),
        plan_id, command_id, params, symbols, timestamp, accumulate, batch_size,
        true, true);
    return PullData{result->data.pull_state, std::move(result->data.frames)};
  });
}

std::vector<utils::Future<void>>
PullRpcClients::NotifyAllTransactionCommandAdvanced(tx::TransactionId tx_id) {
  return clients_.ExecuteOnWorkers<void>(
      0, [tx_id](int worker_id, auto &client) {
        auto res = client.template Call<TransactionCommandAdvancedRpc>(tx_id);
        CHECK(res) << "TransactionCommandAdvanceRpc failed";
      });
}

}  // namespace distributed
