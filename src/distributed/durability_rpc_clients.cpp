#include "distributed/durability_rpc_clients.hpp"

#include "distributed/durability_rpc_messages.hpp"
#include "transactions/transaction.hpp"
#include "utils/future.hpp"

namespace distributed {
utils::Future<bool> DurabilityRpcClients::MakeSnapshot(tx::TransactionId tx) {
  return utils::make_future(std::async(std::launch::async, [this, tx] {
    auto futures = clients_.ExecuteOnWorkers<bool>(
        0, [tx](int worker_id, communication::rpc::ClientPool &client_pool) {
          auto res = client_pool.Call<MakeSnapshotRpc>(tx);
          if (!res) return false;
          return res->member;
        });

    bool created = true;
    for (auto &future : futures) {
      created &= future.get();
    }

    return created;
  }));
}
}  // namespace distributed
