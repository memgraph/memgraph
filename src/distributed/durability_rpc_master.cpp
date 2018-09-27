#include "distributed/durability_rpc_master.hpp"

#include "distributed/durability_rpc_messages.hpp"
#include "transactions/transaction.hpp"
#include "utils/future.hpp"

namespace distributed {
utils::Future<bool> DurabilityRpcMaster::MakeSnapshot(tx::TransactionId tx) {
  return utils::make_future(std::async(std::launch::async, [this, tx] {
    auto futures = coordination_->ExecuteOnWorkers<bool>(
        0, [tx](int worker_id, communication::rpc::ClientPool &client_pool) {
          try {
            auto res = client_pool.Call<MakeSnapshotRpc>(tx);
            return res.member;
          } catch (const communication::rpc::RpcFailedException &e) {
            return false;
          }
        });

    bool created = true;
    for (auto &future : futures) {
      created &= future.get();
    }

    return created;
  }));
}

utils::Future<bool> DurabilityRpcMaster::RecoverWalAndIndexes(
    durability::RecoveryData *recovery_data) {
  return utils::make_future(
      std::async(std::launch::async, [this, recovery_data] {
        auto futures = coordination_->ExecuteOnWorkers<bool>(
            0, [recovery_data](int worker_id,
                               communication::rpc::ClientPool &client_pool) {
              try {
                client_pool.Call<RecoverWalAndIndexesRpc>(*recovery_data);
                return true;
              } catch (const communication::rpc::RpcFailedException &e) {
                return false;
              }
            });

        bool recovered = true;
        for (auto &future : futures) {
          recovered &= future.get();
        }

        return recovered;
      }));
}
}  // namespace distributed
