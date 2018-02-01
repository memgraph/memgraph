#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "database/state_delta.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_rpc_messages.hpp"

namespace tx {

MasterEngine::MasterEngine(communication::rpc::System &system,
                           durability::WriteAheadLog *wal)
    : SingleNodeEngine(wal), rpc_server_(system, kTransactionEngineRpc) {
  rpc_server_.Register<BeginRpc>([this](const BeginReq &) {
      auto tx = Begin();
    return std::make_unique<BeginRes>(TxAndSnapshot{tx->id_, tx->snapshot()});
  });

  rpc_server_.Register<AdvanceRpc>([this](const AdvanceReq &req) {
    return std::make_unique<AdvanceRes>(Advance(req.member));
  });

  rpc_server_.Register<CommitRpc>([this](const CommitReq &req) {
      Commit(*RunningTransaction(req.member));
    return std::make_unique<CommitRes>();
  });

  rpc_server_.Register<AbortRpc>([this](const AbortReq &req) {
      Abort(*RunningTransaction(req.member));
    return std::make_unique<AbortRes>();
  });

  rpc_server_.Register<SnapshotRpc>([this](const SnapshotReq &req) {
    // It is guaranteed that the Worker will not be requesting this for a
    // transaction that's done, and that there are no race conditions here.
    return std::make_unique<SnapshotRes>(
        RunningTransaction(req.member)->snapshot());
  });

  rpc_server_.Register<GcSnapshotRpc>(
      [this](const communication::rpc::Message &) {
        return std::make_unique<SnapshotRes>(GlobalGcSnapshot());
      });

  rpc_server_.Register<ClogInfoRpc>([this](const ClogInfoReq &req) {
    return std::make_unique<ClogInfoRes>(Info(req.member));
  });

  rpc_server_.Register<ActiveTransactionsRpc>(
      [this](const communication::rpc::Message &) {
        return std::make_unique<SnapshotRes>(GlobalActiveTransactions());
      });

  rpc_server_.Register<IsActiveRpc>([this](const IsActiveReq &req) {
    return std::make_unique<IsActiveRes>(GlobalIsActive(req.member));
  });
}
}  // namespace tx
