#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "database/state_delta.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_rpc_messages.hpp"

namespace tx {

MasterEngine::MasterEngine(communication::messaging::System &system,
                           durability::WriteAheadLog *wal)
    : SingleNodeEngine(wal), rpc_server_(system, kTransactionEngineRpc) {
  rpc_server_.Register<SnapshotRpc>([this](const SnapshotReq &req) {
    // It is guaranteed that the Worker will not be requesting this for a
    // transaction that's done, and that there are no race conditions here.
    return std::make_unique<SnapshotRes>(GetSnapshot(req.member));
  });

  rpc_server_.Register<GcSnapshotRpc>(
      [this](const communication::messaging::Message &) {
        return std::make_unique<SnapshotRes>(GlobalGcSnapshot());
      });

  rpc_server_.Register<ClogInfoRpc>([this](const ClogInfoReq &req) {
    return std::make_unique<ClogInfoRes>(Info(req.member));
  });

  rpc_server_.Register<ActiveTransactionsRpc>(
      [this](const communication::messaging::Message &) {
        return std::make_unique<SnapshotRes>(GlobalActiveTransactions());
      });

  rpc_server_.Register<IsActiveRpc>([this](const IsActiveReq &req) {
    return std::make_unique<IsActiveRes>(GlobalIsActive(req.member));
  });
}
}  // namespace tx
