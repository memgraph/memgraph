#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "database/state_delta.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_rpc_messages.hpp"

namespace tx {

MasterEngine::MasterEngine(communication::rpc::Server &server,
                           distributed::RpcWorkerClients &rpc_worker_clients,
                           durability::WriteAheadLog *wal)
    : SingleNodeEngine(wal),
      rpc_server_(server),
      ongoing_produce_joiner_(rpc_worker_clients) {
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

  rpc_server_.Register<CommandRpc>([this](const CommandReq &req) {
    // It is guaranteed that the Worker will not be requesting this for a
    // transaction that's done, and that there are no race conditions here.
    return std::make_unique<CommandRes>(RunningTransaction(req.member)->cid());
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

  rpc_server_.Register<EnsureNextIdGreaterRpc>(
      [this](const EnsureNextIdGreaterReq &req) {
        EnsureNextIdGreater(req.member);
        return std::make_unique<EnsureNextIdGreaterRes>();
      });

  rpc_server_.Register<GlobalLastRpc>([this](const GlobalLastReq &) {
    return std::make_unique<GlobalLastRes>(GlobalLast());
  });
}

void MasterEngine::Commit(const Transaction &t) {
  ongoing_produce_joiner_.JoinOngoingProduces(t.id_);
  SingleNodeEngine::Commit(t);
}

void MasterEngine::Abort(const Transaction &t) {
  ongoing_produce_joiner_.JoinOngoingProduces(t.id_);
  SingleNodeEngine::Abort(t);
}

}  // namespace tx
