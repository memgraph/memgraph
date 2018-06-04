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
  rpc_server_.Register<BeginRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        auto tx = this->Begin();
        BeginRes res(TxAndSnapshot{tx->id_, tx->snapshot()});
        res.Save(res_builder);
      });

  rpc_server_.Register<AdvanceRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        AdvanceRes res(this->Advance(req_reader.getMember()));
        res.Save(res_builder);
      });

  rpc_server_.Register<CommitRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->Commit(*this->RunningTransaction(req_reader.getMember()));
      });

  rpc_server_.Register<AbortRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->Abort(*this->RunningTransaction(req_reader.getMember()));
      });

  rpc_server_.Register<SnapshotRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        // It is guaranteed that the Worker will not be requesting this for a
        // transaction that's done, and that there are no race conditions here.
        SnapshotRes res(
            this->RunningTransaction(req_reader.getMember())->snapshot());
        res.Save(res_builder);
      });

  rpc_server_.Register<CommandRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        // It is guaranteed that the Worker will not be requesting this for a
        // transaction that's done, and that there are no race conditions here.
        CommandRes res(this->RunningTransaction(req_reader.getMember())->cid());
        res.Save(res_builder);
      });

  rpc_server_.Register<GcSnapshotRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        GcSnapshotRes res(this->GlobalGcSnapshot());
        res.Save(res_builder);
      });

  rpc_server_.Register<ClogInfoRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        ClogInfoRes res(this->Info(req_reader.getMember()));
        res.Save(res_builder);
      });

  rpc_server_.Register<ActiveTransactionsRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        ActiveTransactionsRes res(this->GlobalActiveTransactions());
        res.Save(res_builder);
      });

  rpc_server_.Register<EnsureNextIdGreaterRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->EnsureNextIdGreater(req_reader.getMember());
      });

  rpc_server_.Register<GlobalLastRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        GlobalLastRes res(this->GlobalLast());
        res.Save(res_builder);
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
