#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "database/state_delta.hpp"
#include "transactions/distributed/engine_master.hpp"
#include "transactions/distributed/engine_rpc_messages.hpp"

namespace tx {

EngineMaster::EngineMaster(communication::rpc::Server &server,
                           distributed::RpcWorkerClients &rpc_worker_clients,
                           durability::WriteAheadLog *wal)
    : engine_single_node_(wal),
      rpc_server_(server),
      rpc_worker_clients_(rpc_worker_clients) {
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

Transaction *EngineMaster::Begin() { return engine_single_node_.Begin(); }

CommandId EngineMaster::Advance(TransactionId id) {
  return engine_single_node_.Advance(id);
}

CommandId EngineMaster::UpdateCommand(TransactionId id) {
  return engine_single_node_.UpdateCommand(id);
}

void EngineMaster::Commit(const Transaction &t) {
  auto tx_id = t.id_;
  auto futures = rpc_worker_clients_.ExecuteOnWorkers<void>(
      0, [tx_id](int worker_id, communication::rpc::ClientPool &client_pool) {
        auto result = client_pool.Call<NotifyCommittedRpc>(tx_id);
        CHECK(result)
            << "[NotifyCommittedRpc] failed to notify that transaction "
            << tx_id << " committed";
      });

  // We need to wait for all workers to destroy pending futures to avoid
  // using already destroyed (released) transaction objects.
  for (auto &future : futures) {
    future.wait();
  }

  engine_single_node_.Commit(t);
}

void EngineMaster::Abort(const Transaction &t) { engine_single_node_.Abort(t); }

CommitLog::Info EngineMaster::Info(TransactionId tx) const {
  return engine_single_node_.Info(tx);
}

Snapshot EngineMaster::GlobalGcSnapshot() {
  return engine_single_node_.GlobalGcSnapshot();
}

Snapshot EngineMaster::GlobalActiveTransactions() {
  return engine_single_node_.GlobalActiveTransactions();
}

TransactionId EngineMaster::LocalLast() const {
  return engine_single_node_.LocalLast();
}

TransactionId EngineMaster::GlobalLast() const {
  return engine_single_node_.GlobalLast();
}

TransactionId EngineMaster::LocalOldestActive() const {
  return engine_single_node_.LocalOldestActive();
}

void EngineMaster::GarbageCollectCommitLog(TransactionId tx_id) {
  return engine_single_node_.GarbageCollectCommitLog(tx_id);
}

void EngineMaster::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  return engine_single_node_.LocalForEachActiveTransaction(f);
}

Transaction *EngineMaster::RunningTransaction(TransactionId tx_id) {
  return engine_single_node_.RunningTransaction(tx_id);
}

void EngineMaster::EnsureNextIdGreater(TransactionId tx_id) {
  return engine_single_node_.EnsureNextIdGreater(tx_id);
}

void EngineMaster::ClearTransactionalCache(TransactionId oldest_active) {}

}  // namespace tx
