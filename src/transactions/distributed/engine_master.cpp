#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "durability/distributed/state_delta.hpp"
#include "transactions/distributed/engine_master.hpp"
#include "transactions/distributed/engine_rpc_messages.hpp"

namespace tx {

EngineMaster::EngineMaster(communication::rpc::Server *server,
                           distributed::Coordination *coordination,
                           durability::WriteAheadLog *wal)
    : engine_single_node_(wal),
      server_(server),
      coordination_(coordination) {
  server_->Register<BeginRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        auto tx = this->Begin();
        BeginRes res(TxAndSnapshot{tx->id_, tx->snapshot()});
        Save(res, res_builder);
      });

  server_->Register<AdvanceRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        AdvanceRes res(this->Advance(req_reader.getMember()));
        Save(res, res_builder);
      });

  server_->Register<CommitRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->Commit(*this->RunningTransaction(req_reader.getMember()));
      });

  server_->Register<AbortRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->Abort(*this->RunningTransaction(req_reader.getMember()));
      });

  server_->Register<SnapshotRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        // It is guaranteed that the Worker will not be requesting this for a
        // transaction that's done, and that there are no race conditions here.
        SnapshotRes res(
            this->RunningTransaction(req_reader.getMember())->snapshot());
        Save(res, res_builder);
      });

  server_->Register<CommandRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        // It is guaranteed that the Worker will not be requesting this for a
        // transaction that's done, and that there are no race conditions here.
        CommandRes res(this->RunningTransaction(req_reader.getMember())->cid());
        Save(res, res_builder);
      });

  server_->Register<GcSnapshotRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        GcSnapshotRes res(this->GlobalGcSnapshot());
        Save(res, res_builder);
      });

  server_->Register<ClogInfoRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        ClogInfoRes res(this->Info(req_reader.getMember()));
        Save(res, res_builder);
      });

  server_->Register<ActiveTransactionsRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        ActiveTransactionsRes res(this->GlobalActiveTransactions());
        Save(res, res_builder);
      });

  server_->Register<EnsureNextIdGreaterRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->EnsureNextIdGreater(req_reader.getMember());
      });

  server_->Register<GlobalLastRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        GlobalLastRes res(this->GlobalLast());
        Save(res, res_builder);
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
  auto futures = coordination_->ExecuteOnWorkers<void>(
      0, [tx_id](int worker_id, communication::rpc::ClientPool &client_pool) {
        client_pool.Call<NotifyCommittedRpc>(tx_id);
      });

  // We need to wait for all workers to destroy pending futures to avoid
  // using already destroyed (released) transaction objects.
  for (auto &future : futures) {
    future.get();
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
