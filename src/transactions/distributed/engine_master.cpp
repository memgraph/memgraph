#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "durability/distributed/state_delta.hpp"
#include "transactions/distributed/engine_master.hpp"
#include "transactions/distributed/engine_rpc_messages.hpp"

namespace tx {

EngineMaster::EngineMaster(distributed::Coordination *coordination,
                           durability::WriteAheadLog *wal)
    : engine_single_node_(wal), coordination_(coordination) {
  coordination_->Register<BeginRpc>(
      [this](auto *req_reader, auto *res_builder) {
        BeginReq req;
        slk::Load(&req, req_reader);
        auto tx = this->Begin();
        BeginRes res(TxAndSnapshot{tx->id_, tx->snapshot()});
        slk::Save(res, res_builder);
      });

  coordination_->Register<AdvanceRpc>(
      [this](auto *req_reader, auto *res_builder) {
        AdvanceReq req;
        slk::Load(&req, req_reader);
        AdvanceRes res(this->Advance(req.member));
        slk::Save(res, res_builder);
      });

  coordination_->Register<CommitRpc>(
      [this](auto *req_reader, auto *res_builder) {
        CommitReq req;
        slk::Load(&req, req_reader);
        this->Commit(*this->RunningTransaction(req.member));
        CommitRes res;
        slk::Save(res, res_builder);
      });

  coordination_->Register<AbortRpc>(
      [this](auto *req_reader, auto *res_builder) {
        AbortReq req;
        slk::Load(&req, req_reader);
        this->Abort(*this->RunningTransaction(req.member));
        AbortRes res;
        slk::Save(res, res_builder);
      });

  coordination_->Register<SnapshotRpc>(
      [this](auto *req_reader, auto *res_builder) {
        // It is guaranteed that the Worker will not be requesting this for a
        // transaction that's done, and that there are no race conditions here.
        SnapshotReq req;
        slk::Load(&req, req_reader);
        SnapshotRes res(this->RunningTransaction(req.member)->snapshot());
        slk::Save(res, res_builder);
      });

  coordination_->Register<CommandRpc>(
      [this](auto *req_reader, auto *res_builder) {
        // It is guaranteed that the Worker will not be requesting this for a
        // transaction that's done, and that there are no race conditions here.
        CommandReq req;
        slk::Load(&req, req_reader);
        CommandRes res(this->RunningTransaction(req.member)->cid());
        slk::Save(res, res_builder);
      });

  coordination_->Register<GcSnapshotRpc>(
      [this](auto *req_reader, auto *res_builder) {
        GcSnapshotReq req;
        slk::Load(&req, req_reader);
        GcSnapshotRes res(this->GlobalGcSnapshot());
        slk::Save(res, res_builder);
      });

  coordination_->Register<ClogInfoRpc>(
      [this](auto *req_reader, auto *res_builder) {
        ClogInfoReq req;
        slk::Load(&req, req_reader);
        ClogInfoRes res(this->Info(req.member));
        slk::Save(res, res_builder);
      });

  coordination_->Register<ActiveTransactionsRpc>(
      [this](auto *req_reader, auto *res_builder) {
        ActiveTransactionsReq req;
        slk::Load(&req, req_reader);
        ActiveTransactionsRes res(this->GlobalActiveTransactions());
        slk::Save(res, res_builder);
      });

  coordination_->Register<EnsureNextIdGreaterRpc>(
      [this](auto *req_reader, auto *res_builder) {
        EnsureNextIdGreaterReq req;
        slk::Load(&req, req_reader);
        this->EnsureNextIdGreater(req.member);
        EnsureNextIdGreaterRes res;
        slk::Save(res, res_builder);
      });

  coordination_->Register<GlobalLastRpc>(
      [this](auto *req_reader, auto *res_builder) {
        GlobalLastReq req;
        slk::Load(&req, req_reader);
        GlobalLastRes res(this->GlobalLast());
        slk::Save(res, res_builder);
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
