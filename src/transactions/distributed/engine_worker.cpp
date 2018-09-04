#include <chrono>

#include "glog/logging.h"

#include "transactions/distributed/engine_rpc_messages.hpp"
#include "transactions/distributed/engine_worker.hpp"
#include "utils/atomic.hpp"

namespace tx {

EngineWorker::EngineWorker(communication::rpc::Server &server,
                           communication::rpc::ClientPool &master_client_pool,
                           durability::WriteAheadLog *wal)
    : server_(server), master_client_pool_(master_client_pool), wal_(wal) {
  // Register our `NotifyCommittedRpc` server. This RPC should only write the
  // `TxCommit` operation into the WAL. It is only used to indicate that the
  // transaction has succeeded on all workers and that it will be committed on
  // the master. When recovering the cluster from WALs the `TxCommit` operation
  // indicates that all operations for a given transaction were written to the
  // WAL. If we wouldn't have the `TxCommit` after all operations for a given
  // transaction in the WAL we couldn't be sure that all operations were saved
  // (eg. some operations could have been written into one WAL file, some to
  // another file). This way we ensure that if the `TxCommit` is written it was
  // the last thing associated with that transaction and everything before was
  // flushed to the disk.
  // NOTE: We can't cache the commit state for this transaction because this
  // RPC call could fail on other workers which will cause the transaction to be
  // aborted. This mismatch in committed/aborted across workers is resolved by
  // using the master as a single source of truth when doing recovery.
  server_.Register<NotifyCommittedRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        auto tid = req_reader.getMember();
        if (wal_) {
          wal_->Emplace(database::StateDelta::TxCommit(tid));
        }
      });
}

EngineWorker::~EngineWorker() {
  for (auto &kv : active_.access()) {
    delete kv.second;
  }
}

Transaction *EngineWorker::Begin() {
  auto res = master_client_pool_.Call<BeginRpc>();
  CHECK(res) << "BeginRpc failed";
  auto &data = res->member;
  UpdateOldestActive(data.snapshot, data.tx_id);
  Transaction *tx = CreateTransaction(data.tx_id, data.snapshot);
  auto insertion = active_.access().insert(data.tx_id, tx);
  CHECK(insertion.second) << "Failed to start creation from worker";
  VLOG(11) << "[Tx] Starting worker transaction " << data.tx_id;
  return tx;
}

CommandId EngineWorker::Advance(TransactionId tx_id) {
  auto res = master_client_pool_.Call<AdvanceRpc>(tx_id);
  CHECK(res) << "AdvanceRpc failed";
  auto access = active_.access();
  auto found = access.find(tx_id);
  CHECK(found != access.end())
      << "Can't advance a transaction not in local cache";
  SetCommand(found->second, res->member);
  return res->member;
}

CommandId EngineWorker::UpdateCommand(TransactionId tx_id) {
  auto res = master_client_pool_.Call<CommandRpc>(tx_id);
  CHECK(res) << "CommandRpc failed";
  auto cmd_id = res->member;

  // Assume there is no concurrent work being done on this worker in the given
  // transaction. This assumption is sound because command advancing needs to be
  // done in a synchronized fashion, while no workers are executing in that
  // transaction. That assumption lets us freely modify the command id in the
  // cached transaction object, and ensures there are no race conditions on
  // caching a transaction object if it wasn't cached already.

  auto access = active_.access();
  auto found = access.find(tx_id);
  if (found != access.end()) {
    SetCommand(found->second, cmd_id);
  }
  return cmd_id;
}

void EngineWorker::Commit(const Transaction &t) {
  auto res = master_client_pool_.Call<CommitRpc>(t.id_);
  CHECK(res) << "CommitRpc failed";
  VLOG(11) << "[Tx] Commiting worker transaction " << t.id_;
  ClearSingleTransaction(t.id_);
}

void EngineWorker::Abort(const Transaction &t) {
  auto res = master_client_pool_.Call<AbortRpc>(t.id_);
  CHECK(res) << "AbortRpc failed";
  VLOG(11) << "[Tx] Aborting worker transaction " << t.id_;
  ClearSingleTransaction(t.id_);
}

CommitLog::Info EngineWorker::Info(TransactionId tid) const {
  auto info = clog_.fetch_info(tid);
  // If we don't know the transaction to be commited nor aborted, ask the
  // master about it and update the local commit log.
  if (!(info.is_aborted() || info.is_committed())) {
    // @review: this version of Call is just used because Info has no
    // default constructor.
    auto res = master_client_pool_.Call<ClogInfoRpc>(tid);
    CHECK(res) << "ClogInfoRpc failed";
    info = res->member;
    if (!info.is_active()) {
      if (info.is_committed()) clog_.set_committed(tid);
      if (info.is_aborted()) clog_.set_aborted(tid);
      ClearSingleTransaction(tid);
    }
  }

  return info;
}

Snapshot EngineWorker::GlobalGcSnapshot() {
  auto res = master_client_pool_.Call<GcSnapshotRpc>();
  CHECK(res) << "GcSnapshotRpc failed";
  auto snapshot = std::move(res->member);
  UpdateOldestActive(snapshot, local_last_.load());
  return snapshot;
}

Snapshot EngineWorker::GlobalActiveTransactions() {
  auto res = master_client_pool_.Call<ActiveTransactionsRpc>();
  CHECK(res) << "ActiveTransactionsRpc failed";
  auto snapshot = std::move(res->member);
  UpdateOldestActive(snapshot, local_last_.load());
  return snapshot;
}

TransactionId EngineWorker::LocalLast() const { return local_last_; }

TransactionId EngineWorker::GlobalLast() const {
  auto res = master_client_pool_.Call<GlobalLastRpc>();
  CHECK(res) << "GlobalLastRpc failed";
  return res->member;
}

void EngineWorker::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  for (auto pair : active_.access()) f(*pair.second);
}

TransactionId EngineWorker::LocalOldestActive() const { return oldest_active_; }

Transaction *EngineWorker::RunningTransaction(TransactionId tx_id) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  auto res = master_client_pool_.Call<SnapshotRpc>(tx_id);
  CHECK(res) << "SnapshotRpc failed";
  auto snapshot = std::move(res->member);
  UpdateOldestActive(snapshot, local_last_.load());
  return RunningTransaction(tx_id, snapshot);
}

Transaction *EngineWorker::RunningTransaction(TransactionId tx_id,
                                              const Snapshot &snapshot) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  auto new_tx = CreateTransaction(tx_id, snapshot);
  auto insertion = accessor.insert(tx_id, new_tx);
  if (!insertion.second) delete new_tx;
  utils::EnsureAtomicGe(local_last_, tx_id);
  return insertion.first->second;
}

void EngineWorker::ClearTransactionalCache(TransactionId oldest_active) {
  auto access = active_.access();
  for (auto kv : access) {
    if (kv.first < oldest_active) {
      auto transaction_ptr = kv.second;
      if (access.remove(kv.first)) {
        delete transaction_ptr;
      }
    }
  }
}

void EngineWorker::ClearSingleTransaction(TransactionId tx_id) const {
  auto access = active_.access();
  auto found = access.find(tx_id);
  if (found != access.end()) {
    auto transaction_ptr = found->second;
    if (access.remove(found->first)) {
      delete transaction_ptr;
    }
  }
}

void EngineWorker::UpdateOldestActive(const Snapshot &snapshot,
                                      TransactionId alternative) {
  if (snapshot.empty()) {
    oldest_active_.store(std::max(alternative, oldest_active_.load()));
  } else {
    oldest_active_.store(snapshot.front());
  }
}

void EngineWorker::EnsureNextIdGreater(TransactionId tx_id) {
  master_client_pool_.Call<EnsureNextIdGreaterRpc>(tx_id);
}

void EngineWorker::GarbageCollectCommitLog(TransactionId tx_id) {
  clog_.garbage_collect_older(tx_id);
}
}  // namespace tx
