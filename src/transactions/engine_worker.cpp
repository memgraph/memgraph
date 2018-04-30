#include <chrono>

#include "glog/logging.h"

#include "transactions/engine_rpc_messages.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/atomic.hpp"

namespace tx {

WorkerEngine::WorkerEngine(communication::rpc::ClientPool &master_client_pool)
    : master_client_pool_(master_client_pool) {}

WorkerEngine::~WorkerEngine() {
  for (auto &kv : active_.access()) {
    delete kv.second;
  }
}

Transaction *WorkerEngine::Begin() {
  auto res = master_client_pool_.Call<BeginRpc>();
  CHECK(res) << "BeginRpc failed";
  auto &data = res->member;
  UpdateOldestActive(data.snapshot, data.tx_id);
  Transaction *tx = new Transaction(data.tx_id, data.snapshot, *this);
  auto insertion = active_.access().insert(data.tx_id, tx);
  CHECK(insertion.second) << "Failed to start creation from worker";
  VLOG(11) << "[Tx] Starting worker transaction " << data.tx_id;
  return tx;
}

CommandId WorkerEngine::Advance(TransactionId tx_id) {
  auto res = master_client_pool_.Call<AdvanceRpc>(tx_id);
  CHECK(res) << "AdvanceRpc failed";
  auto access = active_.access();
  auto found = access.find(tx_id);
  CHECK(found != access.end())
      << "Can't advance a transaction not in local cache";
  found->second->cid_ = res->member;
  return res->member;
}

CommandId WorkerEngine::UpdateCommand(TransactionId tx_id) {
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
    found->second->cid_ = cmd_id;
  }
  return cmd_id;
}

void WorkerEngine::Commit(const Transaction &t) {
  auto res = master_client_pool_.Call<CommitRpc>(t.id_);
  CHECK(res) << "CommitRpc failed";
  VLOG(11) << "[Tx] Commiting worker transaction " << t.id_;
  ClearSingleTransaction(t.id_);
}

void WorkerEngine::Abort(const Transaction &t) {
  auto res = master_client_pool_.Call<AbortRpc>(t.id_);
  CHECK(res) << "AbortRpc failed";
  VLOG(11) << "[Tx] Aborting worker transaction " << t.id_;
  ClearSingleTransaction(t.id_);
}

CommitLog::Info WorkerEngine::Info(TransactionId tid) const {
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

Snapshot WorkerEngine::GlobalGcSnapshot() {
  auto res = master_client_pool_.Call<GcSnapshotRpc>();
  CHECK(res) << "GcSnapshotRpc failed";
  auto snapshot = std::move(res->member);
  UpdateOldestActive(snapshot, local_last_.load());
  return snapshot;
}

Snapshot WorkerEngine::GlobalActiveTransactions() {
  auto res = master_client_pool_.Call<ActiveTransactionsRpc>();
  CHECK(res) << "ActiveTransactionsRpc failed";
  auto snapshot = std::move(res->member);
  UpdateOldestActive(snapshot, local_last_.load());
  return snapshot;
}

TransactionId WorkerEngine::LocalLast() const { return local_last_; }
TransactionId WorkerEngine::GlobalLast() const {
  auto res = master_client_pool_.Call<GlobalLastRpc>();
  CHECK(res) << "GlobalLastRpc failed";
  return res->member;
}

void WorkerEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  for (auto pair : active_.access()) f(*pair.second);
}

TransactionId WorkerEngine::LocalOldestActive() const { return oldest_active_; }

Transaction *WorkerEngine::RunningTransaction(TransactionId tx_id) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  auto res = master_client_pool_.Call<SnapshotRpc>(tx_id);
  CHECK(res) << "SnapshotRpc failed";
  auto snapshot = std::move(res->member);
  UpdateOldestActive(snapshot, local_last_.load());
  return RunningTransaction(tx_id, snapshot);
}

Transaction *WorkerEngine::RunningTransaction(TransactionId tx_id,
                                              const Snapshot &snapshot) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  auto new_tx = new Transaction(tx_id, snapshot, *this);
  auto insertion = accessor.insert(tx_id, new_tx);
  if (!insertion.second) delete new_tx;
  utils::EnsureAtomicGe(local_last_, tx_id);
  return insertion.first->second;
}

void WorkerEngine::ClearTransactionalCache(TransactionId oldest_active) const {
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

void WorkerEngine::ClearSingleTransaction(TransactionId tx_id) const {
  auto access = active_.access();
  auto found = access.find(tx_id);
  if (found != access.end()) {
    auto transaction_ptr = found->second;
    if (access.remove(found->first)) {
      delete transaction_ptr;
    }
  }
}

void WorkerEngine::UpdateOldestActive(const Snapshot &snapshot,
                                      TransactionId alternative) {
  if (snapshot.empty()) {
    oldest_active_.store(std::max(alternative, oldest_active_.load()));
  } else {
    oldest_active_.store(snapshot.front());
  }
}

void WorkerEngine::EnsureNextIdGreater(TransactionId tx_id) {
  master_client_pool_.Call<EnsureNextIdGreaterRpc>(tx_id);
}

void WorkerEngine::GarbageCollectCommitLog(TransactionId tx_id) {
  clog_.garbage_collect_older(tx_id);
}
}  // namespace tx
