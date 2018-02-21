#include <chrono>

#include "glog/logging.h"

#include "transactions/engine_rpc_messages.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/atomic.hpp"

namespace tx {

WorkerEngine::WorkerEngine(const io::network::Endpoint &endpoint)
    : rpc_client_pool_(endpoint, kTransactionEngineRpc) {
  cache_clearing_scheduler_.Run(kCacheReleasePeriod, [this]() {
    // Use the GC snapshot as it always has at least one member.
    auto res = rpc_client_pool_.Call<GcSnapshotRpc>();
    // There is a race-condition between this scheduled call and worker
    // shutdown. It is possible that the worker has responded to the master it
    // is shutting down, and the master is shutting down (and can't responde to
    // RPCs). At the same time this call gets scheduled, so we get a failed RPC.
    if (!res) {
      LOG(WARNING) << "Transaction cache GC RPC call failed";
    } else {
      CHECK(!res->member.empty()) << "Recieved an empty GcSnapshot";
      ClearCachesBasedOnOldest(res->member.front());
    }
  });
}

WorkerEngine::~WorkerEngine() {
  for (auto &kv : active_.access()) {
    delete kv.second;
  }
}

Transaction *WorkerEngine::Begin() {
  auto res = rpc_client_pool_.Call<BeginRpc>();
  Transaction *tx =
      new Transaction(res->member.tx_id, res->member.snapshot, *this);
  auto insertion = active_.access().insert(res->member.tx_id, tx);
  CHECK(insertion.second) << "Failed to start creation from worker";
  return tx;
}

command_id_t WorkerEngine::Advance(transaction_id_t tx_id) {
  auto res = rpc_client_pool_.Call<AdvanceRpc>(tx_id);
  auto access = active_.access();
  auto found = access.find(tx_id);
  CHECK(found != access.end())
      << "Can't advance a transaction not in local cache";
  found->second->cid_ = res->member;
  return res->member;
}

command_id_t WorkerEngine::UpdateCommand(transaction_id_t tx_id) {
  command_id_t cmd_id = rpc_client_pool_.Call<CommandRpc>(tx_id)->member;

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
  auto res = rpc_client_pool_.Call<CommitRpc>(t.id_);
  ClearCache(t.id_);
}

void WorkerEngine::Abort(const Transaction &t) {
  auto res = rpc_client_pool_.Call<AbortRpc>(t.id_);
  ClearCache(t.id_);
}

CommitLog::Info WorkerEngine::Info(transaction_id_t tid) const {
  auto info = clog_.fetch_info(tid);
  // If we don't know the transaction to be commited nor aborted, ask the
  // master about it and update the local commit log.
  if (!(info.is_aborted() || info.is_committed())) {
    // @review: this version of Call is just used because Info has no
    // default constructor.
    info = rpc_client_pool_.Call<ClogInfoRpc>(tid)->member;
    if (!info.is_active()) {
      if (info.is_committed()) clog_.set_committed(tid);
      if (info.is_aborted()) clog_.set_aborted(tid);
      ClearCache(tid);
    }
  }

  return info;
}

Snapshot WorkerEngine::GlobalGcSnapshot() {
  return std::move(rpc_client_pool_.Call<GcSnapshotRpc>()->member);
}

Snapshot WorkerEngine::GlobalActiveTransactions() {
  return std::move(rpc_client_pool_.Call<ActiveTransactionsRpc>()->member);
}

bool WorkerEngine::GlobalIsActive(transaction_id_t tid) const {
  return rpc_client_pool_.Call<IsActiveRpc>(tid)->member;
}

transaction_id_t WorkerEngine::LocalLast() const { return local_last_; }

void WorkerEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  for (auto pair : active_.access()) f(*pair.second);
}

Transaction *WorkerEngine::RunningTransaction(transaction_id_t tx_id) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  Snapshot snapshot(
      std::move(rpc_client_pool_.Call<SnapshotRpc>(tx_id)->member));
  return RunningTransaction(tx_id, snapshot);
}

Transaction *WorkerEngine::RunningTransaction(transaction_id_t tx_id,
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

void WorkerEngine::ClearCache(transaction_id_t tx_id) const {
  auto access = active_.access();
  auto found = access.find(tx_id);
  if (found != access.end()) {
    delete found->second;
    access.remove(tx_id);
  }
  NotifyListeners(tx_id);
}

void WorkerEngine::ClearCachesBasedOnOldest(transaction_id_t oldest_active) {
  // Take care to handle concurrent calls to this correctly. Try to update the
  // oldest_active_, and only if successful (nobody else did it concurrently),
  // clear caches between the previous oldest (now expired) and new oldest
  // (possibly still alive).
  auto previous_oldest = oldest_active_.load();
  while (
      !oldest_active_.compare_exchange_strong(previous_oldest, oldest_active)) {
    ;
  }
  for (tx::transaction_id_t expired = previous_oldest; expired < oldest_active;
       ++expired) {
    ClearCache(expired);
  }
}
}  // namespace tx
