#include "glog/logging.h"

#include "transactions/engine_rpc_messages.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/atomic.hpp"

namespace tx {

WorkerEngine::WorkerEngine(const io::network::Endpoint &endpoint)
    : rpc_client_pool_(endpoint, kTransactionEngineRpc) {}

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

tx::transaction_id_t WorkerEngine::LocalLast() const { return local_last_; }

void WorkerEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  for (auto pair : active_.access()) f(*pair.second);
}

tx::Transaction *WorkerEngine::RunningTransaction(tx::transaction_id_t tx_id) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  Snapshot snapshot(
      std::move(rpc_client_pool_.Call<SnapshotRpc>(tx_id)->member));
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
}
}  // namespace tx
