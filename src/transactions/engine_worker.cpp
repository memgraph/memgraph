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

void WorkerEngine::Commit(const Transaction &t) {
  auto res = rpc_client_pool_.Call<CommitRpc>(t.id_);
  auto removal = active_.access().remove(t.id_);
  CHECK(removal) << "Can't commit a transaction not in local cache";
}

void WorkerEngine::Abort(const Transaction &t) {
  auto res = rpc_client_pool_.Call<AbortRpc>(t.id_);
  auto removal = active_.access().remove(t.id_);
  CHECK(removal) << "Can't abort a transaction not in local cache";
}

CommitLog::Info WorkerEngine::Info(transaction_id_t tid) const {
  auto info = clog_.fetch_info(tid);
  // If we don't know the transaction to be commited nor aborted, ask the
  // master about it and update the local commit log.
  if (!(info.is_aborted() || info.is_committed())) {
    // @review: this version of Call is just used because Info has no
    // default constructor.
    info = rpc_client_pool_.Call<ClogInfoRpc>(tid)->member;
    DCHECK(info.is_committed() || info.is_aborted())
        << "It is expected that the transaction is not running anymore. This "
           "function should be used only after the snapshot of the current "
           "transaction is checked (in MVCC).";
    if (info.is_committed()) clog_.set_committed(tid);
    if (info.is_aborted()) clog_.set_aborted(tid);
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
  auto insertion =
      accessor.insert(tx_id, new Transaction(tx_id, snapshot, *this));
  utils::EnsureAtomicGe(local_last_, tx_id);
  return insertion.first->second;
}

}  // namespace tx
