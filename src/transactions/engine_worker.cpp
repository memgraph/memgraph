#include "glog/logging.h"

#include "transactions/engine_worker.hpp"
#include "utils/atomic.hpp"

namespace tx {
Transaction *WorkerEngine::LocalBegin(transaction_id_t tx_id) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  Snapshot snapshot = rpc_.Call<Snapshot>("Snapshot", tx_id);
  auto *tx = new Transaction(tx_id, snapshot, *this);
  auto insertion = accessor.insert(tx_id, tx);
  if (!insertion.second) {
    delete tx;
    tx = insertion.first->second;
  }
  utils::EnsureAtomicGe(local_last_, tx_id);
  return tx;
}

CommitLog::Info WorkerEngine::Info(transaction_id_t tid) const {
  auto info = clog_.fetch_info(tid);
  // If we don't know the transaction to be commited nor aborted, ask the
  // master about it and update the local commit log.
  if (!(info.is_aborted() || info.is_committed())) {
    // @review: this version of Call is just used because Info has no default
    // constructor.
    info = rpc_.Call<CommitLog::Info>("Info", info, tid);
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
  return rpc_.Call<Snapshot>("Snapshot");
}

Snapshot WorkerEngine::GlobalActiveTransactions() {
  return rpc_.Call<Snapshot>("Active");
}

bool WorkerEngine::GlobalIsActive(transaction_id_t tid) const {
  return rpc_.Call<bool>("GlobalIsActive", tid);
}

tx::transaction_id_t WorkerEngine::LocalLast() const { return local_last_; }

void WorkerEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  for (auto pair : active_.access()) f(*pair.second);
}

}  // namespace tx
