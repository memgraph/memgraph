#include "glog/logging.h"

#include "transactions/engine_rpc_messages.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/atomic.hpp"

namespace tx {
namespace {
static const auto kRpcTimeout = 100ms;
}

WorkerEngine::WorkerEngine(communication::messaging::System &system,
                           const std::string &tx_server_host,
                           uint16_t tx_server_port)
    : rpc_client_(system, tx_server_host, tx_server_port, "tx_engine") {}

Transaction *WorkerEngine::LocalBegin(transaction_id_t tx_id) {
  auto accessor = active_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  Snapshot snapshot(
      std::move(rpc_client_.Call<SnapshotRpc>(kRpcTimeout, tx_id)->member));
  auto insertion =
      accessor.insert(tx_id, new Transaction(tx_id, snapshot, *this));
  CHECK(insertion.second) << "Transaction already inserted";
  utils::EnsureAtomicGe(local_last_, tx_id);
  return insertion.first->second;
}

CommitLog::Info WorkerEngine::Info(transaction_id_t tid) const {
  auto info = clog_.fetch_info(tid);
  // If we don't know the transaction to be commited nor aborted, ask the
  // master about it and update the local commit log.
  if (!(info.is_aborted() || info.is_committed())) {
    // @review: this version of Call is just used because Info has no default
    // constructor.
    info = rpc_client_.Call<ClogInfoRpc>(kRpcTimeout, tid)->member;
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
  return std::move(rpc_client_.Call<GcSnapshotRpc>(kRpcTimeout)->member);
}

Snapshot WorkerEngine::GlobalActiveTransactions() {
  return std::move(
      rpc_client_.Call<ActiveTransactionsRpc>(kRpcTimeout)->member);
}

bool WorkerEngine::GlobalIsActive(transaction_id_t tid) const {
  return rpc_client_.Call<IsActiveRpc>(kRpcTimeout, tid)->member;
}

tx::transaction_id_t WorkerEngine::LocalLast() const { return local_last_; }

void WorkerEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  for (auto pair : active_.access()) f(*pair.second);
}

}  // namespace tx
