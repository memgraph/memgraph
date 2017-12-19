#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "transactions/engine_master.hpp"
#include "transactions/engine_rpc_messages.hpp"

namespace tx {

MasterEngine::MasterEngine(communication::messaging::System &system) {
  StartServer(system);
}

MasterEngine::~MasterEngine() {
  if (rpc_server_) StopServer();
}

Transaction *MasterEngine::Begin() {
  std::lock_guard<SpinLock> guard(lock_);

  transaction_id_t id{++counter_};
  auto t = new Transaction(id, active_, *this);

  active_.insert(id);
  store_.emplace(id, t);

  return t;
}

void MasterEngine::Advance(transaction_id_t id) {
  std::lock_guard<SpinLock> guard(lock_);

  auto it = store_.find(id);
  DCHECK(it != store_.end())
      << "Transaction::advance on non-existing transaction";

  Transaction *t = it->second.get();
  if (t->cid_ == std::numeric_limits<command_id_t>::max())
    throw TransactionError(
        "Reached maximum number of commands in this "
        "transaction.");

  t->cid_++;
}

void MasterEngine::Commit(const Transaction &t) {
  std::lock_guard<SpinLock> guard(lock_);
  clog_.set_committed(t.id_);
  active_.remove(t.id_);
  store_.erase(store_.find(t.id_));
}

void MasterEngine::Abort(const Transaction &t) {
  std::lock_guard<SpinLock> guard(lock_);
  clog_.set_aborted(t.id_);
  active_.remove(t.id_);
  store_.erase(store_.find(t.id_));
}

CommitLog::Info MasterEngine::Info(transaction_id_t tx) const {
  return clog_.fetch_info(tx);
}

Snapshot MasterEngine::GlobalGcSnapshot() {
  std::lock_guard<SpinLock> guard(lock_);

  // No active transactions.
  if (active_.size() == 0) {
    auto snapshot_copy = active_;
    snapshot_copy.insert(counter_ + 1);
    return snapshot_copy;
  }

  // There are active transactions.
  auto snapshot_copy = store_.find(active_.front())->second->snapshot();
  snapshot_copy.insert(active_.front());
  return snapshot_copy;
}

Snapshot MasterEngine::GlobalActiveTransactions() {
  std::lock_guard<SpinLock> guard(lock_);
  Snapshot active_transactions = active_;
  return active_transactions;
}

bool MasterEngine::GlobalIsActive(transaction_id_t tx) const {
  return clog_.is_active(tx);
}

tx::transaction_id_t MasterEngine::LocalLast() const { return counter_.load(); }

void MasterEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  std::lock_guard<SpinLock> guard(lock_);
  for (auto transaction : active_) {
    f(*store_.find(transaction)->second);
  }
}

void MasterEngine::StartServer(communication::messaging::System &system) {
  CHECK(!rpc_server_) << "Can't start a running server";
  rpc_server_.emplace(system, "tx_engine");

  rpc_server_->Register<SnapshotRpc>([this](const SnapshotReq &req) {
    // It is guaranteed that the Worker will not be requesting this for a
    // transaction that's done, and that there are no race conditions here.
    auto found = store_.find(req.member);
    DCHECK(found != store_.end())
        << "Can't return snapshot for an inactive transaction";
    return std::make_unique<SnapshotRes>(found->second->snapshot());
  });

  rpc_server_->Register<GcSnapshotRpc>(
      [this](const communication::messaging::Message &) {
        return std::make_unique<SnapshotRes>(GlobalGcSnapshot());
      });

  rpc_server_->Register<ClogInfoRpc>([this](const ClogInfoReq &req) {
    return std::make_unique<ClogInfoRes>(Info(req.member));
  });

  rpc_server_->Register<ActiveTransactionsRpc>(
      [this](const communication::messaging::Message &) {
        return std::make_unique<SnapshotRes>(GlobalActiveTransactions());
      });

  rpc_server_->Register<IsActiveRpc>([this](const IsActiveReq &req) {
    return std::make_unique<IsActiveRes>(GlobalIsActive(req.member));
  });

  rpc_server_->Start();
}

void MasterEngine::StopServer() {
  CHECK(rpc_server_) << "Can't stop a server that's not running";
  rpc_server_->Shutdown();
  rpc_server_ = std::experimental::nullopt;
}
}  // namespace tx
