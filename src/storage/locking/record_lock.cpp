#include "storage/locking/record_lock.hpp"

#include <fmt/format.h>
#include <glog/logging.h>
#include <experimental/optional>
#include <stack>
#include <unordered_set>

#include "threading/sync/lock_timeout_exception.hpp"
#include "transactions/engine.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/timer.hpp"

namespace {

// Finds lock cycle that start transaction is a part of and returns id of oldest
// transaction in that cycle. If start transaction is not in a cycle nullopt is
// returned.
template <typename TAccessor>
std::experimental::optional<tx::transaction_id_t> FindOldestTxInLockCycle(
    tx::transaction_id_t start, TAccessor &graph_accessor) {
  std::vector<tx::transaction_id_t> path;
  std::unordered_set<tx::transaction_id_t> visited;

  auto current = start;

  do {
    visited.insert(current);
    path.push_back(current);
    auto it = graph_accessor.find(current);
    if (it == graph_accessor.end()) return std::experimental::nullopt;
    current = it->second;
  } while (visited.find(current) == visited.end());

  if (current == start) {
    // start is a part of the cycle, return oldest transaction.
    CHECK(path.size() >= 2U) << "Cycle must have at least two nodes";
    return *std::min(path.begin(), path.end());
  }

  // There is a cycle, but start is not a part of it. Some transaction that is
  // in a cycle will find it and abort oldest transaction.
  return std::experimental::nullopt;
}

}  // namespace

LockStatus RecordLock::Lock(const tx::Transaction &tx, tx::Engine &engine) {
  if (lock_.try_lock()) {
    owner_ = tx.id_;
    return LockStatus::Acquired;
  }

  tx::transaction_id_t owner = owner_;
  if (owner_ == tx.id_) return LockStatus::AlreadyHeld;

  // In a distributed worker the transaction objects (and the locks they own)
  // are not destructed at the same time like on the master. Consequently a lock
  // might be active for a dead transaction. By asking the transaction engine
  // for transaction info, we'll make the worker refresh it's knowledge about
  // live transactions and release obsolete locks.
  auto info = engine.Info(owner);
  if (!info.is_active()) {
    if (lock_.try_lock()) {
      owner_ = tx.id_;
      return LockStatus::Acquired;
    }
  }

  // Insert edge into local lock_graph.
  auto accessor = engine.local_lock_graph().access();
  auto it = accessor.insert(tx.id_, owner).first;

  auto abort_oldest_tx_in_lock_cycle = [&tx, &accessor, &engine]() {
    // Find oldest transaction in lock cycle if cycle exists and notify that
    // transaction that it should abort.
    // TODO: maybe we can be smarter and abort some other transaction and not
    // the oldest one.
    auto oldest = FindOldestTxInLockCycle(tx.id_, accessor);
    if (oldest) {
      engine.LocalForEachActiveTransaction([&](tx::Transaction &t) {
        if (t.id_ == oldest) {
          t.set_should_abort();
        }
      });
    }
  };

  abort_oldest_tx_in_lock_cycle();

  // Make sure to erase edge on function exit. Either function will throw and
  // transaction will be killed so we should erase the edge because transaction
  // won't exist anymore or owner_ will finish and we will be able to acquire
  // the lock.
  utils::OnScopeExit cleanup{[&tx, &accessor] { accessor.remove(tx.id_); }};

  utils::Timer t;
  while (t.Elapsed() < kTimeout) {
    if (tx.should_abort()) {
      // Message could be incorrect. Transaction could be aborted because it was
      // running for too long time, but that is unlikely and it is not very
      // important which exception (and message) we throw here.
      throw LockTimeoutException(
          "Transaction was aborted since it was oldest in a lock cycle");
    }
    if (lock_.try_lock()) {
      owner_ = tx.id_;
      return LockStatus::Acquired;
    }
    if (owner != owner_) {
      // Owner changed while we were spinlocking. Update the edge and rerun
      // cycle resolution routine.
      // TODO: we should make sure that first transaction that tries to acquire
      // already held lock succeeds in acquiring the lock once transaction that
      // was lock owner finishes. That would probably reduce number of aborted
      // transactions.
      owner = owner_;
      it->second = owner;
      abort_oldest_tx_in_lock_cycle();
    }
    cpu_relax();
  }

  throw LockTimeoutException(fmt::format(
      "Transaction locked for more than {} seconds", kTimeout.count()));
}

void RecordLock::Unlock() { lock_.unlock(); }

constexpr std::chrono::duration<double> RecordLock::kTimeout;
