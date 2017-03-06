#include "transactions/transaction.hpp"

#include <chrono>  // std::chrono::seconds

#include <thread>  // std::this_thread::sleep_for

#include "transactions/engine.hpp"

namespace tx {
Transaction::Transaction(Engine &engine)
    : Transaction(Id(), Snapshot<Id>(), engine) {}

Transaction::Transaction(const Id &&id, const Snapshot<Id> &&snapshot,
                         Engine &engine)
    : id(id), cid(1), engine(engine), snapshot(std::move(snapshot)) {}

Transaction::Transaction(const Id &id, const Snapshot<Id> &snapshot,
                         Engine &engine)
    : id(id), cid(1), engine(engine), snapshot(snapshot) {}

void Transaction::wait_for_active() {
  while (snapshot.size() > 0) {
    auto sid = snapshot.back();
    while (engine.clog.fetch_info(sid).is_active()) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    snapshot.remove(sid);
  }
}

void Transaction::take_lock(RecordLock &lock) { locks.take(&lock, id); }

void Transaction::commit() { engine.commit(*this); }

void Transaction::abort() { engine.abort(*this); }

bool Transaction::all_finished() {
  return !engine.clog.is_active(id) && snapshot.all_finished(engine);
}

bool Transaction::in_snapshot(const Id &id) const {
  return snapshot.is_active(id);
}

Id Transaction::oldest_active() {
  return snapshot.oldest_active().take_or(Id(id));
}
}
