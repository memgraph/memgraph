#include "transactions/transaction.hpp"

#include <chrono>  // std::chrono::seconds

#include <thread>  // std::this_thread::sleep_for

#include "transactions/engine.hpp"

namespace tx {
Transaction::Transaction(transaction_id_t id, const Snapshot &snapshot,
                         Engine &engine)
    : id_(id), engine_(engine), snapshot_(snapshot) {}

void Transaction::TakeLock(RecordLock &lock) { locks_.take(&lock, id_); }

void Transaction::Commit() { engine_.Commit(*this); }

void Transaction::Abort() { engine_.Abort(*this); }
}
