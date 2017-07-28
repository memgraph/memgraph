#include "transactions/transaction.hpp"

#include "transactions/engine.hpp"

namespace tx {

Transaction::Transaction(transaction_id_t id, const Snapshot &snapshot,
                         Engine &engine)
    : id_(id), engine_(engine), snapshot_(snapshot) {}

void Transaction::TakeLock(RecordLock &lock) { locks_.Take(&lock, id_); }

void Transaction::Commit() { engine_.Commit(*this); }

void Transaction::Abort() { engine_.Abort(*this); }
}
