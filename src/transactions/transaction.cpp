#include "transactions/transaction.hpp"

#include "transactions/engine.hpp"

namespace tx
{

Transaction::Transaction(const Id &id, const Snapshot<Id> &snapshot,
                         Engine &engine)
    : id(id), cid(1), snapshot(snapshot), engine(engine)
{
}

void Transaction::take_lock(RecordLock &lock) { locks.take(&lock, id); }

void Transaction::commit() { engine.commit(*this); }

void Transaction::abort() { engine.abort(*this); }

}
