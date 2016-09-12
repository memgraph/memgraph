#include "transactions/transaction_read.hpp"

#include "transactions/engine.hpp"

namespace tx
{

TransactionRead::TransactionRead(Engine &engine)
    : TransactionRead(Id(), Snapshot<Id>(), engine)
{
}

TransactionRead::TransactionRead(const Id &&id, const Snapshot<Id> &&snapshot,
                                 Engine &engine)
    : id(id), cid(1), snapshot(std::move(snapshot)), engine(engine)
{
}

TransactionRead::TransactionRead(const Id &id, const Snapshot<Id> &snapshot,
                                 Engine &engine)
    : id(id), cid(1), snapshot(snapshot), engine(engine)
{
}

bool TransactionRead::all_finished()
{
    return !engine.clog.is_active(id) && snapshot.all_finished(engine);
}

bool TransactionRead::in_snapshot(const Id &id) const
{
    return snapshot.is_active(id);
}

Id TransactionRead::oldest_active()
{
    return snapshot.oldest_active().take_or(Id(id));
}
}
