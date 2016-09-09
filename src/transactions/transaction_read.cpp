#include "transactions/transaction_read.hpp"

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

bool TransactionRead::in_snapshot(const Id &id) const
{
    return snapshot.is_active(id);
}

Id TransactionRead::oldest_active()
{
    return snapshot.oldest_active().take_or(Id(id));
}
}
