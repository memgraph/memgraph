#include "transactions/transaction_id.hpp"

namespace tx
{

TransactionId::TransactionId(Engine &engine)
    : TransactionId(Id(), Snapshot<Id>(), engine)
{
}

TransactionId::TransactionId(const Id &&id, const Snapshot<Id> &&snapshot,
                             Engine &engine)
    : id(id), cid(1), snapshot(std::move(snapshot)), engine(engine)
{
}

TransactionId::TransactionId(const Id &id, const Snapshot<Id> &snapshot,
                             Engine &engine)
    : id(id), cid(1), snapshot(snapshot), engine(engine)
{
}

bool TransactionId::in_snapshot(const Id &id) const
{
    return snapshot.is_active(id);
}

Id TransactionId::oldest_active()
{
    return snapshot.oldest_active().take_or(Id(id));
}
}
