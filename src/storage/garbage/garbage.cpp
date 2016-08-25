#include "storage/garbage/garbage.hpp"

void Garbage::dispose(tx::Snapshot<Id> &&snapshot, DeleteSensitive *data)
{
    // TODO: add to list
}

void Garbage::clean(tx::Engine &engine)
{
    // TODO: iterator throug list and check snapshot
}
