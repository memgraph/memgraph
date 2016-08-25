#pragma once

#include "data_structures/concurrent/concurrent_list.hpp"
#include "mvcc/id.hpp"
#include "storage/garbage/delete_sensitive.hpp"
#include "transactions/snapshot.hpp"

namespace tx
{
class Engine;
}

// Collection of delete sensitive data which need to be safely deleted. That
// meens that all transactions that may have pointer to it must finish before
// the sensitive data can be safely destroyed.
class Garbage
{
public:
    void dispose(tx::Snapshot<Id> &&snapshot, DeleteSensitive *data);

    // Cleaner thread shoul call this method every some time. Removes data which
    // is
    // safe to be deleted.
    void clean(tx::Engine &engine);

private:
    List<std::pair<tx::Snapshot<Id>, DeleteSensitive *>> gar;
};
