#pragma once

#include "storage/indexes/index_update.hpp"
#include "transactions/transaction.hpp"

class Db;
class DbAccessor;
class SnapshotEncoder;

using index_updates_t = std::vector<IndexUpdate>;

// Inner structures local to transaction can hold ref to this structure and use
// its methods.
// Also serves as a barrier for calling methods defined public but meant for
// internal use. That kind of method should request DbTransaction&.
class DbTransaction
{
    friend DbAccessor;

public:
    DbTransaction(Db &db);
    DbTransaction(Db &db, tx::Transaction &trans) : db(db), trans(trans) {}

    // Global transactional algorithms,operations and general methods meant for
    // internal use should be here or should be routed through this object.
    // This should provide cleaner hierarchy of operations on database.
    // For example cleaner.

    // Cleans edge part of database. MUST be called by one cleaner thread at
    // one time.
    void clean_edge_section();

    // Cleans vertex part of database. MUST be called by one cleaner thread at
    // one time..
    void clean_vertex_section();

    // Updates indexes of Vertex/Edges in index_updates. True if indexes are
    // updated successfully. False means that transaction failed.
    bool update_indexes();

    // Will update indexes for given element TG::record_t. Actual update happens
    // with call update_indexes
    template <class TG>
    void to_update_index(typename TG::vlist_t *vlist,
                         typename TG::record_t *record)
    {
        index_updates.push_back(make_index_update(vlist, record));
    }

    index_updates_t index_updates;

    tx::Transaction &trans;

    Db &db;
};
