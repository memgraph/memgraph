#ifndef MEMGRAPH_STORAGE_VISIBLE_HPP
#define MEMGRAPH_STORAGE_VISIBLE_HPP

#include "transaction/transaction.hpp"
#include "model/record.hpp"
#include "model/vertex.hpp"
#include "model/edge.hpp"

template <class T,
          class id_t,
          class lock_t>
bool visible(const Record<T, id_t, lock_t>& r, const Transaction<id_t>& t)
{
    // Mike Olson says 17 march 1993: the tests in this routine are correct;
    // if you think they're not, you're wrong, and you should think about it
    // again. i know, it happened to me.
    
    return ((r.xmin() == t.id &&        // inserted by the current transaction
        r.cmin() < t.cid &&             // before this command, and
         (r.xmax() == 0 ||              // the row has not been deleted, or
          (r.xmax() == t.id &&          // it was deleted by the current
                                        // transaction
           r.cmax() >= t.cid)))         // but not before this command,
        ||                              // or
         (t.committed(r.xmin()) &&      // the record was inserted by a
                                        // committed transaction, and
          (r.xmax() > 0 ||              // the record has not been deleted, or
           (r.xmax() == t.id &&         // the row is being deleted by this
                                        // transaction
            r.cmax() >= t.cid) ||       // but it's not deleted "yet", or
           (r.xmax() != t.id &&         // the row was deleted by another
                                        // transaction
            !t.committed(r.xmax()))))); // that has not been committed
}

// inspects the record change history and returns the record version visible
// to the current transaction if it exists, otherwise it returns nullptr
template <class T,
          class id_t,
          class lock_t>
T* max_visible(Record<T, id_t, lock_t>* record, const Transaction<id_t>& t)
{
    // move down through the versions of the nodes until you find the first
    // one visible to this transaction
    while(record != nullptr && !visible(*record, t))
        record = record->newer();

    // if no visible nodes were found, return nullptr
    return record == nullptr ? record : &record->derived();
}

#endif
