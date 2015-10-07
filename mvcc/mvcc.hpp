#ifndef MEMGRAPH_STORAGE_MODEL_UTILS_MVCC_HPP
#define MEMGRAPH_STORAGE_MODEL_UTILS_MVCC_HPP

#include <atomic>

#include "transactions/transaction.hpp"
#include "transactions/commit_log.hpp"

#include "minmax.hpp"
#include "version.hpp"
#include "hints.hpp"

// the mvcc implementation used here is very much like postgresql's
// more info: https://momjian.us/main/writings/pgsql/mvcc.pdf

namespace mvcc
{

template <class T>
class Mvcc : public Version<T>
{
public:
    Mvcc() = default;

    // tx.min is the id of the transaction that created the record
    // and tx.max is the id of the transaction that deleted the record
    // these values are used to determine the visibility of the record
    // to the current transaction
    MinMax<uint64_t> tx;

    // cmd.min is the id of the command in this transaction that created the
    // record and cmd.max is the id of the command in this transaction that
    // deleted the record. these values are used to determine the visibility
    // of the record to the current command in the running transaction
    MinMax<uint8_t> cmd;

    // check if this record is visible to the transaction t
    bool visible(const tx::Transaction& t)
    {
        // TODO check if the record was created by a transaction that has been
        // aborted. one might implement this by checking the hints in mvcc 
        // anc/or consulting the commit log

        // Mike Olson says 17 march 1993: the tests in this routine are correct;
        // if you think they're not, you're wrong, and you should think about it
        // again. i know, it happened to me.

        return ((tx.min() == t.id &&    // inserted by the current transaction
            cmd.min() < t.cid &&        // before this command, and
             (tx.max() == 0 ||          // the row has not been deleted, or
              (tx.max() == t.id &&      // it was deleted by the current
                                        // transaction
               cmd.max() >= t.cid)))    // but not before this command,
            ||                          // or
             (min_committed(tx.min(), t) && // the record was inserted by a
                                        // committed transaction, and
              (tx.max() == 0 ||         // the record has not been deleted, or
               (tx.max() == t.id &&     // the row is being deleted by this
                                        // transaction
                cmd.max() >= t.cid) ||  // but it's not deleted "yet", or
               (tx.max() != t.id &&     // the row was deleted by another
                                        // transaction
                !max_committed(tx.max(), t) // that has not been committed
            ))));
    }

    // inspects the record change history and returns the record version visible
    // to the current transaction if it exists, otherwise it returns nullptr
    T* latest_visible(const tx::Transaction& t)
    {
        T* record = static_cast<T*>(this), *newer = this->newer();
        
        // move down through the versions of the nodes until you find the first
        // one visible to this transaction. if no visible records are found,
        // the function returns a nullptr
        while(newer != nullptr && !newer->visible(t))
            record = newer, newer = record->newer();

        return record;
    }

    void mark_created(const tx::Transaction& t)
    {
        tx.min(t.id);
        cmd.min(t.cid);
    }

    void mark_deleted(const tx::Transaction& t)
    {
        tx.max(t.id);
        cmd.max(t.cid);
    }

protected:
    Hints hints;

    bool max_committed(uint64_t id, const tx::Transaction& t)
    {
        return committed(hints.max, id, t);
    }

    bool min_committed(uint64_t id, const tx::Transaction& t)
    {
        return committed(hints.min, id, t);
    }

    template <class U>
    bool committed(U& hints, uint64_t id, const tx::Transaction& t)
    {
        auto hint_bits = hints.load();

        // if hints are set, return if xid is committed
        if(!hint_bits.is_unknown())
            return hint_bits.is_committed();

        // if hints are not set:
        // - the creating transaction is still in progress (examine snapshot)
        if(t.snapshot.is_active(id))
            return false;

        // - you are the first one to check since it ended, consult commit log
        auto& clog = tx::CommitLog::get();
        auto info = clog.fetch_info(id);

        if(info.is_committed())
        {
            hints.set_committed();
            return true;
        }

        assert(info.is_aborted());
        hints.set_aborted();
        return false;
    }
};

}

#endif
