#pragma once

#include <atomic>

#include "transactions/transaction.hpp"
#include "transactions/commit_log.hpp"
#include "mvcc/id.hpp"
#include "cre_exp.hpp"
#include "version.hpp"
#include "hints.hpp"
#include "storage/locking/record_lock.hpp"

// the mvcc implementation used here is very much like postgresql's
// more info: https://momjian.us/main/writings/pgsql/mvcc.pdf

namespace mvcc
{

template <class T>
class Record : public Version<T>
{
public:
    // tx.cre is the id of the transaction that created the record
    // and tx.exp is the id of the transaction that deleted the record
    // these values are used to determine the visibility of the record
    // to the current transaction
    CreExp<Id> tx;

    // cmd.cre is the id of the command in this transaction that created the
    // record and cmd.exp is the id of the command in this transaction that
    // deleted the record. these values are used to determine the visibility
    // of the record to the current command in the running transaction
    CreExp<uint8_t> cmd;

    Hints hints;

    // this lock is used by write queries when they update or delete records
    RecordLock lock;

    // check if this record is visible to the transaction t
    bool visible(const tx::Transaction& t)
    {
        // TODO check if the record was created by a transaction that has been
        // aborted. one might implement this by checking the hints in mvcc
        // anc/or consulting the commit log

        // Mike Olson says 17 march 1993: the tests in this routine are correct;
        // if you think they're not, you're wrong, and you should think about it
        // again. i know, it happened to me.

        return ((tx.cre() == t.id &&    // inserted by the current transaction
            cmd.cre() <= t.cid &&       // before this command, and
             (tx.exp() == Id(0) ||      // the row has not been deleted, or
              (tx.exp() == t.id &&      // it was deleted by the current
                                        // transaction
               cmd.exp() >= t.cid)))    // but not before this command,
            ||                          // or
             (cre_committed(tx.cre(), t) && // the record was inserted by a
                                        // committed transaction, and
              (tx.exp() == Id(0) ||     // the record has not been deleted, or
               (tx.exp() == t.id &&     // the row is being deleted by this
                                        // transaction
                cmd.exp() >= t.cid) ||  // but it's not deleted "yet", or
               (tx.exp() != t.id &&     // the row was deleted by another
                                        // transaction
                !exp_committed(tx.exp(), t) // that has not been committed
            ))));
    }

    void mark_created(const tx::Transaction& t)
    {
        tx.cre(t.id);
        cmd.cre(t.cid);
    }

    void mark_deleted(const tx::Transaction& t)
    {
        tx.exp(t.id);
        cmd.exp(t.cid);
    }

    bool exp_committed(const Id& id, const tx::Transaction& t)
    {
        return committed(hints.exp, id, t);
    }

    bool cre_committed(const Id& id, const tx::Transaction& t)
    {
        return committed(hints.cre, id, t);
    }

    template <class U>
    bool committed(U& hints, const Id& id, const tx::Transaction& t)
    {
        // you certainly can't see the transaction with id greater than yours
        // as that means it started after this transaction and if it committed,
        // it committed after this transaction had started.
        if(id > t.id)
            return false;

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
            return hints.set_committed(), true;

        assert(info.is_aborted());
        return hints.set_aborted(), false;
    }
};

}
