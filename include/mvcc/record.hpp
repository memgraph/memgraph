#pragma once

#include <atomic>
#include <iostream>

#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction_read.hpp"

#include "mvcc/cre_exp.hpp"
#include "mvcc/hints.hpp"
#include "mvcc/id.hpp"
#include "mvcc/version.hpp"
#include "storage/locking/record_lock.hpp"

// the mvcc implementation used here is very much like postgresql's
// more info: https://momjian.us/main/writings/pgsql/mvcc.pdf

namespace mvcc
{

template <class T>
class Record : public Version<T>
{
public:

  Record() = default;

  // The copy constructor ignores tx, cmd, hints and super because
  // they contain atomic variables that can't be copied
  // it's still useful to have this copy constructor so that subclass
  // data can easily be copied
  // TODO maybe disable the copy-constructor and instead use a
  // data variable in the version_list update() function (and similar)
  // like it was in Dominik's implementation
  Record(const Record &other) {}

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

    // NOTE: Wasn't used.
    // this lock is used by write queries when they update or delete records
    // RecordLock lock;

    // check if this record is visible to the transaction t
    bool visible(const tx::TransactionRead &t)
    {
        // TODO check if the record was created by a transaction that has been
        // aborted. one might implement this by checking the hints in mvcc
        // anc/or consulting the commit log

        // Mike Olson says 17 march 1993: the tests in this routine are correct;
        // if you think they're not, you're wrong, and you should think about it
        // again. i know, it happened to me.

        return ((tx.cre() == t.id &&     // inserted by the current transaction
                 cmd.cre() <= t.cid &&   // before this command, and
                 (tx.exp() == Id(0) ||   // the row has not been deleted, or
                  (tx.exp() == t.id &&   // it was deleted by the current
                                         // transaction
                   cmd.exp() >= t.cid))) // but not before this command,
                ||                       // or
                (cre_committed(tx.cre(), t) && // the record was inserted by a
                                               // committed transaction, and
                 (tx.exp() == Id(0) ||    // the record has not been deleted, or
                  (tx.exp() == t.id &&    // the row is being deleted by this
                                          // transaction
                   cmd.exp() >= t.cid) || // but it's not deleted "yet", or
                  (tx.exp() != t.id &&    // the row was deleted by another
                                          // transaction
                   !exp_committed(tx.exp(), t) // that has not been committed
                   ))));
    }

    void mark_created(const tx::TransactionRead &t)
    {
        tx.cre(t.id);
        cmd.cre(t.cid);
    }

    void mark_deleted(const tx::TransactionRead &t)
    {
        tx.exp(t.id);
        cmd.exp(t.cid);
    }

    bool exp_committed(const Id &id, const tx::TransactionRead &t)
    {
        return committed(hints.exp, id, t);
    }

    bool exp_committed(const tx::TransactionRead &t)
    {
        return committed(hints.exp, tx.exp(), t);
    }

    bool cre_committed(const Id &id, const tx::TransactionRead &t)
    {
        return committed(hints.cre, id, t);
    }

    bool cre_committed(const tx::TransactionRead &t)
    {
        return committed(hints.cre, tx.cre(), t);
    }

    // True if record was deleted before id.
    bool is_deleted_before(const Id &id)
    {
        return tx.exp() != Id(0) && tx.exp() < id;
    }

    // TODO: Test this
    // True if this record is visible for write.
    bool is_visible_write(const tx::TransactionRead &t)
    {
        return (tx.cre() == t.id &&      // inserted by the current transaction
                cmd.cre() <= t.cid &&    // before this command, and
                (tx.exp() == Id(0) ||    // the row has not been deleted, or
                 (tx.exp() == t.id &&    // it was deleted by the current
                                         // transaction
                  cmd.exp() >= t.cid))); // but not before this command,
    }

protected:
    template <class U>
    bool committed(U &hints, const Id &id, const tx::TransactionRead &t)
    {
        // you certainly can't see the transaction with id greater than yours
        // as that means it started after this transaction and if it committed,
        // it committed after this transaction had started.
        if (id > t.id) return false;

        // The creating transaction is still in progress (examine snapshot)
        if (t.in_snapshot(id)) return false;

        auto hint_bits = hints.load();

        // TODO: Validate if this position is valid for next if.
        // if hints are set, return if xid is committed
        if (!hint_bits.is_unknown()) return hint_bits.is_committed();

        // if hints are not set:
        // - you are the first one to check since it ended, consult commit log
        auto info = t.engine.clog.fetch_info(id);

        if (info.is_committed()) return hints.set_committed(), true;

        assert(info.is_aborted());
        return hints.set_aborted(), false;
    }
};
}
