#pragma once

#include <atomic>
#include <iostream>

#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

#include "mvcc/cre_exp.hpp"
#include "mvcc/hints.hpp"
#include "mvcc/id.hpp"
#include "mvcc/version.hpp"
#include "storage/locking/record_lock.hpp"

// the mvcc implementation used here is very much like postgresql's
// more info: https://momjian.us/main/writings/pgsql/mvcc.pdf

namespace mvcc {

template <class T>
class Record : public Version<T> {
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
  bool visible(const tx::Transaction &t) {
    // TODO check if the record was created by a transaction that has been
    // aborted. one might implement this by checking the hints in mvcc
    // anc/or consulting the commit log

    // Mike Olson says 17 march 1993: the tests in this routine are correct;
    // if you think they're not, you're wrong, and you should think about it
    // again. i know, it happened to me.

    return ((tx.cre() == t.id &&      // inserted by the current transaction
             cmd.cre() < t.cid &&     // before this command, and
             (tx.exp() == Id(0) ||    // the row has not been deleted, or
              (tx.exp() == t.id &&    // it was deleted by the current
                                      // transaction
               cmd.exp() >= t.cid)))  // but not before this command,
            ||                        // or
            (cre_committed(tx.cre(), t) &&  // the record was inserted by a
                                            // committed transaction, and
             (tx.exp() == Id(0) ||     // the record has not been deleted, or
              (tx.exp() == t.id &&     // the row is being deleted by this
                                       // transaction
               cmd.exp() >= t.cid) ||  // but it's not deleted "yet", or
              (tx.exp() != t.id &&     // the row was deleted by another
                                       // transaction
               !exp_committed(tx.exp(), t)  // that has not been committed
               ))));
  }

  void mark_created(const tx::Transaction &t) {
    tx.cre(t.id);
    cmd.cre(t.cid);
  }

  void mark_deleted(const tx::Transaction &t) {
    tx.exp(t.id);
    cmd.exp(t.cid);
  }

  bool exp_committed(const Id &id, const tx::Transaction &t) {
    return committed(hints.exp, id, t);
  }

  bool exp_committed(const tx::Transaction &t) {
    return committed(hints.exp, tx.exp(), t.engine);
  }

  bool cre_committed(const Id &id, const tx::Transaction &t) {
    return committed(hints.cre, id, t);
  }

  bool cre_committed(const tx::Transaction &t) {
    return committed(hints.cre, tx.cre(), t);
  }

  // True if record was deleted before id.
  bool is_deleted_before(const Id &id) {
    return tx.exp() != Id(0) && tx.exp() < id;
  }

  // TODO: Test this
  // True if this record is visible for write.
  // Note that this logic is different from the one above
  // in the sense that a record is visible if created before
  // OR DURING this command. this is done to support cypher's
  // queries which can match, update and return in the same query
  bool is_visible_write(const tx::Transaction &t) {
    return (tx.cre() == t.id &&       // inserted by the current transaction
            cmd.cre() <= t.cid &&     // before OR DURING this command, and
            (tx.exp() == Id(0) ||     // the row has not been deleted, or
             (tx.exp() == t.id &&     // it was deleted by the current
                                      // transaction
              cmd.exp() >= t.cid)));  // but not before this command,
  }

  /**
   * True if this record is created in the current command
   * of the given transaction.
   */
  bool is_created_by(const tx::Transaction &t) {
    return tx.cre() == t.id && cmd.cre() == t.cid;
  }

  /**
   * True if this record is deleted in the current command
   * of the given transaction.
   */
  bool is_deleted_by(const tx::Transaction &t) {
    return tx.exp() == t.id && cmd.exp() == t.cid;
  }

 protected:
  template <class U>
  /**
   * @brief - Check if the id is commited from the perspective of transactio,
   * i.e. transaction can see the transaction with that id (it happened before
   * the transaction, and is not in the snapshot). This method is used to test
   * for visibility of some record.
   * @param hints - hints to use to determine commit/abort
   * about transactions commit/abort status
   * @param id - id to check if it's commited and visible
   * @return true if the id is commited and visible for the transaction t.
   */
  bool committed(U &hints, const Id &id, const tx::Transaction &t) {
    // Dominik Gleich says 4 april 2017: the tests in this routine are correct;
    // if you think they're not, you're wrong, and you should think about it
    // again. I know, it happened to me (and also to Matej Gradicek).

    // You certainly can't see the transaction with id greater than yours as
    // that means it started after this transaction and if it commited, it
    // commited after this transaction has started.
    if (id >= t.id) return false;

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

    debug_assert(info.is_aborted(),
                 "Info isn't aborted, but function would return as aborted.");
    return hints.set_aborted(), false;
  }

  template <class U>
  /**
   * @brief - Check if the id is commited.
   * @param hints - hints to use to determine commit/abort
   * @param id - id to check if commited
   * @param engine - engine instance with information about transactions
   * statuses
   * @return true if it's commited, false otherwise
   */
  bool committed(U &hints, const Id &id, tx::Engine &engine) {
    auto hint_bits = hints.load();
    // if hints are set, return if xid is committed
    if (!hint_bits.is_unknown()) return hint_bits.is_committed();

    // if hints are not set:
    // - you are the first one to check since it ended, consult commit log
    auto info = engine.clog.fetch_info(id);
    if (info.is_committed()) return hints.set_committed(), true;
    if (info.is_aborted()) return hints.set_aborted(), false;
    return false;
  }
};
}
