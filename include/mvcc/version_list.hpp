#pragma once

#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

#include "memory/lazy_gc.hpp"
#include "mvcc/serialization_error.hpp"
#include "storage/locking/record_lock.hpp"

namespace mvcc {

  template<class T>
  class VersionList {
    friend class Accessor;

  public:
    using uptr = std::unique_ptr<VersionList<T>>;
    using item_t = T;

    VersionList() = default;

    VersionList(const VersionList &) = delete;

    /* @brief Move constructs the version list
     * Note: use only at the beginning of the "other's" lifecycle since this
     * constructor doesn't move the RecordLock, but only the head pointer
     */
    VersionList(VersionList &&other) {
      this->head = other.head.load();
      other.head = nullptr;
    }

    ~VersionList() { delete head.load(); }

    friend std::ostream &operator<<(std::ostream &stream,
                                    const VersionList<T> &vlist) {
      stream << "VersionList" << std::endl;

      auto record = vlist.head.load();

      while (record != nullptr) {
        stream << "-- " << *record << std::endl;
        record = record->next();
      }

      return stream;
    }

    auto gc_lock_acquire() { return std::unique_lock<RecordLock>(lock); }

    // Frees all records which are deleted by transaction older than given id.
    // EXPECTS THAT THERE IS NO ACTIVE TRANSACTION WITH ID LESS THAN GIVEN ID.
    // EXPECTS THAT THERE WON'T BE SIMULATAIUS CALLS FROM DIFFERENT THREADS OF
    // THIS METHOD.
    // True if this whole version list isn't needed any more. There is still
    // possibilty that someone is reading it at this moment but he cant change
    // it or get anything from it.
    // TODO: Validate this method
    bool gc_deleted(const Id &id) {
      auto r = head.load(std::memory_order_seq_cst);
      T *bef = nullptr;

      //    nullptr
      //       |
      //     [v1]      ...
      //       |
      //     [v2] <------+
      //       |         |
      //     [v3] <------+
      //       |         |  Jump backwards until you find a first old deleted
      //   [VerList] ----+  version, or you reach the end of the list
      //
      while (r != nullptr && !r->is_deleted_before(id)) {
        bef = r;
        r = r->next(std::memory_order_seq_cst);
      }

      if (bef == nullptr) {
        // if r==nullptr he is needed and it is expecting insert.
        // if r!=nullptr vertex has been explicitly deleted. It can't be
        // updated because for update, visible record is needed and at this
        // point whe know that there is no visible record for any
        // transaction. Also it cant be inserted because head isn't nullptr.
        // Remove also requires visible record. Find wont return any record
        // because none is visible.
        return r != nullptr;
      } else {
        if (r != nullptr) {
          // Bef is possible visible to some transaction but r is not and
          // the implementation of this version list guarantees that
          // record r and older records aren't accessed.
          bef->next(nullptr, std::memory_order_seq_cst);
          delete r; // THIS IS ISSUE IF MULTIPLE THREADS TRY TO DO THIS
        }

        return false;
      }
    }

    void vacuum() {}

    T *find(const tx::TransactionRead &t) const {
      auto r = head.load(std::memory_order_seq_cst);

      //    nullptr
      //       |
      //     [v1]      ...
      //       |
      //     [v2] <------+
      //       |         |
      //     [v3] <------+
      //       |         |  Jump backwards until you find a first visible
      //   [VerList] ----+  version, or you reach the end of the list
      //
      while (r != nullptr && !r->visible(t))
        r = r->next(std::memory_order_seq_cst);

      return r;
    }

    T *insert(tx::Transaction &t) {
      assert(head == nullptr);

      // create a first version of the record
      // TODO replace 'new' with something better
      auto v1 = new T();

      // mark the record as created by the transaction t
      v1->mark_created(t);

      head.store(v1, std::memory_order_seq_cst);

      return v1;
    }

    T *update(tx::Transaction &t) {
      assert(head != nullptr);
      auto record = find(t);

      // check if we found any visible records
      if (!record) return nullptr;

      return update(record, t);
    }

    T *update(T *record, tx::Transaction &t) {
      assert(record != nullptr);
      lock_and_validate(record, t);

      // It could be done with unique_ptr but while this could mean memory
      // leak on exception, unique_ptr could mean use after free. Memory
      // leak is less dangerous.
      auto updated = new T();
      updated->data = record->data;

      updated->mark_created(t);
      record->mark_deleted(t);

      updated->next(record, std::memory_order_seq_cst);
      head.store(updated, std::memory_order_seq_cst);

      return updated;
    }

    bool remove(tx::Transaction &t) {
      assert(head != nullptr);
      auto record = find(t);

      if (!record) return false;

      // TODO: Is this lock and validate necessary
      lock_and_validate(record, t);
      return remove(record, t), true;
    }

    void remove(T *record, tx::Transaction &t) {
      assert(record != nullptr);
      lock_and_validate(record, t);
      record->mark_deleted(t);
    }

  private:
    void lock_and_validate(T *record, tx::Transaction &t) {
      assert(record != nullptr);

      // take a lock on this node
      t.take_lock(lock);

      // if the record hasn't been deleted yet or the deleting transaction
      // has aborted, it's ok to modify it
      if (!record->tx.exp() || !record->exp_committed(t)) return;

      // if it committed, then we have a serialization conflict
      assert(record->hints.load().exp.is_committed());
      throw SerializationError();
    }

    std::atomic<T *> head{nullptr};
    RecordLock lock;
  };
}
