#pragma once

#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

#include "memory/lazy_gc.hpp"
#include "mvcc/serialization_error.hpp"
#include "storage/locking/record_lock.hpp"
#include "utils/assert.hpp"

namespace mvcc {

template <class T>
class VersionList {
 public:
  using uptr = std::unique_ptr<VersionList<T>>;
  using item_t = T;

  /* @brief Constructor that is used to insert one item into VersionList.
     @param t - transaction
     @param T item - item which will point to new and only entry in
     version_list.
     @param args - args forwarded to constructor of item T.
   */
  template <typename... Args>
  VersionList(tx::Transaction &t, T *&item, Args &&... args) {
    item = insert(t, std::forward<Args>(args)...);
  }

  VersionList() = delete;
  VersionList(const VersionList &) = delete;
  VersionList &operator=(const VersionList &) = delete;

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

  /**
   * This method is NOT thread-safe. This should never be called with a
   * transaction id newer than the oldest active transaction id.
   * Garbage collect (delete) all records which are no longer visible for any
   * transaction with an id greater or equal to id.
   * @param id - transaction id from which to start garbage collection
   * @return true - If version list is empty after garbage collection.
  */
  bool GcDeleted(const Id &id) {
    auto newest_deleted_record = head.load(std::memory_order_seq_cst);
    T *oldest_not_deleted_record = nullptr;

    //    nullptr
    //       |
    //     [v1]      ...
    //       |
    //     [v2] <------+  newest_deleted_record
    //       |         |
    //     [v3] <------+  oldest_not_deleted_record
    //       |         |  Jump backwards until you find a first old deleted
    //   [VerList] ----+  record, or you reach the end of the list
    //
    while (newest_deleted_record != nullptr &&
           !newest_deleted_record->is_deleted_before(id)) {
      oldest_not_deleted_record = newest_deleted_record;
      newest_deleted_record =
          newest_deleted_record->next(std::memory_order_seq_cst);
    }

    if (oldest_not_deleted_record == nullptr) {
      // This can happen only if the head already points to a deleted record or
      // the version list is empty. This means that the version_list is ready
      // for complete destruction.
      if (newest_deleted_record != nullptr) delete newest_deleted_record;
      head.store(nullptr, std::memory_order_seq_cst);
      return true;
    }
    // oldest_not_deleted_record might be visible to some transaction but
    // newest_deleted_record is not.
    oldest_not_deleted_record->next(
        nullptr, std::memory_order_seq_cst);  // No transaction will look
                                              // further than this record and
                                              // that's why it's safe to set
                                              // next to nullptr.
    // Call destructor which will clean everything older than this record since
    // they are called recursively.
    if (newest_deleted_record != nullptr)
      delete newest_deleted_record;  // THIS IS ISSUE IF MULTIPLE THREADS TRY TO
                                     // DO THIS
    return false;
  }

  T *find(const tx::Transaction &t) const {
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

  T *update(tx::Transaction &t) {
    debug_assert(head != nullptr, "Head is nullptr on update.");
    auto record = find(t);

    // check if we found any visible records
    if (!record) return nullptr;

    return update(record, t);
  }

  T *update(T *record, tx::Transaction &t) {
    debug_assert(record != nullptr, "Record is nullptr on update.");
    lock_and_validate(record, t);

    // It could be done with unique_ptr but while this could mean memory
    // leak on exception, unique_ptr could mean use after free. Memory
    // leak is less dangerous.
    auto updated = new T(*record);

    updated->mark_created(t);
    record->mark_deleted(t);

    updated->next(record, std::memory_order_seq_cst);
    head.store(updated, std::memory_order_seq_cst);

    return updated;
  }

  bool remove(tx::Transaction &t) {
    debug_assert(head != nullptr, "Head is nullptr on removal.");
    auto record = find(t);

    if (!record) return false;

    // TODO: Is this lock and validate necessary
    lock_and_validate(record, t);
    return remove(record, t), true;
  }

  void remove(T *record, tx::Transaction &t) {
    debug_assert(record != nullptr, "Record is nullptr on removal.");
    lock_and_validate(record, t);
    record->mark_deleted(t);
  }

 private:
  void lock_and_validate(T *record, tx::Transaction &t) {
    debug_assert(record != nullptr,
                 "Record is nullptr on lock and validation.");

    // take a lock on this node
    t.take_lock(lock);

    // if the record hasn't been deleted yet or the deleting transaction
    // has aborted, it's ok to modify it
    if (!record->tx.exp() || !record->exp_committed(t)) return;

    // if it committed, then we have a serialization conflict
    debug_assert(record->hints.load().exp.is_committed(),
                 "Serialization conflict.");
    throw SerializationError();
  }

  /**
   * This is private because this should only be called from the constructor.
   * Otherwise head might be nullptr while version_list exists and it could
   * interfere with GC.
   * @tparam Args forwarded to the constructor of T
   */
  template <typename... Args>
  T *insert(tx::Transaction &t, Args &&... args) {
    debug_assert(head == nullptr, "Head is not nullptr on creation.");

    // create a first version of the record
    // TODO replace 'new' with something better
    auto v1 = new T(std::forward<Args>(args)...);

    // mark the record as created by the transaction t
    v1->mark_created(t);

    head.store(v1, std::memory_order_seq_cst);
    return v1;
  }

  std::atomic<T *> head{nullptr};
  RecordLock lock;
};
}
