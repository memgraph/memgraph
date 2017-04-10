#pragma once

#include <shared_mutex>

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

  /**
   * @brief Constructor that is used to insert one item into VersionList.
   * @param t - transaction
   * @param args - args forwarded to constructor of item T (for
   * creating the first Record (Version) in this VersionList.
   */
  template <typename... Args>
  VersionList(tx::Transaction &t, Args &&... args) {
    // TODO replace 'new' with something better
    auto v1 = new T(std::forward<Args>(args)...);

    // mark the record as created by the transaction t
    v1->mark_created(t);

    head.store(v1, std::memory_order_seq_cst);
  }

  VersionList() = delete;
  VersionList(const VersionList &) = delete;
  VersionList &operator=(const VersionList &) = delete;
  // We do a lot of raw-pointer ops with VLists, and these ops assume that a
  // VList's address identifies a vertex/edge absolutely and during it's whole
  // lifteme. We also assume that the VList owner is the database and that
  // ownership is also handled via raw pointers so this shouldn't be moved or
  // move assigned.
  VersionList(const VersionList &&other) = delete;
  VersionList &operator=(const VersionList &&other) = delete;

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

    // This can happen only if the head already points to a deleted record or
    // the version list is empty. This means that the version_list is ready
    // for complete destruction.
    if (oldest_not_deleted_record == nullptr) {
      // Head points to a deleted record.
      if (newest_deleted_record != nullptr) {
        {
          // We need to take an exclusive lock here since if some thread is
          // currently doing the find operation we could free something to which
          // find method points.
          std::unique_lock<std::shared_timed_mutex> lock(head_mutex_);
          head.store(nullptr, std::memory_order_seq_cst);
        }
        // This is safe to do since we unlinked head and now no record* in
        // another thread which is executing the find function can see any of
        // the nodes we are going to delete.
        delete newest_deleted_record;
      }
      return true;
    }
    // oldest_not_deleted_record might be visible to some transaction but
    // newest_deleted_record is not and will never be visted by the find
    // function and as such doesn't represent pointer invalidation
    // race-condition risk.
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

  T *find(const tx::Transaction &t) {
    // We need to take a shared_lock here because GC could delete the first
    // entry pointed by head, or some later entry when following head->next
    // pointers.
    std::shared_lock<std::shared_timed_mutex> slock(head_mutex_);
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

  /**
   * Looks for and sets two versions. The 'old' version is the
   * newest version that is visible by the current transaction+command,
   * but has not been created by it. The 'new' version is the version
   * that has been created by current transaction+command.
   *
   * It is possible that both, either or neither are found:
   *     - both are found when an existing record has been modified
   *     - only old is found when an existing record has not been modified
   *     - only new is found when the whole vlist was created
   *     - neither is found when for example the record has been deleted but not
   *       garbage collected yet
   *
   * @param t The transaction
   */
  void find_set_new_old(const tx::Transaction &t, T *&old_ref, T *&new_ref) {
    // Take a look in find to understand why this is needed.
    std::shared_lock<std::shared_timed_mutex> slock(head_mutex_);

    // assume that the sought old record is further down the list
    // from new record, so that if we found old we can stop looking
    new_ref = nullptr;
    old_ref = head.load(std::memory_order_seq_cst);
    while (old_ref != nullptr && !old_ref->visible(t)) {
      if (!new_ref && old_ref->is_created_by(t)) new_ref = old_ref;
      old_ref = old_ref->next(std::memory_order_seq_cst);
    }
  }

  T *update(tx::Transaction &t) {
    debug_assert(head != nullptr, "Head is nullptr on update.");
    auto record = find(t);

    // check if we found any visible records
    if (!record) return nullptr;

    return update(record, t);
  }

  // TODO(flor): This should also be private but can't be right now because of
  // the way graph_db_accessor works.
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

  T *update(T *record, tx::Transaction &t) {
    debug_assert(record != nullptr, "Record is nullptr on update.");
    lock_and_validate(record, t);

    // It could be done with unique_ptr but while this could mean memory
    // leak on exception, unique_ptr could mean use after free. Memory
    // leak is less dangerous.
    auto updated = new T(*record);

    updated->mark_created(t);
    record->mark_deleted(t);

    // Updated version should point to the latest available version. Older
    // versions that can be deleted will be removed during the GC phase.
    updated->next(head.load(), std::memory_order_seq_cst);

    // Store the updated version as the first version point to by head.
    head.store(updated, std::memory_order_seq_cst);

    return updated;
  }

  std::atomic<T *> head{nullptr};
  RecordLock lock;
  // We need this mutex to make the nodes deletion operation (to avoid memory
  // leak) atomic with regards to the find operation. Otherwise we might have a
  // pointer to a version that is currently being deleted, and accessing it will
  // throw a SEGFAULT.
  // TODO(C++17)
  std::shared_timed_mutex head_mutex_;  // This should be changed
                                        // when we migrate to C++17.
};
}
