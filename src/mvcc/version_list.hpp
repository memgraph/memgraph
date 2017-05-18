#pragma once

#include <shared_mutex>

#include "memory/lazy_gc.hpp"
#include "storage/locking/record_lock.hpp"
#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"
#include "utils/assert.hpp"
#include "utils/exceptions.hpp"

namespace mvcc {

class SerializationError : public utils::BasicException {
  static constexpr const char *default_message =
      "Can't serialize due to\
        concurrent operation(s)";

 public:
  using utils::BasicException::BasicException;
  SerializationError() : BasicException(default_message) {}
};

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
   * Re-links all records which are no longer visible for any
   * transaction with an id greater or equal to id.
   * @param id - transaction id from which to start garbage collection
   * that is not visible anymore. If none exists to_delete will rbecome nullptr.
   * @param engine - transaction engine to use - we need it to check which
   * records were commited and which werent
   * @return pair<status, to_delete>; status is true - If version list is empty
   * after garbage collection. to_delete points to the newest record that is not
   * visible anymore. If none exists to_delete will point to nullptr.
  */
  std::pair<bool, T *> GcDeleted(const Id &id, tx::Engine &engine) {

    //    nullptr
    //       |
    //     [v1]      ...  all of this gets deleted!
    //       |
    //     [v2] <------+  head_of_deletable_records
    //       |         |
    //     [v3] <------+  oldest_visible_record
    //       |         |  Jump backwards until you find the oldest visible
    //   [VerList] ----+  record, or you reach the end of the list
    //
    
    auto current = head.load();
    T *head_of_deletable_records = current;
    T *oldest_visible_record = nullptr;
    while (current) {
      if (!current->is_not_visible_from(id, engine))
          oldest_visible_record = current;
        current = current->next();
    }
    if (oldest_visible_record)
      head_of_deletable_records = oldest_visible_record->next();

    // This can happen only if the head already points to a deleted record or
    // the version list is empty. This means that the version_list is ready
    // for complete destruction.
    if (oldest_visible_record == nullptr) {
      // Head points to a deleted record.
      if (head_of_deletable_records != nullptr) {
        head.store(nullptr, std::memory_order_seq_cst);
        // This is safe to return as ready for deletion since we unlinked head
        // above and this will only be deleted after the last active transaction
        // ends.
        return std::make_pair(true, head_of_deletable_records);
      }
      return std::make_pair(true, nullptr);
    }
    // oldest_visible_record might be visible to some transaction but
    // head_of_deletable_records is not and will never be visted by the find
    // function and as such doesn't represent pointer invalidation
    // race-condition risk.
    oldest_visible_record->next(
        nullptr, std::memory_order_seq_cst);  // No transaction will look
                                              // further than this record and
                                              // that's why it's safe to set
                                              // next to nullptr.
    // Calling destructor  of head_of_deletable_records will clean everything older
    // than this record since they are called recursively.
    return std::make_pair(false, head_of_deletable_records);
  }

  /**
   * @brief - returns oldest record
   * @return nullptr if none exist
   */
  T *Oldest() {
    auto r = head.load(std::memory_order_seq_cst);
    while (r && r->next(std::memory_order_seq_cst))
      r = r->next(std::memory_order_seq_cst);
    return r;
  }

  T *find(const tx::Transaction &t) {
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
  void find_set_old_new(const tx::Transaction &t, T *&old_ref, T *&new_ref) {
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
    permanent_assert(record != nullptr, "Updating nullptr record");

    return update(record, t);
  }

  void remove(tx::Transaction &t) {
    debug_assert(head != nullptr, "Head is nullptr on removal.");
    auto record = find(t);

    permanent_assert(record != nullptr, "Removing nullptr record");

    // TODO: Is this lock and validate necessary
    lock_and_validate(record, t);
    remove(record, t);
  }

  // TODO(flor): This should also be private but can't be right now because of
  // the way graph_db_accessor works.
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
};
}
