#pragma once

#include "storage/single_node_ha/gid.hpp"
#include "storage/common/locking/record_lock.hpp"
#include "transactions/transaction.hpp"
#include "utils/cast.hpp"
#include "utils/exceptions.hpp"

namespace mvcc {

class SerializationError : public utils::BasicException {
  static constexpr const char *default_message =
      "Can't serialize due to concurrent operations.";

 public:
  using utils::BasicException::BasicException;
  SerializationError() : BasicException(default_message) {}
};

template <class T>
class VersionList {
 public:
  /**
   * @brief Constructor that is used to insert one item into VersionList.
   *
   * @param t - transaction
   * @param gid - Version list identifier. Uniqueness guaranteed by the code
   * creating this version list.
   * @param args - args forwarded to constructor of item T (for
   * creating the first Record (Version) in this VersionList.
   */
  template <typename... Args>
  VersionList(const tx::Transaction &t, gid::Gid gid, Args &&... args)
      : gid_(gid) {
    // TODO replace 'new' with something better
    auto *v1 = new T(std::forward<Args>(args)...);
    v1->mark_created(t);
    head_ = v1;
  }

  VersionList() = delete;
  VersionList(const VersionList &) = delete;
  VersionList &operator=(const VersionList &) = delete;
  // We do a lot of raw-pointer ops with VLists, and these ops assume that a
  // VList's address identifies a vertex/edge absolutely and during it's whole
  // lifteme. We also assume that the VList owner is the database and that
  // ownership is also handled via raw pointers so this shouldn't be moved or
  // move assigned.
  VersionList(VersionList &&other) = delete;
  VersionList &operator=(VersionList &&other) = delete;

  ~VersionList() { delete head_.load(); }

  friend std::ostream &operator<<(std::ostream &stream,
                                  const VersionList<T> &vlist) {
    stream << "VersionList" << std::endl;

    T *record = vlist.head_;

    while (record != nullptr) {
      stream << "-- " << *record << std::endl;
      record = record->next();
    }

    return stream;
  }

  /**
   * Garbage collects records that are not reachable/visible anymore.
   *
   * Relinks this version-list so that garbage collected records are no
   * longer reachable through this version list.
   * Visibility is defined in mvcc::Record::is_not_visible_from,
   * to which the given `snapshot` is passed.
   *
   * This method is NOT thread-safe.
   *
   * @param snapshot - the GC snapshot. Consists of the oldest active
   * transaction's snapshot, with that transaction's id appened as last.
   * @param engine - transaction engine to use - we need it to check which
   * records were commited and which weren't
   * @return pair<status, to_delete>; status is true - If version list is empty
   * after garbage collection. to_delete points to the newest record that is not
   * visible anymore. If none exists to_delete will point to nullptr.
   */
  std::pair<bool, T *> GcDeleted(const tx::Snapshot &snapshot,
                                 const tx::Engine &engine) {
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

    T *head = head_;
    T *current = head;
    T *oldest_visible_record = nullptr;
    while (current) {
      // Populate hints only when needed to avoid excessive rpc calls on
      // workers.
      // snapshot.back() corresponds to the oldest active transaction,
      // and this makes it set only hint bits when the creating or expiring
      // transaction of a record is older than that)
      current->populate_hints(engine, snapshot.back());
      if (!current->is_not_visible_from(snapshot, engine))
        oldest_visible_record = current;
      current = current->next();
    }

    if (oldest_visible_record) {
      T *head_of_deletable_records = oldest_visible_record->next();
      // oldest_visible_record might be visible to some transaction but
      // head_of_deletable_records is not and will never be visted by the find
      // function and as such doesn't represent pointer invalidation
      // race-condition risk.
      oldest_visible_record->next(nullptr);  // No transaction will look
                                             // further than this record and
                                             // that's why it's safe to set
                                             // next to nullptr.
      // Calling destructor  of head_of_deletable_records will clean everything
      // older than this record since they are called recursively.
      return std::make_pair(false, head_of_deletable_records);
    }

    // This can happen only if the head points to a expired record. Since there
    // is no visible records in this version_list we can remove it.
    head_ = nullptr;
    // This is safe to return as ready for deletion since we unlinked head
    // above and this will only be deleted after the last active transaction
    // ends.
    return std::make_pair(true, head);
  }

  /**
   * @brief - returns oldest record
   * @return nullptr if none exist
   */
  T *Oldest() {
    T *r = head_;
    while (r && r->next(std::memory_order_seq_cst))
      r = r->next(std::memory_order_seq_cst);
    return r;
  }

  T *find(const tx::Transaction &t) {
    T *r = head_;

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
  void find_set_old_new(const tx::Transaction &t, T **old_ref, T **new_ref) {
    // assume that the sought old record is further down the list
    // from new record, so that if we found old we can stop looking
    *new_ref = nullptr;
    *old_ref = head_;
    while (*old_ref != nullptr && !(*old_ref)->visible(t)) {
      if (!*new_ref && (*old_ref)->is_created_by(t)) *new_ref = *old_ref;
      *old_ref = (*old_ref)->next(std::memory_order_seq_cst);
    }
  }

  /**
   * Looks for the first visible record seen by this transaction. If the current
   * transaction has already created new record in the current command then that
   * record is returned, else first older visible record is updated. New record
   * becomes head of the version list and it is returned. There should always be
   * older visible record when this update is called.
   *
   * @param t The transaction
   */
  T *update(const tx::Transaction &t) {
    DCHECK(head_ != nullptr) << "Head is nullptr on update.";
    T *old_record = nullptr;
    T *new_record = nullptr;
    find_set_old_new(t, &old_record, &new_record);

    // check if current transaction in current cmd has
    // already updated version list
    if (new_record) return new_record;

    // check if we found any visible records
    CHECK(old_record != nullptr) << "Updating nullptr record";

    return update(old_record, t);
  }

  /** Makes the given record as being expired by the given transaction. */
  void remove(T *record, const tx::Transaction &t) {
    DCHECK(record != nullptr) << "Record is nullptr on removal.";
    lock_and_validate(record, t);
    record->mark_expired(t);
  }

  const gid::Gid gid_;

  int64_t cypher_id() { return utils::MemcpyCast<int64_t>(gid_); }

 private:
  void lock_and_validate(T *record, const tx::Transaction &t) {
    DCHECK(record != nullptr) << "Record is nullptr on lock and validation.";

    // take a lock on this node
    t.TakeLock(lock_);

    // if the record hasn't been deleted yet or the deleting transaction
    // has aborted, it's ok to modify it
    if (!record->tx().exp || !record->exp_committed(t.engine_)) return;

    // if it committed, then we have a serialization conflict
    throw SerializationError();
  }

  T *update(T *record, const tx::Transaction &t) {
    DCHECK(record != nullptr) << "Record is nullptr on update.";
    lock_and_validate(record, t);

    // It could be done with unique_ptr but while this could mean memory
    // leak on exception, unique_ptr could mean use after free. Memory
    // leak is less dangerous.
    auto *updated = record->CloneData();

    updated->mark_created(t);
    record->mark_expired(t);

    // Updated version should point to the latest available version. Older
    // versions that can be deleted will be removed during the GC phase.
    updated->next(head_.load(), std::memory_order_seq_cst);

    // Store the updated version as the first version point to by head.
    head_.store(updated, std::memory_order_seq_cst);

    return updated;
  }

  std::atomic<T *> head_{nullptr};
  RecordLock lock_;
};
}  // namespace mvcc
